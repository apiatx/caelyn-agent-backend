"""
Social page X-dashboard service.

Derives 4 Social sections from the existing x_consensus_cache snapshot —
NO additional Grok / XAI calls are made here.

Sections:
  A. x_consensus         — from raw.consensus_picks (classification-gated)
  B. freshest_alpha      — deterministic, novelty-relative
  C. theme_leadership    — from raw.hype_radar + raw.market_pulse [UNCHANGED]
  D. sentiment_accel     — deterministic, prior-base + strengthening slope

Section separation design (unified classification pass):
  A single _classify_tickers_for_sections() pass runs first and assigns
  every ticker in _backend_ranked to exactly one bucket:

    fa   — Freshest Alpha
           Criteria: high-quality source (top_trader/above_average_trader)
                   + recency ≤ FA_RECENCY_CUTOFF
                   + IS novel relative to prior history
           Novelty (when prior_br populated):
             prior_raw_score < FA_PRIOR_PRESENCE_THRESHOLD
           Novelty (when prior_br empty — fallback to current-snapshot signals):
             bullish_account_count == 1  (single-source, not yet consensus)
             OR buzz_trend == "New Mention"  (Grok flags as brand-new)

    sa   — Sentiment Acceleration
           Criteria: NOT classified as FA
                   + has established prior base (prior_raw ≥ threshold OR
                     Grok signals "Accelerating"/"Rising")
                   + is strengthening (accel_ratio > threshold OR Grok says so)

    xc   — X Consensus
           Criteria: NOT classified as FA or SA
                   + bullish_account_count ≥ 2

    none — excluded from all three sections

  Mutual exclusion is enforced. FA is highest priority, XC is lowest.

Data paths:
  When _mention_data IS present: rich per-mention window scoring is used for
  FA + SA, with classification applied as a post-filter (mutual exclusion).
  When _mention_data is absent: deterministic builders use _backend_ranked
  directly (always populated). Legacy Grok field fallbacks are REMOVED —
  they were causing the wrong tickers to appear.

  X Consensus always uses consensus_picks (Grok curated) filtered by
  classification: only 'xc'-classified tickers survive.
"""
from __future__ import annotations

import time
from typing import Any, Optional


# ── Tier constants ────────────────────────────────────────────────────────────

_FA_ELIGIBLE_TIERS: frozenset[str] = frozenset({"top_trader", "above_average_trader"})

# Grok buzz_trend values that signal "brand-new name" vs "has momentum history"
_FA_BUZZ_NOVEL:    frozenset[str] = frozenset({"New Mention", ""})
_SA_BUZZ_MOMENTUM: frozenset[str] = frozenset({"Accelerating", "Rising"})

# ── Classification thresholds ─────────────────────────────────────────────────

# Prior raw_score below this → ticker has no meaningful established history → FA eligible
_FA_PRIOR_PRESENCE_THRESHOLD: float = 0.30

# FA: hard recency cutoff in days (mentions older than this are not "fresh alpha")
_FA_RECENCY_CUTOFF: int = 14

# SA: current raw_score must be at least this multiple of prior to show "acceleration"
_SA_ACCEL_RATIO_MIN: float = 1.20

# XC: minimum number of bullish accounts required for "shared conviction"
_XC_MIN_ACCOUNTS: int = 2

# ── Freshest Alpha scoring ────────────────────────────────────────────────────

_FA_RECENCY_BOOSTS: list[tuple[int, float]] = [
    (0,  3.0),
    (1,  2.5),
    (3,  2.0),
    (7,  1.0),
    (14, 0.3),
    (21, 0.1),
]
_FA_RECENCY_FALLBACK = 0.02


def _fa_recency_boost(days: int) -> float:
    for bound, w in _FA_RECENCY_BOOSTS:
        if days <= bound:
            return w
    return _FA_RECENCY_FALLBACK


# ── Snapshot loaders ──────────────────────────────────────────────────────────

def _load_snapshots() -> tuple[Optional[dict], Optional[dict]]:
    from services.x_consensus_cache import _load_disk_cache, _load_prior_cache
    return _load_disk_cache(), _load_prior_cache()


def _raw(snapshot: Optional[dict]) -> dict:
    if not snapshot or not isinstance(snapshot, dict):
        return {}
    r = snapshot.get("raw")
    return r if isinstance(r, dict) else {}


def _name_lookup(current_snap: Optional[dict]) -> dict[str, tuple[str, str]]:
    lookup: dict[str, tuple[str, str]] = {}
    raw = _raw(current_snap)
    for p in (raw.get("consensus_picks") or []):
        if not isinstance(p, dict):
            continue
        t = (p.get("ticker") or "").upper().lstrip("$").strip()
        if t:
            lookup[t] = (p.get("name") or "", p.get("tradingview_symbol") or "")
    for ft in (raw.get("fresh_trades") or []):
        if not isinstance(ft, dict):
            continue
        t = (ft.get("ticker") or "").upper().lstrip("$").strip()
        if t and t not in lookup:
            lookup[t] = (ft.get("name") or "", ft.get("tradingview_symbol") or "")
    return lookup


# ── Unified classifier ────────────────────────────────────────────────────────

def _classify_tickers_for_sections(
    backend_ranked: list[dict],
    prior_br_map: dict[str, dict],
    buzz_map: dict[str, str],
) -> dict[str, str]:
    """
    Assign every ticker in backend_ranked to exactly one section bucket.

    Returns {ticker: 'fa' | 'sa' | 'xc' | 'none'}

    Classification priority: fa > sa > xc > none

    Signals used:
      has_top_conviction  — any top_trader/above_average_trader in top_accounts
      recency_days_min    — freshest mention in current scan
      bullish_account_count — unique bullish accounts in current scan
      prior_raw_score     — from prior _backend_ranked (0 if brand-new or prior empty)
      accel_ratio         — cur_raw / (prior_raw + ε): how much stronger than before
      buzz_trend          — Grok's label from consensus_picks:
                            "Accelerating"/"Rising" → has momentum history
                            "New Mention"/""        → brand-new name

    When prior_br_map has data (ideal case):
      is_novel     = prior_raw < FA_PRIOR_PRESENCE_THRESHOLD
      has_prior_base = prior_raw ≥ FA_PRIOR_PRESENCE_THRESHOLD
      is_accelerating = accel_ratio ≥ SA_ACCEL_RATIO_MIN
                     OR buzz in SA_BUZZ_MOMENTUM

    When prior_br_map is empty (degraded case — no historical baseline):
      is_novel     = accts == 1            (single-source = not yet consensus)
                  OR buzz in FA_BUZZ_NOVEL  (Grok says brand-new)
      has_prior_base = buzz in SA_BUZZ_MOMENTUM  (Grok says has momentum history)
      is_accelerating = buzz in SA_BUZZ_MOMENTUM
    """
    prior_has_data = len(prior_br_map) > 0
    classes: dict[str, str] = {}

    for bs in backend_ranked:
        ticker        = bs["ticker"]
        accts         = bs.get("bullish_account_count") or 0
        rec           = bs.get("recency_days_min")
        rec           = int(rec) if rec is not None else 999
        cur_raw       = float(bs.get("raw_score") or 0.0)
        has_top_qual  = bs.get("has_top_conviction", False)
        buzz          = buzz_map.get(ticker, "")

        prior         = prior_br_map.get(ticker)
        prior_raw     = float(prior.get("raw_score") or 0.0) if prior else 0.0
        accel_ratio   = cur_raw / (prior_raw + 0.01)

        if prior_has_data:
            # ── Rich path: actual historical comparison ────────────────────
            is_novel       = prior_raw < _FA_PRIOR_PRESENCE_THRESHOLD
            has_prior_base = prior_raw >= _FA_PRIOR_PRESENCE_THRESHOLD
            is_accel       = (accel_ratio >= _SA_ACCEL_RATIO_MIN
                              or buzz in _SA_BUZZ_MOMENTUM)
        else:
            # ── Degraded path: no prior data, use current signals ──────────
            # Novel = single-source (not yet multi-account) OR Grok says new
            is_novel       = (accts == 1) or (buzz in _FA_BUZZ_NOVEL)
            # Prior base = Grok's context signals prior momentum history
            has_prior_base = buzz in _SA_BUZZ_MOMENTUM
            is_accel       = buzz in _SA_BUZZ_MOMENTUM

        # Priority 1 — Freshest Alpha
        if has_top_qual and rec <= _FA_RECENCY_CUTOFF and is_novel:
            classes[ticker] = "fa"
            continue

        # Priority 2 — Sentiment Acceleration
        if has_prior_base and is_accel:
            classes[ticker] = "sa"
            continue

        # Priority 3 — X Consensus
        if accts >= _XC_MIN_ACCOUNTS:
            classes[ticker] = "xc"
            continue

        classes[ticker] = "none"

    fa_n  = sum(1 for v in classes.values() if v == "fa")
    sa_n  = sum(1 for v in classes.values() if v == "sa")
    xc_n  = sum(1 for v in classes.values() if v == "xc")
    print(
        f"[social_x] classifier: {len(classes)} tickers → "
        f"FA={fa_n}, SA={sa_n}, XC={xc_n}, none={len(classes)-fa_n-sa_n-xc_n} "
        f"(prior_has_data={prior_has_data})"
    )
    return classes


# ── Section A — X Consensus ───────────────────────────────────────────────────

def _build_x_consensus(
    raw: dict,
    classified: dict[str, str],
) -> list[dict]:
    """
    Normalise consensus_picks into the Social-page row format.

    Only tickers classified as 'xc' survive.
    Tickers classified as 'fa' or 'sa' are excluded — they have a more precise
    section that better represents their signal.

    Contract fields are preserved exactly for backward compatibility.
    """
    picks = raw.get("consensus_picks") or []
    out: list[dict] = []
    excluded = 0

    for p in picks:
        if not isinstance(p, dict):
            continue
        ticker = (p.get("ticker") or "").upper().lstrip("$")
        if not ticker:
            continue

        section = classified.get(ticker, "xc")
        if section in ("fa", "sa", "none"):
            excluded += 1
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

    if excluded:
        print(
            f"[social_x] X Consensus: excluded {excluded} ticker(s) "
            f"(classified as FA or SA)"
        )
    return out


# ── Section B — Freshest Alpha ────────────────────────────────────────────────

def _build_freshest_alpha(
    current_snap: Optional[dict],
    prior_snap: Optional[dict],
    classified: dict[str, str],
) -> dict:
    """
    Surface newly emerging ticker calls from ONLY top_trader + above_average_trader
    accounts.

    Primary path (_mention_data present):
      Uses per-mention recency + conviction from Phase-1 extraction.
      Only tickers classified as 'fa' are emitted (mutual exclusion enforced).

    Fallback path (_mention_data absent):
      Uses _backend_ranked directly — always available.
      Tickers classified as 'fa' are scored by raw_score × fa_recency_boost.
      No legacy Grok fresh_trades fallback (removed — caused wrong classifications).
    """
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []

    if mention_data:
        return _build_fa_from_mention_data(current_snap, prior_snap, classified)

    # _backend_ranked fallback
    backend_ranked: list[dict] = (current_snap or {}).get("_backend_ranked") or []
    prior_br: list[dict]       = (prior_snap   or {}).get("_backend_ranked") or []
    prior_rank_by_ticker        = {s["ticker"]: i for i, s in enumerate(prior_br)}
    name_map                    = _name_lookup(current_snap)
    return _build_fa_from_backend_ranked(
        classified, backend_ranked, prior_rank_by_ticker, name_map
    )


def _build_fa_from_mention_data(
    current_snap: Optional[dict],
    prior_snap: Optional[dict],
    classified: dict[str, str],
) -> dict:
    """FA using rich per-mention data from _mention_data."""
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []
    prior_br: list[dict]     = (prior_snap or {}).get("_backend_ranked") or []
    prior_rank_by_ticker      = {s["ticker"]: i for i, s in enumerate(prior_br)}
    name_map                  = _name_lookup(current_snap)
    buckets: dict[str, dict]  = {}

    for acct in mention_data:
        if acct.get("category") not in _FA_ELIGIBLE_TIERS:
            continue
        handle  = acct.get("handle", "")
        tier_w  = float(acct.get("weight") or 0.0)
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

            # Mutual exclusion: only 'fa' classified tickers
            if classified.get(ticker, "fa") != "fa":
                continue

            rd_raw       = m.get("recency_days")
            recency_days = int(rd_raw) if rd_raw is not None else 30
            recency_days = max(0, min(recency_days, 365))
            if recency_days > 21:
                continue

            fa_boost  = _fa_recency_boost(recency_days)
            conviction = (m.get("conviction") or "medium").lower()
            conv_mult  = {"high": 1.3, "medium": 1.0, "low": 0.6}.get(conviction, 1.0)
            mention_score = tier_w * fa_boost * conv_mult

            if ticker not in buckets:
                buckets[ticker] = {
                    "ticker":          ticker,
                    "score":           0.0,
                    "min_recency":     9999,
                    "accounts":        {},
                    "theses":          [],
                    "catalysts":       [],
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
        if b["min_recency"] > 21 or b["score"] <= 0:
            continue

        prior_rank = prior_rank_by_ticker.get(ticker)
        if prior_rank is None:
            novelty_mult = 1.5
        elif prior_rank >= 10:
            novelty_mult = 1.2
        elif prior_rank >= 5:
            novelty_mult = 0.9
        else:
            novelty_mult = 0.75

        n_accts      = len(b["accounts"])
        breadth_mult = 1.0 + min(0.20 * (n_accts - 1), 0.40)
        final_score  = b["score"] * novelty_mult * breadth_mult

        tier_order = {"top_trader": 0, "above_average_trader": 1}
        top_thesis_entry = min(
            b["theses"],
            key=lambda t: tier_order.get(t["tier"], 9),
        ) if b["theses"] else None

        name, tv_sym   = name_map.get(ticker, ("", ""))
        accts_sorted   = sorted(b["accounts"].items(), key=lambda x: -x[1])

        results.append({
            "ticker":              ticker,
            "name":                name,
            "tradingview_symbol":  tv_sym,
            "first_mentioned_by":  [f"@{h}" for h, _ in accts_sorted],
            "why_fresh": (
                f"{'New' if prior_rank is None else 'Emerging'} call "
                f"from {n_accts} top-quality trader(s) within last "
                f"{b['min_recency']}d"
                + (f" — {', '.join(sorted(b['conviction_seen']))} conviction"
                   if b["conviction_seen"] else "")
            ),
            "entry_thesis":       top_thesis_entry["text"] if top_thesis_entry else "",
            "spotlight_badge":    False,
            "spotlight_signal":   None,
            "freshest_alpha_score":   round(final_score, 3),
            "min_recency_days":       b["min_recency"] if b["min_recency"] < 9999 else None,
            "quality_account_count":  n_accts,
            "is_brand_new":           prior_rank is None,
            "novelty_mult":           round(novelty_mult, 2),
            "catalysts":              list(dict.fromkeys(b["catalysts"]))[:5],
            "top_accounts": [
                {"handle": h, "contribution": round(s, 3)} for h, s in accts_sorted
            ],
        })

    return _finalise_fa(results)


def _build_fa_from_backend_ranked(
    classified: dict[str, str],
    backend_ranked: list[dict],
    prior_rank_by_ticker: dict[str, int],
    name_map: dict[str, tuple[str, str]],
) -> dict:
    """
    FA fallback when _mention_data is absent.

    Scores each 'fa'-classified ticker using:
      raw_score × fa_recency_boost(recency_days_min) × novelty_mult × breadth_mult

    where novelty_mult is based on prior snapshot ranking (same as _mention_data path).
    No Grok fresh_trades used — _backend_ranked always contains more complete data.
    """
    results: list[dict] = []

    for bs in backend_ranked:
        ticker = bs["ticker"]
        if classified.get(ticker) != "fa":
            continue

        rec = int(bs.get("recency_days_min") or 0)
        if rec > 21:
            continue

        cur_raw   = float(bs.get("raw_score") or 0.0)
        fa_boost  = _fa_recency_boost(rec)

        prior_rank = prior_rank_by_ticker.get(ticker)
        if prior_rank is None:
            novelty_mult = 1.5
        elif prior_rank >= 10:
            novelty_mult = 1.2
        elif prior_rank >= 5:
            novelty_mult = 0.9
        else:
            novelty_mult = 0.75

        n_accts      = bs.get("bullish_account_count") or 1
        breadth_mult = 1.0 + min(0.20 * (n_accts - 1), 0.40)
        final_score  = cur_raw * fa_boost * novelty_mult * breadth_mult

        name, tv_sym  = name_map.get(ticker, ("", ""))
        theses        = bs.get("thesis_fragments") or []
        top_thesis    = theses[0]["text"] if theses else ""
        top_accts_raw = sorted(
            bs.get("top_accounts") or [],
            key=lambda a: -float(a.get("contribution") or 0),
        )

        results.append({
            "ticker":              ticker,
            "name":                name,
            "tradingview_symbol":  tv_sym,
            "first_mentioned_by":  [f"@{a['handle']}" for a in top_accts_raw],
            "why_fresh": (
                f"{'New' if prior_rank is None else 'Emerging'} call "
                f"from {n_accts} top-quality trader(s), recency ≤{rec}d"
            ),
            "entry_thesis":       top_thesis,
            "spotlight_badge":    False,
            "spotlight_signal":   None,
            "freshest_alpha_score":   round(final_score, 3),
            "min_recency_days":       rec,
            "quality_account_count":  n_accts,
            "is_brand_new":           prior_rank is None,
            "novelty_mult":           round(novelty_mult, 2),
            "catalysts":              (bs.get("catalyst_list") or [])[:5],
            "top_accounts": [
                {
                    "handle":       a.get("handle"),
                    "contribution": round(float(a.get("contribution") or 0), 3),
                }
                for a in top_accts_raw
            ],
        })

    return _finalise_fa(results)


def _finalise_fa(results: list[dict]) -> dict:
    """Sort FA results, attach spotlight badge, return standard shape."""
    results.sort(key=lambda x: -x["freshest_alpha_score"])

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
        spotlight = {"ticker": top["ticker"], "signal": top["spotlight_signal"]}

    return {"trades": results[:20], "spotlight": spotlight}


# ── Section C — Theme Leadership (unchanged) ──────────────────────────────────

def _build_theme_leadership(raw: dict) -> dict:
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


# ── Section D — Sentiment Acceleration ───────────────────────────────────────

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
    if slope_7_to_3 >= 1.5 and slope_14_to_7 >= 1.2:
        return "Accelerating"
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
    classified: dict[str, str],
) -> list[dict]:
    """
    Surface tickers where social consensus is intensifying over time.

    Primary path (_mention_data present):
      Multi-window slope analysis from per-mention recency data.
      Only tickers classified as 'sa' are emitted (mutual exclusion enforced).

    Fallback path (_mention_data absent):
      Uses _backend_ranked directly (always available).
      Requires classification='sa' (prior-base + strengthening signal).
      No legacy hype_score fallback (removed — caused wrong classifications).
    """
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []

    if mention_data:
        return _build_sa_from_mention_data(current_snap, prior_snap, classified)

    backend_ranked: list[dict] = (current_snap or {}).get("_backend_ranked") or []
    prior_br: list[dict]       = (prior_snap   or {}).get("_backend_ranked") or []
    prior_br_map                = {s["ticker"]: s for s in prior_br}

    cur_raw_snap = _raw(current_snap)
    buzz_map: dict[str, str] = {
        (p.get("ticker") or "").upper().lstrip("$"): p.get("buzz_trend", "")
        for p in (cur_raw_snap.get("consensus_picks") or [])
        if isinstance(p, dict)
    }

    name_map = _name_lookup(current_snap)
    return _build_sa_from_backend_ranked(
        classified, backend_ranked, prior_br_map, buzz_map, name_map
    )


def _build_sa_from_mention_data(
    current_snap: Optional[dict],
    prior_snap: Optional[dict],
    classified: dict[str, str],
) -> list[dict]:
    """SA using rich per-mention window data."""
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []
    name_map                  = _name_lookup(current_snap)
    prior_br_by_ticker: dict[str, dict] = {
        s["ticker"]: s
        for s in ((prior_snap or {}).get("_backend_ranked") or [])
    }

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

            # Mutual exclusion: only 'sa' classified tickers
            if classified.get(ticker, "sa") != "sa":
                continue

            rd_raw       = m.get("recency_days")
            recency_days = int(rd_raw) if rd_raw is not None else 91
            recency_days = max(0, min(recency_days, 365))

            conviction  = (m.get("conviction") or "medium").lower()
            conv_mult   = {"high": 1.2, "medium": 1.0, "low": 0.7}.get(conviction, 1.0)
            catalysts   = [str(c) for c in (m.get("catalysts") or []) if c]
            spec_mult   = 1.2 if catalysts else 1.0
            base_score  = tier_w * conv_mult * spec_mult

            if ticker not in buckets:
                buckets[ticker] = {
                    "ticker":      ticker,
                    "w3": 0.0, "w7": 0.0, "w14": 0.0, "w30": 0.0, "w90": 0.0,
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

        if w14 <= 0:
            continue

        # SA requires prior base: must have activity outside the freshest window
        # (w30 > 0 means there were mentions older than 7 days — established presence)
        if w30 == 0 and len(b["accounts"]) == 1:
            continue

        slope_7_to_3   = w3  / (w7  + 0.01)
        slope_14_to_7  = w7  / (w14 + 0.01)
        slope_30_to_14 = w14 / (w30 + 0.01)

        base_intensity = w3 * 4.0 + w7 * 2.0 + w14 * 1.0 + w30 * 0.5

        accel_bonus = 0.0
        if slope_7_to_3   > 0.5:
            accel_bonus += 0.3 * min(slope_7_to_3, 2.0)
        if slope_14_to_7  > 0.5:
            accel_bonus += 0.2 * min(slope_14_to_7, 2.0)
        if slope_30_to_14 > 0.5:
            accel_bonus += 0.1 * min(slope_30_to_14, 2.0)

        n_accts      = len(b["accounts"])
        breadth_mult = 1.0 + min(0.15 * (n_accts - 1), 0.45)
        final_accel  = base_intensity * (1.0 + accel_bonus) * breadth_mult

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

        top_thesis   = b["theses"][0]["text"] if b["theses"] else ""
        prior        = prior_br_by_ticker.get(ticker)
        prior_acct   = prior["bullish_account_count"] if prior else 0
        norm_score   = min(round(final_accel * 2.0), 100)
        w3_vs_w14    = w3 / (w14 + 0.01)
        buzz_trend   = _sa_buzz_trend(slope_7_to_3, slope_14_to_7, w3_vs_w14)
        con_strength = _sa_consensus_strength(n_accts, final_accel)
        name, tv_sym = _name_lookup(current_snap).get(ticker, ("", ""))

        results.append({
            "ticker":                    ticker,
            "name":                      name,
            "tradingview_symbol":        tv_sym,
            "current_hype_score":        norm_score,
            "prior_hype_score":          0,
            "hype_delta":                norm_score,
            "current_trader_count":      n_accts,
            "prior_trader_count":        prior_acct,
            "trader_count_delta":        n_accts - prior_acct,
            "current_consensus_strength": con_strength,
            "buzz_trend":                buzz_trend,
            "is_new_entry":              prior is None,
            "thesis":                    top_thesis,
            "why_now": "; ".join(parts) if parts else "Sustained momentum",
            "accel_score":               round(final_accel, 3),
            "window_scores": {
                "w3":  round(w3,  3), "w7":  round(w7,  3),
                "w14": round(w14, 3), "w30": round(w30, 3),
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


def _build_sa_from_backend_ranked(
    classified: dict[str, str],
    backend_ranked: list[dict],
    prior_br_map: dict[str, dict],
    buzz_map: dict[str, str],
    name_map: dict[str, tuple[str, str]],
) -> list[dict]:
    """
    SA fallback when _mention_data is absent.

    Scores each 'sa'-classified ticker using:
      raw_score × breadth_mult × (1 + accel_bonus)

    accel_bonus:
      When prior available: based on accel_ratio (current / prior)
      When prior absent:    based on buzz_trend (Accelerating → +0.3)
    """
    results: list[dict] = []

    for bs in backend_ranked:
        ticker = bs["ticker"]
        if classified.get(ticker) != "sa":
            continue

        cur_raw = float(bs.get("raw_score") or 0.0)
        accts   = bs.get("bullish_account_count") or 1
        rec     = int(bs.get("recency_days_min") or 0)
        buzz    = buzz_map.get(ticker, "")

        prior     = prior_br_map.get(ticker)
        prior_raw = float(prior.get("raw_score") or 0.0) if prior else 0.0

        breadth_mult = 1.0 + min(0.15 * (accts - 1), 0.45)

        if prior_raw > 0:
            accel_ratio  = cur_raw / (prior_raw + 0.01)
            accel_bonus  = min((accel_ratio - 1.0) * 0.5, 0.5) if accel_ratio > 1 else 0.0
        else:
            # Degraded: no prior data — use Grok buzz as proxy
            accel_bonus = 0.3 if buzz in _SA_BUZZ_MOMENTUM else 0.0
            accel_ratio = 1.0

        final_score = cur_raw * breadth_mult * (1.0 + accel_bonus)

        # buzz_trend label
        if buzz in _SA_BUZZ_MOMENTUM:
            buzz_trend = buzz
        elif accel_ratio >= 1.5:
            buzz_trend = "Accelerating"
        elif accel_ratio >= 1.1:
            buzz_trend = "Rising"
        else:
            buzz_trend = "Stable"

        name, tv_sym  = name_map.get(ticker, ("", ""))
        theses        = bs.get("thesis_fragments") or []
        top_thesis    = theses[0]["text"] if theses else ""
        prior_acct    = prior["bullish_account_count"] if prior else 0
        norm_score    = min(round(final_score * 2.0), 100)
        con_strength  = _sa_consensus_strength(accts, final_score)

        parts: list[str] = []
        if buzz in _SA_BUZZ_MOMENTUM:
            parts.append(f"{buzz} momentum — {accts} account(s)")
        if prior_raw > 0 and accel_ratio > 1.2:
            parts.append(f"Score {accel_ratio:.1f}× prior baseline")

        results.append({
            "ticker":                    ticker,
            "name":                      name,
            "tradingview_symbol":        tv_sym,
            "current_hype_score":        norm_score,
            "prior_hype_score":          0,
            "hype_delta":                norm_score,
            "current_trader_count":      accts,
            "prior_trader_count":        prior_acct,
            "trader_count_delta":        accts - prior_acct,
            "current_consensus_strength": con_strength,
            "buzz_trend":                buzz_trend,
            "is_new_entry":              prior is None,
            "thesis":                    top_thesis,
            "why_now": ("; ".join(parts) if parts else "Sustained and strengthening momentum"),
            "accel_score":               round(final_score, 3),
            "window_scores": {
                "w3": 0.0, "w7": 0.0, "w14": 0.0, "w30": 0.0, "w90": 0.0,
            },
            "slope_7_to_3":  0.0,
            "slope_14_to_7": 0.0,
            "w3_vs_w14":     0.0,
            "account_count":             accts,
            "min_recency_days":          rec,
            "catalysts":                 (bs.get("catalyst_list") or [])[:5],
        })

    results.sort(key=lambda x: -x["accel_score"])
    return results[:12]


# ── Metadata helpers ──────────────────────────────────────────────────────────

def _build_metadata(snapshot: Optional[dict]) -> dict:
    """Build the standard metadata block from a current snapshot.

    Key design: auto-schedule fields and manual-refresh fields are COMPLETELY
    SEPARATE so the frontend can enable/disable each button independently.

    Auto-schedule fields:
      auto_refresh_window_open    — True only 08:00-20:00 America/Chicago
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

    Orchestration:
      1. Load current + prior snapshots.
      2. Run _classify_tickers_for_sections() — single unified pass that assigns
         every ticker in _backend_ranked to fa / sa / xc / none.
      3. Pass classification map to all three section builders.
      4. Each builder enforces mutual exclusion: only tickers with its own
         classification are emitted.

    Shape contract:
      A. Home-style consensus payload (flat, unchanged):
           generated_at, top_tickers, key_themes, notable_accounts,
           is_stale, stale, data_state, age_seconds, refresh_in_progress,
           available, refresh_window_open, next_allowed_refresh_at, timezone
      B. Three Social-only sibling sections (additive):
           x_consensus          — consensus_picks filtered by classification
           freshest_alpha       — novelty-relative, top-tier accounts only
           theme_leadership     — from raw.hype_radar + raw.market_pulse [UNCHANGED]
           sentiment_acceleration — prior-base + strengthening slope
      C. Convenience keys from raw snapshot:
           market_pulse, portfolio_bias, spotlight
      D. Social-specific metadata.
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
            "x_consensus":            [],
            "freshest_alpha":         {"trades": [], "spotlight": None},
            "theme_leadership":       {"themes": [], "market_pulse": None},
            "sentiment_acceleration": [],
            "metadata":               _build_metadata(None),
        }

    # ── Step 1: Build classifier inputs ──────────────────────────────────────
    backend_ranked: list[dict] = (current_snap or {}).get("_backend_ranked") or []
    prior_br: list[dict]       = (prior_snap   or {}).get("_backend_ranked") or []
    prior_br_map: dict[str, dict] = {s["ticker"]: s for s in prior_br}

    buzz_map: dict[str, str] = {
        (p.get("ticker") or "").upper().lstrip("$"): (p.get("buzz_trend") or "")
        for p in (cur_raw.get("consensus_picks") or [])
        if isinstance(p, dict)
    }

    # ── Step 2: Unified classification pass ──────────────────────────────────
    classified = _classify_tickers_for_sections(backend_ranked, prior_br_map, buzz_map)

    # ── Step 3: Build sections (each respects classified map) ─────────────────
    return {
        **home_payload,
        "market_pulse":   cur_raw.get("market_pulse"),
        "portfolio_bias": cur_raw.get("portfolio_bias"),
        "spotlight":      cur_raw.get("spotlight"),
        "x_consensus":            _build_x_consensus(cur_raw, classified),
        "freshest_alpha":         _build_freshest_alpha(current_snap, prior_snap, classified),
        "theme_leadership":       _build_theme_leadership(cur_raw),
        "sentiment_acceleration": _build_sentiment_accel(current_snap, prior_snap, classified),
        "metadata":               _build_metadata(current_snap),
    }
