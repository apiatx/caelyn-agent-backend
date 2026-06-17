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

# Grok buzz_trend values used ONLY in the degraded classifier path
# (when prior _backend_ranked is empty and no actual accel_ratio is computable).
# These are proxies of last resort — never used when real prior data is available.
_FA_BUZZ_NOVEL:    frozenset[str] = frozenset({"New Mention", ""})
_SA_BUZZ_ACCEL:    frozenset[str] = frozenset({"Accelerating"})      # strong momentum only
_SA_BUZZ_MOMENTUM: frozenset[str] = frozenset({"Accelerating", "Rising"})  # kept for SA builder

# ── Classification thresholds ─────────────────────────────────────────────────

# Prior raw_score below this → ticker has no meaningful established history → FA eligible
_FA_PRIOR_PRESENCE_THRESHOLD: float = 0.30

# FA: hard recency cutoff in days (mentions older than this are not "fresh alpha")
_FA_RECENCY_CUTOFF: int = 10

# SA: single-snapshot accel_ratio fallback (used when history has < 4d of spread)
_SA_ACCEL_RATIO_MIN: float = 1.20

# SA multi-window thresholds — active when ticker history has enough temporal spread
# to produce meaningful per-day rate comparisons across rolling windows.
#
#   _SA_WINDOW_MIN_SPREAD_7D  — require ≥7 days of observation history to compute
#                                w7 daily-rate vs long-run daily-rate comparison.
#   _SA_WINDOW_MIN_SPREAD_3D  — require ≥4 days to compare w3 vs w7 daily rates.
#   _SA_W90_BASE_MIN          — minimum cumulative window score (all obs ≤90d) for a
#                                ticker to be considered "established" via history.
#   _SA_W7_DAILY_RATE_MULT    — 7d daily rate must be ≥ this × historical daily rate.
#   _SA_W3_DAILY_RATE_MULT    — 3d daily rate must be ≥ this × 7d daily rate
#                                (catches late-breaking surges within the 7d window).
_SA_WINDOW_MIN_SPREAD_7D:  float = 7.0
_SA_WINDOW_MIN_SPREAD_3D:  float = 4.0
_SA_W90_BASE_MIN:          float = 0.30
_SA_W7_DAILY_RATE_MULT:    float = 1.25
_SA_W3_DAILY_RATE_MULT:    float = 1.20

# XC: minimum number of bullish accounts required for "shared conviction"
_XC_MIN_ACCOUNTS: int = 2

# ── Freshest Alpha — source accounts (explicit 8-account subset) ──────────────
# These are the ONLY accounts used to populate Freshest Alpha.
# Only bullish mentions within the past _FA_MAX_RECENCY_DAYS are eligible.
# No other social sections read this set; it does not disturb the broader
# account universe used for XC / SA / Theme Leadership.
_FA_SOURCE_ACCOUNTS: frozenset[str] = frozenset({
    "aleabitoreddit",
    "PepInvestStocks",
    "Kaizen_Investor",
    "yianisz",
    "Ren_aramb",
    "FinnStockinger",
    "napoleon21st",
    "TheStockDon",
})

# Maximum recency in days for a mention to qualify for Freshest Alpha.
# 24-hour window: recency_days ≤ 1 (0 = today, 1 = yesterday / within 24 h).
_FA_MAX_RECENCY_DAYS: int = 1

# Recency decay weights for the 0–5 day window.
# More recent = higher weight; anything beyond _FA_MAX_RECENCY_DAYS is dropped.
_FA_SOURCE_RECENCY_BOOSTS: list[tuple[int, float]] = [
    (0, 3.0),   # today
    (1, 2.5),   # yesterday
    (2, 2.0),
    (3, 1.5),
    (4, 1.0),
    (5, 0.7),
]


def _fa_source_recency_boost(days: int) -> float:
    for bound, w in _FA_SOURCE_RECENCY_BOOSTS:
        if days <= bound:
            return w
    return 0.0   # beyond _FA_MAX_RECENCY_DAYS — caller already filters these out


# ── Freshest Alpha scoring (legacy — used by old classify-gated path) ─────────

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

def _load_snapshots() -> tuple[Optional[dict], Optional[dict], dict]:
    from services.x_consensus_cache import _load_disk_cache, _load_prior_cache, load_ticker_history
    return _load_disk_cache(), _load_prior_cache(), load_ticker_history()


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


# ── SA multi-window signal ────────────────────────────────────────────────────

def _sa_window_signal(
    ticker: str,
    ticker_history: Optional[dict],
    prior_raw: float,
    cur_raw: float,
    now_ts: float,
) -> tuple[bool, bool]:
    """Compute (has_prior_base, is_accel) using the best available temporal signal.

    Uses ticker history observations (timestamped past scans) to build rolling
    per-day rate comparisons across windows: w3 / w7 / w90.  The quality of the
    signal improves automatically as history accumulates over real calendar days.

    Window selection priority:

    1. 7d+ spread:  Compare 7d daily rate vs historical daily rate (long baseline).
                    Also check 3d daily rate vs 7d daily rate as a surge booster.
                    This is the "90d baseline → 7d intensity → 3d intensity" signal
                    the user described — fire SA when recent week is proportionally
                    stronger than the established long-run rate.

    2. 4d+ spread:  Compare 3d daily rate vs 7d daily rate.
                    Catches "getting hotter this week vs last few days" when we
                    don't yet have a full 7-day window split.

    3. Fallback:    Single-snapshot accel_ratio (current / prior raw score).
                    Used when history is too new to have temporal spread.
                    This is the minimum viable signal — always available.

    has_prior_base is broadened beyond "appears in immediately prior snapshot":
      A ticker qualifies as established if its cumulative window score (w90) is
      above _SA_W90_BASE_MIN — i.e. it has been actively mentioned across multiple
      past scans, even if it temporarily fell below the save threshold in the
      latest prior snapshot.
    """
    obs_list = (ticker_history or {}).get(ticker, [])
    if obs_list:
        # Age each observation in fractional days from now
        aged = [
            (max(0.0, (now_ts - float(o["t"])) / 86400.0), float(o.get("r", 0) or 0))
            for o in obs_list
        ]
        spread = max((d for d, _ in aged), default=0.0)

        def wsum(max_d: float) -> float:
            return sum(r for d, r in aged if d <= max_d)

        if spread >= _SA_WINDOW_MIN_SPREAD_7D:
            # ── 7d-spread path: primary multi-window gate ──────────────────
            w3  = wsum(3.0)
            w7  = wsum(7.0)
            w90 = wsum(90.0)    # all obs if total span < 90 days

            has_pb = (
                (w90  >= _SA_W90_BASE_MIN)
                or (prior_raw >= _FA_PRIOR_PRESENCE_THRESHOLD)
            )

            # Compare 7d daily rate vs long-run daily rate
            # base_daily uses actual span so a 10-day history isn't treated as 90d
            base_daily = w90 / max(spread, 0.1)
            w7_daily   = w7  / 7.0

            accel = w7_daily >= base_daily * _SA_W7_DAILY_RATE_MULT

            # Boost: 3d daily rate surging above 7d daily rate
            if not accel and w7 > 0:
                w3_daily = w3 / 3.0
                if w3_daily >= (w7 / 7.0) * _SA_W3_DAILY_RATE_MULT:
                    accel = True

            return has_pb, accel

        if spread >= _SA_WINDOW_MIN_SPREAD_3D:
            # ── 4-7d spread path: w3 vs w7 rate comparison ────────────────
            w3 = wsum(3.0)
            w7 = wsum(7.0)   # includes obs between 3-7 days ago

            has_pb = (
                (w7   >= _SA_W90_BASE_MIN)
                or (prior_raw >= _FA_PRIOR_PRESENCE_THRESHOLD)
            )

            if w7 > 0:
                w3_daily = w3 / 3.0
                w7_daily = w7 / 7.0
                accel    = w3_daily >= w7_daily * _SA_W3_DAILY_RATE_MULT
            else:
                accel = False

            return has_pb, accel

    # ── Fallback: single-snapshot accel_ratio ──────────────────────────────
    # Used when history has < 4 days of spread (e.g. system just started,
    # or all scans happened within the same day).
    has_pb = prior_raw >= _FA_PRIOR_PRESENCE_THRESHOLD
    accel  = (cur_raw / (prior_raw + 0.01)) >= _SA_ACCEL_RATIO_MIN
    return has_pb, accel


# ── Unified classifier ────────────────────────────────────────────────────────

def _classify_tickers_for_sections(
    backend_ranked: list[dict],
    prior_br_map: dict[str, dict],
    buzz_map: dict[str, str],
    ticker_history: Optional[dict] = None,
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

    When prior_br_map has data (rich path — history-driven):
      is_novel       = prior_raw < FA_PRIOR_PRESENCE_THRESHOLD
                       (brand-new or very low historical presence)
      has_prior_base,
      is_accel       = _sa_window_signal(ticker, ticker_history, prior_raw, cur_raw, now)
                       Multi-window signal — see _sa_window_signal() docstring.
                       Priority: 7d-spread → 4d-spread → single-snapshot accel_ratio.
                       No Grok buzz_trend used anywhere in the rich path.
      XC requires:   NOT fa AND NOT sa AND has_prior_base AND accts >= XC_MIN_ACCOUNTS
                     Established + multi-account + not novel + not accelerating.

    When prior_br_map is empty (degraded path — last-resort proxies only):
      is_novel       = accts == 1 OR buzz in FA_BUZZ_NOVEL
      has_prior_base = buzz == "Accelerating"   (strongest Grok signal only)
      is_accel       = has_prior_base
      XC:            NOT fa AND NOT sa AND accts >= XC_MIN_ACCOUNTS
    """
    prior_has_data = len(prior_br_map) > 0
    now            = time.time()
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

        # History supplement: if the ticker is missing from the immediately prior
        # snapshot but has appeared in multiple past scans (history), use its
        # historical mean as prior_raw.  This prevents an established ticker that
        # temporarily fell below the save threshold from being mis-labelled as novel.
        if prior_raw == 0.0 and ticker_history:
            hist_obs = [o for o in (ticker_history.get(ticker) or [])
                        if float(o.get("r", 0)) >= _FA_PRIOR_PRESENCE_THRESHOLD]
            if len(hist_obs) >= 2:
                prior_raw = sum(float(o["r"]) for o in hist_obs) / len(hist_obs)

        accel_ratio   = cur_raw / (prior_raw + 0.01)

        if prior_has_data:
            # ── Rich path: multi-window history drives classification ──────────
            # Novelty is still based on prior_raw (supplemented from history above).
            is_novel = prior_raw < _FA_PRIOR_PRESENCE_THRESHOLD
            # SA has_prior_base and is_accel use the best available window signal.
            # _sa_window_signal() selects: 7d-spread → 4d-spread → accel_ratio.
            # Grok buzz_trend is not consulted here.
            has_prior_base, is_accel = _sa_window_signal(
                ticker, ticker_history, prior_raw, cur_raw, now
            )
        else:
            # ── Degraded path: no prior data — proxy signals only ─────────────
            # Use only the strongest Grok buzz signal ("Accelerating") as proxy
            # for "has prior history". "Rising"/"Stable" are ambiguous; only
            # "Accelerating" implies Grok observed repeated multi-scan momentum.
            is_novel       = (accts == 1) or (buzz in _FA_BUZZ_NOVEL)
            has_prior_base = (buzz in _SA_BUZZ_ACCEL)
            is_accel       = has_prior_base

        # Priority 1 — Freshest Alpha
        # low/no prior history + top-tier source + fresh recency
        if has_top_qual and rec <= _FA_RECENCY_CUTOFF and is_novel:
            classes[ticker] = "fa"
            continue

        # Priority 2 — Sentiment Acceleration
        # had meaningful prior presence AND is measurably stronger now
        if has_prior_base and is_accel:
            classes[ticker] = "sa"
            continue

        # Priority 3 — X Consensus
        # established (has prior presence) + multi-account + not novel + not accelerating
        # In the rich path: stable or declining established multi-account conviction.
        # In the degraded path: multi-account not captured by FA or SA.
        if accts >= _XC_MIN_ACCOUNTS and (has_prior_base or not prior_has_data):
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

        # Only exclude tickers captured by Freshest Alpha (they have a dedicated
        # section that is more precise).  SA, XC, and unclassified tickers all
        # belong here — X Consensus reflects Grok's curated overall conviction
        # picture, not a narrowly filtered residual.
        section = classified.get(ticker, "xc")
        if section == "fa":
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

def _build_fa_from_source_accounts(
    current_snap: Optional[dict],
) -> dict:
    """
    Freshest Alpha — primary path.

    Sources ONLY the 8 accounts defined in _FA_SOURCE_ACCOUNTS.
    Eligibility: bullish sentiment + recency_days <= _FA_MAX_RECENCY_DAYS.
    No classifier gate — if these accounts positively mention a ticker recently,
    it appears here regardless of broader consensus or prior-snapshot history.

    Ranking factors:
      1. Recency: more recent → higher weight (see _FA_SOURCE_RECENCY_BOOSTS)
      2. Account weight: from X_SELECT_ACCOUNTS (top_trader=1.0, above_avg=0.8)
      3. Conviction: high=×1.3, medium=×1.0, low=×0.7
      4. Lower-cap urgency boost: ×1.5 for tickers NOT in the major consensus set
         (i.e. not in consensus_picks with hype_score≥75 AND trader_count≥2).
         This is data-driven — no ticker names hardcoded.

    Returns the standard FA dict shape:
      {"trades": [...], "spotlight": {...}}
    """
    from services.x_consensus_cache import _ACCOUNT_WEIGHT_BY_HANDLE

    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []
    raw = _raw(current_snap)

    # Build "major names" set — consensus tickers with high conviction + breadth.
    # Tickers NOT here are treated as lower-cap / non-market-leader candidates.
    major_names: set[str] = {
        (p.get("ticker") or "").upper().lstrip("$")
        for p in (raw.get("consensus_picks") or [])
        if isinstance(p, dict)
        and (p.get("hype_score") or 0) >= 75
        and (p.get("trader_count") or 0) >= 2
    }

    name_map = _name_lookup(current_snap)
    buckets: dict[str, dict] = {}

    for acct in mention_data:
        handle = acct.get("handle", "")
        if handle not in _FA_SOURCE_ACCOUNTS:
            continue

        acct_weight = float(
            _ACCOUNT_WEIGHT_BY_HANDLE.get(handle) or acct.get("weight") or 0.8
        )

        for m in (acct.get("mentions") or []):
            if not isinstance(m, dict):
                continue
            if (m.get("sentiment") or "neutral").lower() != "bullish":
                continue

            ticker = (m.get("ticker") or "").upper().strip().lstrip("$")
            if not ticker or len(ticker) > 12 or " " in ticker:
                continue

            rd_raw = m.get("recency_days")
            recency_days = int(rd_raw) if rd_raw is not None else 999
            recency_days = max(0, recency_days)
            if recency_days > _FA_MAX_RECENCY_DAYS:
                continue

            rec_boost   = _fa_source_recency_boost(recency_days)
            conviction  = (m.get("conviction") or "medium").lower()
            conv_mult   = {"high": 1.3, "medium": 1.0, "low": 0.7}.get(conviction, 1.0)
            mention_score = acct_weight * rec_boost * conv_mult

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
            b["score"]       += mention_score
            b["min_recency"]  = min(b["min_recency"], recency_days)
            b["accounts"][handle] = b["accounts"].get(handle, 0.0) + mention_score
            thesis = (m.get("thesis") or "").strip()
            if thesis:
                b["theses"].append({"handle": handle, "text": thesis})
            catalysts = [str(c) for c in (m.get("catalysts") or []) if c]
            b["catalysts"].extend(catalysts)
            b["conviction_seen"].add(conviction)

    if not buckets:
        return {"trades": [], "spotlight": None}

    results: list[dict] = []
    for ticker, b in buckets.items():
        if b["score"] <= 0 or b["min_recency"] > _FA_MAX_RECENCY_DAYS:
            continue

        n_accts = len(b["accounts"])

        # Lower-cap urgency boost: tickers outside the major-names set are likely
        # smaller/less-followed names — the exact use-case for this section.
        lower_cap_mult = 1.0 if ticker in major_names else 1.5

        # Breadth: multiple source accounts mentioning same ticker is a signal.
        breadth_mult = 1.0 + min(0.20 * (n_accts - 1), 0.40)

        final_score = b["score"] * lower_cap_mult * breadth_mult

        accts_sorted = sorted(b["accounts"].items(), key=lambda x: -x[1])
        name, tv_sym = name_map.get(ticker, ("", ""))
        top_thesis   = b["theses"][0]["text"] if b["theses"] else ""
        is_major     = ticker in major_names

        results.append({
            "ticker":              ticker,
            "name":                name,
            "tradingview_symbol":  tv_sym,
            "first_mentioned_by":  [f"@{h}" for h, _ in accts_sorted],
            "why_fresh": (
                f"{'Known' if is_major else 'Fresh'} call"
                f" from {n_accts} source account(s) within last "
                f"{b['min_recency']}d"
                + (f" — {', '.join(sorted(b['conviction_seen']))} conviction"
                   if b["conviction_seen"] else "")
            ),
            "entry_thesis":            top_thesis,
            "spotlight_badge":         False,
            "spotlight_signal":        None,
            "freshest_alpha_score":    round(final_score, 3),
            "min_recency_days":        b["min_recency"] if b["min_recency"] < 9999 else None,
            "quality_account_count":   n_accts,
            "is_brand_new":            not is_major,
            "novelty_mult":            round(lower_cap_mult, 2),
            "catalysts":               list(dict.fromkeys(b["catalysts"]))[:5],
            "top_accounts": [
                {"handle": h, "contribution": round(s, 3)} for h, s in accts_sorted
            ],
        })

    return _finalise_fa(results)


def _build_freshest_alpha(
    current_snap: Optional[dict],
    prior_snap: Optional[dict],
    classified: dict[str, str],
) -> dict:
    """
    Surface fresh ticker calls from the 8 specified FA source accounts.

    Primary path: _build_fa_from_source_accounts()
      Filters _mention_data to _FA_SOURCE_ACCOUNTS, bullish, recency <= 5 days.
      No classifier gate — FA is purely account-driven, not consensus-driven.

    Fallback (no mention data at all):
      Uses _backend_ranked directly with the classified 'fa' gate.
      This only fires when Grok returned no per-mention extraction.
    """
    mention_data: list[dict] = (current_snap or {}).get("_mention_data") or []

    if mention_data:
        return _build_fa_from_source_accounts(current_snap)

    # _backend_ranked fallback — only when Grok returned no per-mention data
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

            # Mutual exclusion: only 'fa' classified tickers.
            # Default 'none' for tickers outside _backend_ranked top-N
            # (e.g. rank 31+): they haven't been through the historical gate.
            if classified.get(ticker, "none") != "fa":
                continue

            rd_raw       = m.get("recency_days")
            recency_days = int(rd_raw) if rd_raw is not None else 10
            recency_days = max(0, min(recency_days, 365))
            if recency_days > 10:
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
    ("w10", 10),   # full 10-day lookback window (replaces w14/w30/w90)
]


def _sa_consensus_strength(account_count: int, accel_score: float) -> str:
    if account_count >= 4 and accel_score >= 5.0:
        return "Very High"
    if account_count >= 3 or accel_score >= 3.0:
        return "High"
    if account_count >= 2 or accel_score >= 1.5:
        return "Medium"
    return "Low"


def _sa_buzz_trend(slope_7_to_3: float, slope_10_to_7: float, w3_vs_w10: float = 0.0) -> str:
    if slope_7_to_3 >= 1.5 and slope_10_to_7 >= 1.2:
        return "Accelerating"
    if w3_vs_w10 >= 0.6 and slope_10_to_7 >= 0.6:
        return "Accelerating"
    if slope_7_to_3 >= 1.2 or w3_vs_w10 >= 0.7:
        return "Rising"
    if slope_7_to_3 >= 0.8 or w3_vs_w10 >= 0.35:
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

            # Mutual exclusion: only 'sa' classified tickers.
            # Default 'none' for tickers outside _backend_ranked top-N.
            if classified.get(ticker, "none") != "sa":
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
                    "w3": 0.0, "w7": 0.0, "w10": 0.0,
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
        w3, w7, w10 = b["w3"], b["w7"], b["w10"]

        if w10 <= 0:
            continue

        # SA requires signal beyond the freshest 3-day window — thin single-account
        # data with nothing in the 4-10 day range is not meaningful acceleration.
        if w7 == 0 and len(b["accounts"]) == 1:
            continue

        slope_7_to_3  = w3  / (w7  + 0.01)
        slope_10_to_7 = w7  / (w10 + 0.01)

        base_intensity = w3 * 4.0 + w7 * 2.0 + w10 * 1.0

        accel_bonus = 0.0
        if slope_7_to_3  > 0.5:
            accel_bonus += 0.3 * min(slope_7_to_3, 2.0)
        if slope_10_to_7 > 0.5:
            accel_bonus += 0.2 * min(slope_10_to_7, 2.0)

        n_accts      = len(b["accounts"])
        breadth_mult = 1.0 + min(0.15 * (n_accts - 1), 0.45)
        final_accel  = base_intensity * (1.0 + accel_bonus) * breadth_mult

        parts: list[str] = []
        if slope_7_to_3 >= 1.5 and slope_10_to_7 >= 1.2:
            parts.append("Rapidly accelerating — stronger each window")
        elif slope_10_to_7 >= 1.2:
            parts.append("Building momentum (last 7d > 10d baseline)")
        elif slope_7_to_3 >= 1.2:
            parts.append("Late surge — 3d hotter than 7d")
        if n_accts >= 3:
            parts.append(f"{n_accts} accounts in consensus")
        elif n_accts == 2:
            parts.append("Cross-account agreement")

        top_thesis   = b["theses"][0]["text"] if b["theses"] else ""
        prior        = prior_br_by_ticker.get(ticker)
        prior_acct   = prior["bullish_account_count"] if prior else 0
        norm_score   = min(round(final_accel * 2.0), 100)
        w3_vs_w10    = w3 / (w10 + 0.01)
        buzz_trend   = _sa_buzz_trend(slope_7_to_3, slope_10_to_7, w3_vs_w10)
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
                "w3":  round(w3,  3),
                "w7":  round(w7,  3),
                "w10": round(w10, 3),
            },
            "slope_7_to_3":              round(slope_7_to_3,  2),
            "slope_10_to_7":             round(slope_10_to_7, 2),
            "w3_vs_w10":                 round(w3_vs_w10, 2),
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
                "w3": 0.0, "w7": 0.0, "w10": 0.0,
            },
            "slope_7_to_3":  0.0,
            "slope_10_to_7": 0.0,
            "w3_vs_w10":     0.0,
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

# Sections hot-cache: keyed by the snapshot's _saved_at epoch float.
# Stores only the expensive-to-compute section payloads (classifier output +
# all four section builders).  The time-sensitive parts (_public_payload and
# _build_metadata) are always recomputed fresh so fields like
# refresh_in_progress, age_seconds, and window_open are never stale.
# Invalidated automatically when _saved_at changes (i.e. new XAI scan).
_SECTIONS_CACHE: dict = {}   # {"saved_at": float, "data": dict}


def build_x_dashboard() -> dict:
    """
    Build the Social X-dashboard payload from cached snapshots only.
    Zero Grok/XAI calls.

    Orchestration:
      1. Load current snapshot (hot-cached — no JSON re-parse if file unchanged).
      2. Check sections cache keyed by _saved_at — skip classifier + builders
         when the snapshot has not changed since the last call (~once per day).
      3. On cache miss: load prior snapshot + ticker history, run the unified
         classifier, build all four sections, and store in sections cache.
      4. Always recompute _public_payload + _build_metadata fresh so
         time-sensitive fields (refresh_in_progress, age_seconds, etc.) reflect
         the current moment.

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
        _load_disk_cache,
        _load_prior_cache,
        load_ticker_history,
        _ASKLIVERMORE_FALLBACK,
    )

    # ── Load current snapshot (mtime hot-cached — no JSON re-parse if unchanged) ─
    current_snap  = _load_disk_cache()
    snap_saved_at = float((current_snap or {}).get("_saved_at") or 0.0)
    cur_raw       = _raw(current_snap)

    # Time-sensitive home payload + metadata are always recomputed fresh.
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

    # ── Sections hot-cache check ──────────────────────────────────────────────
    # When the XAI snapshot has not changed, skip the classifier and all four
    # section builders entirely.  Only prior_snap and ticker_history are not
    # loaded in this path (they are only needed for the full build below).
    if snap_saved_at > 0 and _SECTIONS_CACHE.get("saved_at") == snap_saved_at:
        return {
            **home_payload,
            **_SECTIONS_CACHE["data"],
            "metadata": _build_metadata(current_snap),
        }

    # ── Full build — only runs when snapshot actually changes (~once per day) ──
    prior_snap     = _load_prior_cache()
    ticker_history = load_ticker_history()

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
    classified = _classify_tickers_for_sections(
        backend_ranked, prior_br_map, buzz_map, ticker_history
    )

    # ── Step 3: Build sections (each respects classified map) ─────────────────
    x_consensus_data            = _build_x_consensus(cur_raw, classified)
    freshest_alpha_data         = _build_freshest_alpha(current_snap, prior_snap, classified)
    theme_leadership_data       = _build_theme_leadership(cur_raw)
    sentiment_acceleration_data = _build_sentiment_accel(current_snap, prior_snap, classified)

    # ── Section-status + cache-status (reliability diagnostics) ───────────────
    _lkg_sections: list[str] = list((current_snap or {}).get("_lkg_sections_used") or [])
    _lkg_set: set[str] = set(_lkg_sections)

    _UI_CACHE_DEPS: dict[str, set[str]] = {
        "x_consensus":            {"_backend_ranked", "consensus_picks"},
        "freshest_alpha":         {"_mention_data", "_backend_ranked"},
        "theme_leadership":       {"consensus_picks"},
        "sentiment_acceleration": {"_mention_data"},
    }

    def _sec_status(section_key: str, data) -> str:
        if isinstance(data, list):
            has_data = len(data) > 0
        elif isinstance(data, dict):
            has_data = bool(
                data.get("trades") or data.get("themes") or data.get("market_pulse")
            )
        else:
            has_data = bool(data)
        if not has_data:
            return "empty"
        deps = _UI_CACHE_DEPS.get(section_key, set())
        if deps & _lkg_set:
            return "lkg"
        return "ok"

    section_status = {
        "x_consensus":            _sec_status("x_consensus",            x_consensus_data),
        "freshest_alpha":         _sec_status("freshest_alpha",         freshest_alpha_data),
        "theme_leadership":       _sec_status("theme_leadership",       theme_leadership_data),
        "sentiment_acceleration": _sec_status("sentiment_acceleration", sentiment_acceleration_data),
    }
    cache_status = (
        "no_data"     if not current_snap else
        "lkg_partial" if _lkg_set         else
        "ok"
    )

    # ── Ask Livermore signal — additive pass-through from snapshot ────────────
    _al_signal = (current_snap or {}).get("ask_livermore_signal")
    if not isinstance(_al_signal, dict):
        _al_signal = dict(_ASKLIVERMORE_FALLBACK)

    # Sections payload: stable, cached keyed by snapshot _saved_at
    sections_data = {
        "market_pulse":             cur_raw.get("market_pulse"),
        "portfolio_bias":           cur_raw.get("portfolio_bias"),
        "spotlight":                cur_raw.get("spotlight"),
        "x_consensus":              x_consensus_data,
        "freshest_alpha":           freshest_alpha_data,
        "theme_leadership":         theme_leadership_data,
        "sentiment_acceleration":   sentiment_acceleration_data,
        "ask_livermore_signal":     _al_signal,
        "section_status":           section_status,
        "cache_status":             cache_status,
        "lkg_sections":             sorted(_lkg_set),
    }

    # Store in sections cache — invalidated automatically when _saved_at changes
    _SECTIONS_CACHE["saved_at"] = snap_saved_at
    _SECTIONS_CACHE["data"]     = sections_data
    print(
        f"[SOCIAL_X] sections cache updated saved_at={snap_saved_at} "
        f"xc={len(x_consensus_data)} fa_trades="
        f"{len((freshest_alpha_data or {}).get('trades') or [])} "
        f"sa={len(sentiment_acceleration_data)}"
    )

    return {
        **home_payload,
        **sections_data,
        "metadata": _build_metadata(current_snap),
    }
