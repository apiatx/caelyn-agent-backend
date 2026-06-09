"""
Bi-hourly cached X "Select Trader Consensus" snapshot.

Architecture:
  Phase 1 (parallel batches): Grok searches X and returns structured per-account
    mention data (ticker, sentiment, recency_days, conviction, thesis, catalysts).
  Backend scoring: deterministic weighted engine aggregates Phase 1 data and
    produces a ranked ticker list using tier weights × recency × conviction × breadth.
  Phase 2 (1 synthesis call): Grok writes thesis text / schema fields using the
    backend-determined rank order — it does NOT re-rank.

This ensures top_trader accounts have real numeric influence, recency is
prioritised via explicit decay buckets, and fresh/hidden names surface instead
of only the most-obvious tickers.

Refresh window: 08:00–20:00 America/Chicago only.
"""
from __future__ import annotations

import asyncio
import json
import re as _re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python <3.9 fallback

# ── Canonical account universe (27 accounts, 6 categories) ────────────────
# SOURCE OF TRUTH — do not edit individual handles without user approval.
# The Social page `/api/social/query` imports X_SELECT_HANDLES (derived below).
X_SELECT_ACCOUNTS: list[dict] = [
    # ── Macro / Big Picture — used for market_pulse only, NOT ticker ranking ─
    {"handle": "KobeissiLetter",  "category": "macro_big_picture",    "weight": 0.0},
    # ── Top Traders — highest conviction (weight 1.0) ────────────────────────
    {"handle": "aleabitoreddit",  "category": "top_trader",           "weight": 1.0},
    {"handle": "PepInvestStocks", "category": "top_trader",           "weight": 1.0},
    {"handle": "Kaizen_Investor", "category": "top_trader",           "weight": 1.0},
    {"handle": "yianisz",         "category": "top_trader",           "weight": 1.0},
    {"handle": "FinnStockinger",  "category": "top_trader",           "weight": 1.0},
    {"handle": "UncleAlpha007",   "category": "top_trader",           "weight": 1.0},
    {"handle": "Mike10947310",    "category": "top_trader",           "weight": 1.0},
    # ── Above Average Traders — second tier (weight 0.8) ─────────────────────
    {"handle": "crux_capital_",   "category": "above_average_trader", "weight": 0.8},
    {"handle": "HyperTechInvest", "category": "above_average_trader", "weight": 0.8},
    {"handle": "ThematicTrader",  "category": "above_average_trader", "weight": 0.8},
    {"handle": "JonkooTrades",    "category": "above_average_trader", "weight": 0.8},
    {"handle": "Ren_aramb",       "category": "above_average_trader", "weight": 0.8},
    {"handle": "napoleon21st",    "category": "above_average_trader", "weight": 0.8},
    {"handle": "TheStockDon",     "category": "above_average_trader", "weight": 0.8},
    # ── Investment Themes + Datapoints + Stock Lists (weight 0.55) ───────────
    {"handle": "equitydd",        "category": "theme_datapoints",     "weight": 0.55},
]

# Flat handle list derived from the structured config — preserves backward
# compatibility with all code that imports X_SELECT_HANDLES.
X_SELECT_HANDLES: list[str] = [a["handle"] for a in X_SELECT_ACCOUNTS]

# Fast per-handle lookups
_ACCOUNT_WEIGHT_BY_HANDLE: dict[str, float]    = {a["handle"]: a["weight"]   for a in X_SELECT_ACCOUNTS}
_ACCOUNT_CATEGORY_BY_HANDLE: dict[str, str]    = {a["handle"]: a["category"] for a in X_SELECT_ACCOUNTS}

# ── Tier weights (used by backend scoring engine) ─────────────────────────
_TIER_WEIGHTS: dict[str, float] = {
    "top_trader":          1.00,
    "above_average_trader": 0.80,
    "theme_datapoints":    0.55,
    "thematic_investor":   0.45,
    "retail_trader":       0.35,
    "macro_big_picture":   0.0,   # excluded from ticker ranking
}

# ── Recency decay buckets: (upper_bound_days_inclusive, weight) ───────────
# Most-recent → highest weight; 3-month-old → very small weight.
_RECENCY_BUCKETS: list[tuple[int, float]] = [
    (1,  1.00),   # today / yesterday
    (3,  0.80),   # last 3 days
    (7,  0.60),   # past week
    (14, 0.40),   # past 2 weeks
    (30, 0.20),   # past month
    (90, 0.10),   # past 3 months
]
_RECENCY_FALLBACK = 0.05  # older than 3 months

# Disk cache paths — current snapshot + immediately prior snapshot for delta math.
_CACHE_PATH         = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_PRIOR_CACHE_PATH   = Path(__file__).parent.parent / "data" / "x_consensus_weekly_prior.json"
# Rolling per-ticker raw_score history — accumulated across N past snapshots.
# Provides a multi-scan baseline so the classifier can detect long-term establishment
# even when a ticker temporarily drops out of the current top-ranked slice.
_TICKER_HISTORY_PATH = Path(__file__).parent.parent / "data" / "x_consensus_ticker_history.json"
_TICKER_HISTORY_MAX_OBS = 10   # keep last N scored observations per ticker
_CACHE_TTL_SECONDS = 23 * 3600  # 23 hours — refreshed once daily at noon; fresh all day
_BATCH_SIZE = 8                 # accounts per Phase-1 batch (focused x_search per group)
_PHASE1_CONCURRENCY = 2         # max concurrent Grok batch calls
_PHASE1_TIMEOUT = 120.0         # seconds — 8-handle x_search batches routinely take 60-70 s

# ── Section-level validation minimums ────────────────────────────────────────
# A new snapshot must clear these thresholds per section before the cache is
# overwritten.  If a section is below minimum the last-known-good value is
# merged in instead of discarding it (LKG-merge behaviour).
_SECTION_MIN_BACKEND_RANKED:  int = 3   # scored tickers in _backend_ranked
_SECTION_MIN_MENTION_DATA:    int = 1   # per-account records in _mention_data
_SECTION_MIN_CONSENSUS_PICKS: int = 1   # items in raw.consensus_picks
_SECTION_MIN_TOP_TICKERS:     int = 1   # items in top_tickers

# Rolling per-scan diagnostics log — persisted to disk so ops can inspect
# what happened on the last N scans without reading server logs.
_SCAN_DIAGNOSTICS_PATH = Path(__file__).parent.parent / "data" / "social_scan_diagnostics.json"
_SCAN_DIAGNOSTICS_MAX  = 30  # keep last N entries

# Module-level lock so only one background refresh runs at a time across the
# whole process, regardless of how many Home requests land simultaneously.
_REFRESH_LOCK = asyncio.Lock()

# ── Refresh window: 08:00–18:00 America/Chicago, DST-safe (10-hour window) ─
_REFRESH_TZ = ZoneInfo("America/Chicago")
_WINDOW_START_HOUR = 8   # 08:00 Chicago
_WINDOW_END_HOUR   = 18  # 18:00 Chicago (exclusive) — narrowed from 20:00

# ── Manual-refresh cooldown ───────────────────────────────────────────────
# Prevents overnight spam while still allowing occasional user-initiated
# overrides.  Module-level float (epoch seconds); None means never run.
_MANUAL_COOLDOWN_SECONDS: int = 30 * 60  # 30 minutes
_last_manual_refresh_at: Optional[float] = None


def _next_manual_allowed_iso() -> Optional[str]:
    """ISO-8601 UTC timestamp when the next manual refresh is permitted.

    Returns None if no manual refresh has ever been run (i.e. immediately
    available).
    """
    global _last_manual_refresh_at
    if _last_manual_refresh_at is None:
        return None
    next_ts = _last_manual_refresh_at + _MANUAL_COOLDOWN_SECONDS
    dt = datetime.fromtimestamp(next_ts, tz=timezone.utc)
    return dt.isoformat()


def _manual_refresh_available() -> bool:
    """True if the cooldown window has passed (or never been set)."""
    global _last_manual_refresh_at
    if _last_manual_refresh_at is None:
        return True
    return (time.time() - _last_manual_refresh_at) >= _MANUAL_COOLDOWN_SECONDS


def _in_refresh_window() -> bool:
    """Return True only if current America/Chicago time is 08:00–19:59."""
    now_ct = datetime.now(_REFRESH_TZ)
    return _WINDOW_START_HOUR <= now_ct.hour < _WINDOW_END_HOUR


def _next_window_open_iso() -> str:
    """
    ISO-8601 timestamp (UTC) of the next 08:00 America/Chicago open.

    If we are currently before 08:00 today, that is still today's open.
    If we are at or after 20:00, the next open is tomorrow at 08:00.
    """
    now_ct = datetime.now(_REFRESH_TZ)
    # Choose today or tomorrow depending on where we are in the day
    if now_ct.hour < _WINDOW_START_HOUR:
        target_date = now_ct.date()
    else:
        target_date = now_ct.date() + timedelta(days=1)
    # Build 08:00 Chicago in a DST-aware way (ZoneInfo handles fold/gap)
    next_open_ct = datetime(
        target_date.year, target_date.month, target_date.day,
        _WINDOW_START_HOUR, 0, 0,
        tzinfo=_REFRESH_TZ,
    )
    return next_open_ct.astimezone(timezone.utc).isoformat()


def _load_disk_cache() -> Optional[dict]:
    """Return the raw saved snapshot dict if it exists on disk, else None."""
    if not _CACHE_PATH.exists():
        return None
    try:
        raw = json.loads(_CACHE_PATH.read_text())
        return raw if isinstance(raw, dict) else None
    except Exception as e:
        print(f"[X_CONSENSUS] Cache read error: {e}")
        return None


def _load_prior_cache() -> Optional[dict]:
    """Return the previous snapshot dict if it exists on disk, else None."""
    if not _PRIOR_CACHE_PATH.exists():
        return None
    try:
        raw = json.loads(_PRIOR_CACHE_PATH.read_text())
        return raw if isinstance(raw, dict) else None
    except Exception as e:
        print(f"[X_CONSENSUS] Prior cache read error: {e}")
        return None


def _update_ticker_history(backend_ranked: list, saved_at: float) -> None:
    """Append the current snapshot's raw_scores to the rolling ticker history.

    Stores the last _TICKER_HISTORY_MAX_OBS observations per ticker so the
    classifier can detect long-term establishment without relying solely on
    the immediately prior snapshot.
    """
    try:
        history: dict = {}
        if _TICKER_HISTORY_PATH.exists():
            try:
                history = json.loads(_TICKER_HISTORY_PATH.read_text()) or {}
            except Exception:
                history = {}

        for bs in backend_ranked:
            ticker    = bs.get("ticker")
            raw_score = bs.get("raw_score")
            accts     = bs.get("bullish_account_count", 0)
            if not ticker or raw_score is None:
                continue
            obs = {"t": saved_at, "r": round(float(raw_score), 4), "a": accts}
            ticker_obs = history.get(ticker, [])
            ticker_obs.append(obs)
            # Keep only the most recent N observations
            history[ticker] = ticker_obs[-_TICKER_HISTORY_MAX_OBS:]

        _TICKER_HISTORY_PATH.write_text(json.dumps(history, indent=2))
    except Exception as e:
        print(f"[X_CONSENSUS] Ticker history write error: {e}")


def load_ticker_history() -> dict:
    """Return the rolling ticker history dict {ticker: [obs, ...]} from disk.

    Each observation: {"t": epoch_float, "r": raw_score, "a": acct_count}
    Returns empty dict if the file does not exist yet.
    """
    if not _TICKER_HISTORY_PATH.exists():
        return {}
    try:
        return json.loads(_TICKER_HISTORY_PATH.read_text()) or {}
    except Exception as e:
        print(f"[X_CONSENSUS] Ticker history read error: {e}")
        return {}


def _validate_snapshot_sections(snapshot: dict) -> dict[str, bool]:
    """Check which sections of a freshly-built snapshot meet minimum content
    thresholds.

    Returns {section_name: True/False}.
      True  — section has enough data; safe to overwrite cache.
      False — section is empty/below minimum; LKG value should be preserved.
    """
    br  = snapshot.get("_backend_ranked") or []
    md  = snapshot.get("_mention_data") or []
    raw = snapshot.get("raw") or {}
    cp  = raw.get("consensus_picks") or []
    ht  = snapshot.get("top_tickers") or []
    return {
        "_backend_ranked":  len(br) >= _SECTION_MIN_BACKEND_RANKED,
        "_mention_data":    len(md) >= _SECTION_MIN_MENTION_DATA,
        "consensus_picks":  len(cp) >= _SECTION_MIN_CONSENSUS_PICKS,
        "top_tickers":      len(ht) >= _SECTION_MIN_TOP_TICKERS,
    }


def _merge_lkg_sections(
    new_snap: dict,
    lkg_snap: Optional[dict],
    section_ok: dict[str, bool],
) -> list[str]:
    """Overwrite empty sections in new_snap with last-known-good values.

    Mutates new_snap in place.
    Returns list of section names that were restored from LKG (for logging/diagnostics).
    Only sections that failed validation are touched — sections that passed keep
    the fresh data from the new scan.
    """
    if not lkg_snap:
        return []

    merged: list[str] = []

    if not section_ok.get("_backend_ranked"):
        lkg_br = lkg_snap.get("_backend_ranked")
        if lkg_br:
            new_snap["_backend_ranked"] = lkg_br
            merged.append("_backend_ranked")
            print(
                f"[X_CONSENSUS][LKG] _backend_ranked preserved from prior "
                f"({len(lkg_br)} tickers)"
            )

    if not section_ok.get("_mention_data"):
        lkg_md = lkg_snap.get("_mention_data")
        if lkg_md:
            new_snap["_mention_data"] = lkg_md
            merged.append("_mention_data")
            print(
                f"[X_CONSENSUS][LKG] _mention_data preserved from prior "
                f"({len(lkg_md)} records)"
            )

    raw_new = new_snap.get("raw")
    raw_lkg = lkg_snap.get("raw")
    if isinstance(raw_new, dict) and isinstance(raw_lkg, dict):
        if not section_ok.get("consensus_picks"):
            lkg_cp = raw_lkg.get("consensus_picks")
            if lkg_cp:
                raw_new["consensus_picks"] = lkg_cp
                merged.append("consensus_picks")
                print(
                    f"[X_CONSENSUS][LKG] raw.consensus_picks preserved from prior "
                    f"({len(lkg_cp)} picks)"
                )

    if not section_ok.get("top_tickers"):
        lkg_ht = lkg_snap.get("top_tickers")
        if lkg_ht:
            new_snap["top_tickers"] = lkg_ht
            merged.append("top_tickers")
            print(
                f"[X_CONSENSUS][LKG] top_tickers preserved from prior "
                f"({len(lkg_ht)} tickers)"
            )

    return merged


def _append_scan_diagnostics(entry: dict) -> None:
    """Append one diagnostics entry to the rolling JSON log.

    Keeps the last _SCAN_DIAGNOSTICS_MAX entries.
    Never raises — diagnostics must never break the main refresh loop.
    """
    try:
        _SCAN_DIAGNOSTICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        existing: list = []
        if _SCAN_DIAGNOSTICS_PATH.exists():
            try:
                raw_text = _SCAN_DIAGNOSTICS_PATH.read_text()
                existing = json.loads(raw_text) or []
                if not isinstance(existing, list):
                    existing = []
            except Exception:
                existing = []
        existing.append(entry)
        existing = existing[-_SCAN_DIAGNOSTICS_MAX:]
        _SCAN_DIAGNOSTICS_PATH.write_text(json.dumps(existing, indent=2))
    except Exception as exc:
        print(f"[X_CONSENSUS] Diagnostics write error: {exc}")


def load_scan_diagnostics() -> list:
    """Return the rolling scan diagnostics list from disk (newest last).

    Returns an empty list if the file does not exist or is unreadable.
    Public — imported by the /api/social/diagnostics endpoint.
    """
    if not _SCAN_DIAGNOSTICS_PATH.exists():
        return []
    try:
        raw = json.loads(_SCAN_DIAGNOSTICS_PATH.read_text())
        return raw if isinstance(raw, list) else []
    except Exception as exc:
        print(f"[X_CONSENSUS] Diagnostics read error: {exc}")
        return []


def _save_disk_cache(data: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Rotate current → prior ONLY when current is a clean scan (no LKG sections).
        # This guarantees prior always holds the last genuinely full scan, so
        # future LKG merges always fall back to truly clean data even after
        # many consecutive bad runs.  A merged snapshot that rotated into prior
        # is still fine data-wise but we prefer the last clean one as anchor.
        if _CACHE_PATH.exists():
            try:
                _current_text = _CACHE_PATH.read_text()
                _current_json = json.loads(_current_text)
                _current_is_clean = not (_current_json.get("_lkg_sections_used") or [])
                if _current_is_clean:
                    _PRIOR_CACHE_PATH.write_text(_current_text)
                    print("[X_CONSENSUS] Prior snapshot updated (current was clean)")
                else:
                    print(
                        "[X_CONSENSUS] Prior snapshot preserved — current has LKG "
                        f"sections {_current_json.get('_lkg_sections_used')}; "
                        "keeping last clean prior intact"
                    )
            except Exception as e:
                print(f"[X_CONSENSUS] Prior cache rotate error: {e}")
        data["_saved_at"] = time.time()
        _CACHE_PATH.write_text(json.dumps(data, indent=2))
        # Update rolling per-ticker history for multi-scan historical signals.
        _update_ticker_history(
            data.get("_backend_ranked", []),
            data["_saved_at"],
        )
    except Exception as e:
        print(f"[X_CONSENSUS] Cache write error: {e}")


def _is_fresh(raw: Optional[dict]) -> bool:
    if not raw:
        return False
    saved = raw.get("_saved_at") or 0
    try:
        return (time.time() - float(saved)) < _CACHE_TTL_SECONDS
    except Exception:
        return False


async def _fetch_batch(
    batch_accounts: list[dict],
    data_service,
    since_date: str,
    batch_idx: int,
) -> str:
    """Phase-1 batch: one focused Grok x_search call for a small group of accounts.

    Passing only 8 handles to x_search_config means Grok searches each account
    thoroughly — the original approach that produced rich, accurate results.
    """
    handles = [a["handle"] for a in batch_accounts]
    account_lines = "\n".join(
        f"  - @{a['handle']} [{a['category']}]" for a in batch_accounts
    )
    prompt = (
        f"Search X/Twitter posts (since {since_date}) from these specific curated "
        f"trader accounts and return structured ticker mention data:\n\n"
        + account_lines
        + "\n\nFor EACH account listed above, extract ALL ticker and asset mentions. "
        "Return a JSON array — exactly one element per account — with this structure:\n"
        '[\n'
        '  {\n'
        '    "handle": "accounthandle",\n'
        '    "mentions": [\n'
        '      {\n'
        '        "ticker": "$NVDA",\n'
        '        "sentiment": "bullish",\n'
        '        "recency_days": 1,\n'
        '        "conviction": "high",\n'
        '        "thesis": "Short summary of their reasoning",\n'
        '        "catalysts": ["earnings beat", "AI demand"]\n'
        '      }\n'
        '    ]\n'
        '  }\n'
        ']\n\n'
        "Rules:\n"
        "- sentiment must be exactly 'bullish', 'bearish', or 'neutral'.\n"
        "- recency_days: best-estimate days since the post (0=today, 1=yesterday, 7=a week ago).\n"
        "- conviction: 'high', 'medium', or 'low' based on the trader's language and emphasis.\n"
        "- catalysts: list specific events/catalysts cited; empty array [] if none.\n"
        "- Include ALL tickers mentioned, even ETFs and sector plays.\n"
        "- If an account has no ticker mentions, include them with 'mentions': [].\n"
        "- Return ONLY the JSON array. No markdown fences, no explanation text."
    )

    try:
        result = await data_service.xai._call_grok_with_x_search(
            prompt=prompt,
            raw_mode=True,
            use_deep_model=False,
            timeout=_PHASE1_TIMEOUT,
            x_search_config={"allowed_x_handles": handles, "from_date": since_date},
            max_output_tokens=2000,
        )
    except Exception as e:
        print(f"[X_CONSENSUS] Batch {batch_idx} exception ({handles[0]}…): {e}")
        return ""

    text = ""
    if isinstance(result, dict):
        text = result.get("_raw_analysis", "") or result.get("error", "")

    if text and text.startswith("xAI"):
        print(
            f"[X_CONSENSUS] Batch {batch_idx} ({handles[0]}…) API/timeout error: {text[:120]}"
        )
        return ""

    print(
        f"[X_CONSENSUS] Batch {batch_idx}: {len(handles)} accounts "
        f"({handles[0]}…) → {len(text):,} chars"
    )
    return text


# ── Backend scoring helpers ───────────────────────────────────────────────


def _get_recency_weight(days: int) -> float:
    """Map an approximate age-in-days to a recency decay weight."""
    for threshold, weight in _RECENCY_BUCKETS:
        if days <= threshold:
            return weight
    return _RECENCY_FALLBACK


def _parse_batch_mentions(batch_text: str) -> list[dict]:
    """Extract the JSON array from a Phase-1 Grok response.

    Tries three strategies in order:
      1. Whole text is valid JSON.
      2. First '[' … last ']' span is valid JSON.
      3. Returns [] so the caller degrades gracefully.
    """
    if not batch_text:
        return []
    stripped = batch_text.strip()
    # Strategy 1 — whole text
    try:
        data = json.loads(stripped)
        if isinstance(data, list):
            return data
    except Exception:
        pass
    # Strategy 2 — find outermost [ … ] span
    start = stripped.find("[")
    end   = stripped.rfind("]")
    if start != -1 and end > start:
        try:
            data = json.loads(stripped[start:end + 1])
            if isinstance(data, list):
                return data
        except Exception:
            pass
    # Strategy 3 — no parseable JSON found
    print(f"[X_CONSENSUS] Batch parse fallback — no JSON in {len(batch_text)} chars (Phase-1 raw text kept for synthesis)")
    return []


def _backend_score_tickers(all_account_mentions: list[dict]) -> list[dict]:
    """Deterministic weighted scoring engine.

    Input: list of {handle, mentions: [{ticker, sentiment, recency_days,
           conviction, thesis, catalysts}]} from parsed Phase-1 output.

    Scoring formula per bullish mention:
      base  = tier_weight × recency_weight × conviction_mult × specificity_mult
    Final score per ticker:
      base_sum × breadth_mult × freshness_mult

    Returns ranked list (highest backend_score first) of ticker score dicts.
    macro_big_picture accounts are excluded entirely.
    Only bullish mentions contribute to ranking score.
    """
    # {ticker → aggregation bucket}
    buckets: dict[str, dict] = {}

    for acct in all_account_mentions:
        if not isinstance(acct, dict):
            continue
        handle   = acct.get("handle", "")
        category = _ACCOUNT_CATEGORY_BY_HANDLE.get(handle, "")
        tier_w   = _TIER_WEIGHTS.get(category, 0.0)
        if tier_w == 0.0:
            continue  # skip macro + unknowns

        for m in acct.get("mentions", []):
            if not isinstance(m, dict):
                continue
            raw_ticker = (m.get("ticker") or "").upper().strip().lstrip("$").strip()
            # Basic sanity checks — skip garbage tokens
            if not raw_ticker or len(raw_ticker) > 12 or " " in raw_ticker:
                continue
            sentiment = (m.get("sentiment") or "neutral").lower()
            if sentiment not in ("bullish", "bearish", "neutral"):
                sentiment = "neutral"
            if sentiment != "bullish":
                continue  # bearish/neutral don't inflate ranking score

            _recency_raw = m.get("recency_days")
            recency_days = int(_recency_raw) if _recency_raw is not None else 14
            recency_days = max(0, min(recency_days, 365))
            recency_w    = _get_recency_weight(recency_days)

            conviction      = (m.get("conviction") or "medium").lower()
            conviction_mult = {"high": 1.2, "medium": 1.0, "low": 0.7}.get(conviction, 1.0)

            catalysts        = [str(c) for c in (m.get("catalysts") or []) if c]
            specificity_mult = 1.2 if catalysts else 1.0

            mention_score = tier_w * recency_w * conviction_mult * specificity_mult

            if raw_ticker not in buckets:
                buckets[raw_ticker] = {
                    "ticker":                raw_ticker,
                    "raw_score":             0.0,
                    "bullish_accounts":      set(),
                    "account_contributions": {},
                    "min_recency":           9999,
                    "theses":                [],
                    "all_catalysts":         [],
                }
            b = buckets[raw_ticker]
            b["raw_score"]       += mention_score
            b["bullish_accounts"].add(handle)
            b["account_contributions"][handle] = (
                b["account_contributions"].get(handle, 0.0) + mention_score
            )
            b["min_recency"] = min(b["min_recency"], recency_days)
            thesis_text = (m.get("thesis") or "").strip()
            if thesis_text:
                b["theses"].append({
                    "handle":      handle,
                    "category":    category,
                    "tier_weight": tier_w,
                    "text":        thesis_text,
                })
            b["all_catalysts"].extend(catalysts)

    _TIER_ORDER = {
        "top_trader": 0, "above_average_trader": 1,
        "theme_datapoints": 2, "thematic_investor": 3, "retail_trader": 4,
    }

    results: list[dict] = []
    for ticker, b in buckets.items():
        unique_n = len(b["bullish_accounts"])
        if unique_n == 0:
            continue

        # Breadth bonus: +15% per additional unique bullish account, capped at +50%
        breadth_mult = 1.0 + min(0.15 * (unique_n - 1), 0.50)

        # Freshness boost: new call within 3 days → +30%, within 7 days → +10%
        min_rec = b["min_recency"]
        freshness_mult = 1.30 if min_rec <= 3 else (1.10 if min_rec <= 7 else 1.0)

        final_score = b["raw_score"] * breadth_mult * freshness_mult

        has_top_conviction = any(
            _ACCOUNT_CATEGORY_BY_HANDLE.get(h, "") in ("top_trader", "above_average_trader")
            for h in b["bullish_accounts"]
        )

        # Top-tier theses first
        sorted_theses = sorted(
            b["theses"],
            key=lambda t: (_TIER_ORDER.get(t["category"], 9), -t["tier_weight"]),
        )[:3]

        # Deduplicate catalysts (case-insensitive)
        seen: set[str] = set()
        deduped_cats: list[str] = []
        for c in b["all_catalysts"]:
            lc = c.lower().strip()
            if lc and lc not in seen:
                seen.add(lc)
                deduped_cats.append(c)

        top_accounts = sorted(
            [
                {
                    "handle":       h,
                    "category":     _ACCOUNT_CATEGORY_BY_HANDLE.get(h, ""),
                    "contribution": round(s, 3),
                }
                for h, s in b["account_contributions"].items()
            ],
            key=lambda x: -x["contribution"],
        )[:6]

        results.append({
            "ticker":                ticker,
            "backend_score":         round(final_score, 3),
            "raw_score":             round(b["raw_score"], 3),
            "breadth_score":         round(breadth_mult, 3),
            "freshness_score":       round(freshness_mult, 3),
            "recency_days_min":      min_rec if min_rec < 9999 else None,
            "bullish_account_count": unique_n,
            "has_top_conviction":    has_top_conviction,
            "top_accounts":          top_accounts,
            "thesis_fragments":      sorted_theses,
            "catalyst_list":         deduped_cats[:6],
        })

    # Sort: backend_score DESC, top_conviction as tiebreaker
    results.sort(key=lambda x: (-x["backend_score"], not x["has_top_conviction"]))
    return results


# ── Consensus normaliser ──────────────────────────────────────────────────


def _normalize_consensus(
    raw_result: Any,
    *,
    backend_scores: Optional[dict] = None,
) -> dict:
    """Convert the Grok synthesis response into the Home-shaped snapshot.

    backend_scores: optional {ticker: score_dict} from _backend_score_tickers.
    When provided, consensus_picks are RE-SORTED by backend_score and each
    entry is enriched with scoring metadata.  If absent, Grok's own order
    is preserved (safe fallback for when Phase-1 parsing yields nothing).

    Limits increased: top_tickers cap raised from 20 → 30.
    """
    if not isinstance(raw_result, dict):
        return {"top_tickers": [], "key_themes": [], "notable_accounts": []}

    picks = raw_result.get("consensus_picks") or []

    # Build enriched list, merging backend scores where available
    enriched: list[dict] = []
    for p in picks:
        if not isinstance(p, dict):
            continue
        symbol = (p.get("ticker") or p.get("symbol") or p.get("asset") or "").upper().lstrip("$").strip()
        if not symbol:
            continue
        bs = (backend_scores or {}).get(symbol)
        enriched.append({
            "_symbol":         symbol,
            "_backend_score":  bs["backend_score"] if bs else -1.0,
            "_bs":             bs,
            "_p":              p,
        })

    # Re-sort by backend score when available; otherwise keep Grok's order
    if backend_scores:
        enriched.sort(key=lambda x: -x["_backend_score"])

    top_tickers: list[dict] = []
    for e in enriched[:30]:   # increased cap: 20 → 30
        p  = e["_p"]
        bs = e["_bs"]
        entry: dict = {
            "symbol":    e["_symbol"],
            "mentions":  p.get("mention_count") or p.get("mentions") or p.get("count"),
            "sentiment": p.get("sentiment") or p.get("bias") or p.get("direction"),
            "rationale": p.get("thesis") or p.get("rationale") or p.get("summary") or "",
            "accounts":  p.get("accounts") or p.get("traders") or [],
        }
        if bs:
            entry.update({
                "backend_score":         bs.get("backend_score"),
                "recency_days_min":      bs.get("recency_days_min"),
                "bullish_account_count": bs.get("bullish_account_count"),
                "has_top_conviction":    bs.get("has_top_conviction"),
                "top_accounts":          bs.get("top_accounts"),
                "catalyst_list":         bs.get("catalyst_list"),
            })
        top_tickers.append(entry)

    key_themes_raw = raw_result.get("market_pulse") or raw_result.get("key_themes") or []
    if isinstance(key_themes_raw, str):
        key_themes = [key_themes_raw]
    elif isinstance(key_themes_raw, list):
        key_themes = [str(t) for t in key_themes_raw if t][:8]   # raised: 6 → 8
    else:
        key_themes = []

    accounts = raw_result.get("accounts_analyzed") or []
    notable_accounts = [str(a) for a in accounts if a][:27] if isinstance(accounts, list) else []

    return {
        "top_tickers":     top_tickers,
        "key_themes":      key_themes,
        "notable_accounts": notable_accounts,
        "raw":             raw_result,
    }


async def _run_refresh(data_service) -> Optional[dict]:
    """Multi-batch X consensus refresh — focused x_search per account group.

    Phase 1 (N batch calls, concurrent ≤ _PHASE1_CONCURRENCY):
      Each batch of _BATCH_SIZE accounts gets its own focused Grok x_search call.
      Passing only 8 handles per call means Grok searches each account thoroughly,
      producing rich, accurate per-account mention data.  Batches run concurrently
      (semaphore limits to _PHASE1_CONCURRENCY=2 at once) to minimise wall time.
    Backend scoring: deterministic engine aggregates Phase-1 data and produces
      a ranked ticker list (tier × recency × conviction × breadth).  Macro
      accounts (@KobeissiLetter) are excluded from ticker ranking.
    Phase 2 (1 synthesis call): Grok writes thesis text and all schema fields.
      It receives the backend-determined rank order and must follow it — it does
      NOT decide final ranking itself.

    Call count per refresh: ceil(27/8) + 1 = 4 + 1 = 5.
    With 4h TTL + 08:00–18:00 window ≈ 2–3 refreshes/day → ~10–15 calls/day.

    Fallback: if Phase-1 parsing yields no structured data (Grok returned prose),
      the combined raw text is still passed to Phase 2 unchanged.  No crash.
    """
    try:
        from agent.prompts import X_SELECT_TRADER_CONSENSUS_CONTRACT
    except Exception as e:
        print(f"[X_CONSENSUS] Could not import contract: {e}")
        return None

    if not data_service or not getattr(data_service, "xai", None):
        print("[X_CONSENSUS] No xAI provider — skipping refresh")
        return None

    _cat_counts = {}
    for a in X_SELECT_ACCOUNTS:
        _cat_counts[a["category"]] = _cat_counts.get(a["category"], 0) + 1

    batches = [
        X_SELECT_ACCOUNTS[i:i + _BATCH_SIZE]
        for i in range(0, len(X_SELECT_ACCOUNTS), _BATCH_SIZE)
    ]
    print(
        f"[X_CONSENSUS] Refresh starting — {len(X_SELECT_ACCOUNTS)} accounts "
        f"({_cat_counts.get('top_trader',0)} top, "
        f"{_cat_counts.get('above_average_trader',0)} above_avg, "
        f"{_cat_counts.get('retail_trader',0)} retail, "
        f"{_cat_counts.get('thematic_investor',0)} thematic, "
        f"{_cat_counts.get('theme_datapoints',0)} datapoints, "
        f"{_cat_counts.get('macro_big_picture',0)} macro) "
        f"— {len(batches)} focused Phase-1 batches (≤{_BATCH_SIZE} accts each, "
        f"concurrency={_PHASE1_CONCURRENCY})"
    )

    from datetime import datetime, timedelta, timezone as _tz
    since_date = (datetime.now(_tz.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

    # ── Phase 1: concurrent focused batch calls ────────────────────────────
    semaphore = asyncio.Semaphore(_PHASE1_CONCURRENCY)

    async def _guarded_batch(batch_accounts: list[dict], idx: int) -> str:
        async with semaphore:
            return await _fetch_batch(batch_accounts, data_service, since_date, idx)

    batch_texts: list[str] = await asyncio.gather(
        *[_guarded_batch(b, i + 1) for i, b in enumerate(batches)]
    )

    all_account_mentions: list[dict] = []   # for backend scoring
    combined_data: list[str] = []            # raw text for Phase-2 synthesis

    for idx, (batch_accounts, text) in enumerate(zip(batches, batch_texts), 1):
        if not text:
            print(f"[X_CONSENSUS] Batch {idx}: empty/error — skipping")
            continue
        handle_labels = ", ".join(f"@{a['handle']}" for a in batch_accounts)
        combined_data.append(f"=== Batch {idx} ({handle_labels}) ===\n{text}")
        parsed = _parse_batch_mentions(text)
        if parsed:
            all_account_mentions.extend(parsed)
        else:
            print(f"[X_CONSENSUS] Batch {idx}: no JSON — raw text kept for Phase-2")

    print(
        f"[X_CONSENSUS] Phase-1 complete: {len(combined_data)}/{len(batches)} batches "
        f"returned data, {len(all_account_mentions)} account records parsed"
    )

    if not combined_data:
        print("[X_CONSENSUS] Phase-1 returned nothing — aborting refresh (keep existing cache)")
        _append_scan_diagnostics({
            "scan_ts":            datetime.now(timezone.utc).isoformat(),
            "accounts_count":     len(X_SELECT_ACCOUNTS),
            "batch_count":        len(batches),
            "batches_returned":   0,
            "sections_ok":        [],
            "sections_missing":   ["_backend_ranked", "_mention_data", "consensus_picks", "top_tickers"],
            "lkg_sections_used":  [],
            "ticker_count":       0,
            "mention_records":    0,
            "consensus_picks":    0,
            "top_tickers":        0,
            "cache_write_status": "aborted_empty_phase1",
            "error":              "Phase-1 returned no data from any batch — existing cache kept",
        })
        return None

    # ── Backend scoring ───────────────────────────────────────────────────
    backend_ranked: list[dict] = []
    if all_account_mentions:
        backend_ranked = _backend_score_tickers(all_account_mentions)
        print(
            f"[X_CONSENSUS] Backend scoring: {len(all_account_mentions)} account records → "
            f"{len(backend_ranked)} scored tickers"
        )
        if backend_ranked:
            top5 = [(s["ticker"], s["backend_score"]) for s in backend_ranked[:5]]
            print(f"[X_CONSENSUS] Top-5 backend: {top5}")
    else:
        print("[X_CONSENSUS] No structured Phase-1 data — backend scoring skipped; using Grok-only rank")

    backend_score_by_ticker: dict[str, dict] = {s["ticker"]: s for s in backend_ranked}

    # ── Phase 2: synthesis (Grok writes text, backend determined order) ───
    combined_text = "\n\n".join(combined_data)
    print(f"[X_CONSENSUS] Synthesis phase: {len(combined_text):,} chars of raw data")

    # Build the rank preamble only when backend produced results
    if backend_ranked:
        top_for_synthesis = backend_ranked[:30]
        rank_lines = "\n".join(
            f"  {i + 1:2d}. ${s['ticker']}"
            f" [score={s['backend_score']:.2f}"
            f", {s['bullish_account_count']} accts"
            f", recency≤{s['recency_days_min']}d"
            + (", ⭐top_trader" if s["has_top_conviction"] else "")
            + "]"
            for i, s in enumerate(top_for_synthesis)
        )
        rank_preamble = (
            f"BACKEND PRE-RANKED TICKER ORDER "
            f"(deterministic: tier_weight × recency_decay × conviction × breadth — "
            f"{len(backend_ranked)} total tickers scored):\n"
            + rank_lines
            + "\n\n"
            "SYNTHESIS RULES:\n"
            "1. Output consensus_picks in EXACTLY the above rank order — do NOT reorder.\n"
            "2. Your role is writing accurate thesis text, catalysts, name, and schema fields.\n"
            "3. For any tickers in the data but NOT in the pre-ranked list, append them after.\n"
            "4. Market pulse / portfolio bias: use @KobeissiLetter macro context heavily.\n"
            "5. fresh_trades: tickers with recency_days ≤ 7 and has_top_conviction=True are best candidates.\n\n"
        )
    else:
        rank_preamble = (
            "Note: backend scoring had insufficient structured data. "
            "Use your best judgment to rank by conviction strength and recency.\n\n"
        )

    synthesis_prompt = (
        f"Raw X/Twitter data from {len(X_SELECT_ACCOUNTS)} curated trader accounts "
        f"({_cat_counts.get('top_trader',0)} top_traders, "
        f"{_cat_counts.get('above_average_trader',0)} above_avg, "
        f"{_cat_counts.get('retail_trader',0)} retail, "
        f"{_cat_counts.get('thematic_investor',0)} thematic, "
        f"{_cat_counts.get('theme_datapoints',0)} datapoints, "
        f"1 macro/market-context account).\n\n"
        + rank_preamble
        + "RAW TRADER DATA (per-account, with tier labels):\n"
        + combined_text
        + "\n\nReturn ONLY valid JSON per your system schema. "
        "No markdown fences, no backticks, no extra text."
    )

    try:
        result = await data_service.xai._call_grok_with_x_search(
            prompt=synthesis_prompt,
            raw_mode=False,
            use_deep_model=True,
            timeout=90.0,
            system_text=X_SELECT_TRADER_CONSENSUS_CONTRACT,
            max_output_tokens=4000,
        )
    except Exception as e:
        print(f"[X_CONSENSUS] Synthesis exception: {e}")
        return None

    if not isinstance(result, dict) or result.get("error"):
        err = result.get("error", "unknown") if isinstance(result, dict) else str(result)
        print(f"[X_CONSENSUS] Synthesis error: {err}")
        return None

    # ── Normalise and persist ─────────────────────────────────────────────
    normalized = _normalize_consensus(result, backend_scores=backend_score_by_ticker)

    accounts_meta = [
        {"handle": a["handle"], "category": a["category"], "weight": a["weight"]}
        for a in X_SELECT_ACCOUNTS
    ]

    # Enrich Phase-1 per-account mention data with category + weight so Social
    # section builders can filter by tier without re-importing account config.
    # This adds zero API calls — data is already computed during the refresh.
    mention_data: list[dict] = [
        {
            "handle":   acct.get("handle", ""),
            "category": _ACCOUNT_CATEGORY_BY_HANDLE.get(acct.get("handle", ""), ""),
            "weight":   _ACCOUNT_WEIGHT_BY_HANDLE.get(acct.get("handle", ""), 0.0),
            "mentions": acct.get("mentions", []),
        }
        for acct in all_account_mentions
        if acct.get("handle") and acct.get("mentions")
    ]

    snapshot = {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "handles":           X_SELECT_HANDLES,     # flat list (backward compat)
        "accounts":          accounts_meta,         # structured config
        "top_tickers":       normalized["top_tickers"],
        "key_themes":        normalized["key_themes"],
        "notable_accounts":  normalized["notable_accounts"],
        "raw":               normalized.get("raw"),
        # Internal fields — not included in public Home/Social payload
        "_backend_ranked":      backend_ranked[:50],
        "_backend_parse_count": len(all_account_mentions),
        # Per-account Phase-1 mention data (category + weight enriched).
        # Used by Social section builders (freshest_alpha, sentiment_acceleration)
        # to derive section rankings deterministically — zero extra API calls.
        "_mention_data":        mention_data,
    }

    # ── Section-level validation + LKG merge ─────────────────────────────────
    # Read the CURRENT disk cache BEFORE it gets rotated inside _save_disk_cache,
    # so we can restore any sections that came back empty from this scan.
    _pre_write_lkg = _load_disk_cache()
    _section_ok    = _validate_snapshot_sections(snapshot)
    _lkg_merged    = _merge_lkg_sections(snapshot, _pre_write_lkg, _section_ok)
    snapshot["_lkg_sections_used"] = _lkg_merged

    _sections_ok_names   = [k for k, v in _section_ok.items() if v]
    _sections_fail_names = [k for k, v in _section_ok.items() if not v]
    if _lkg_merged:
        print(
            f"[X_CONSENSUS] LKG merge applied — "
            f"failed sections: {_sections_fail_names}, "
            f"restored from prior: {_lkg_merged}"
        )

    # ── Diagnostics log ───────────────────────────────────────────────────────
    _write_status = "written_lkg_partial" if _lkg_merged else "written_clean"
    _append_scan_diagnostics({
        "scan_ts":            datetime.now(timezone.utc).isoformat(),
        "accounts_count":     len(X_SELECT_ACCOUNTS),
        "batch_count":        len(batches),
        "batches_returned":   len(combined_data),
        "sections_ok":        _sections_ok_names,
        "sections_missing":   _sections_fail_names,
        "lkg_sections_used":  _lkg_merged,
        "ticker_count":       len(snapshot.get("_backend_ranked") or []),
        "mention_records":    len(snapshot.get("_mention_data") or []),
        "consensus_picks":    len((snapshot.get("raw") or {}).get("consensus_picks") or []),
        "top_tickers":        len(snapshot.get("top_tickers") or []),
        "cache_write_status": _write_status,
        "error":              None,
    })

    _save_disk_cache(snapshot)
    print(
        f"[X_CONSENSUS] Refresh complete — {len(snapshot['top_tickers'])} tickers, "
        f"{len(snapshot.get('_backend_ranked') or [])} backend-scored, "
        f"{len(snapshot.get('_mention_data') or [])} mention records saved"
        + (f" | LKG-merged: {_lkg_merged}" if _lkg_merged else "")
    )
    return snapshot


async def _trigger_background_refresh(data_service) -> None:
    """Fire-and-forget refresh guarded by the module-level lock.

    If another refresh is already running (lock held), return immediately —
    this is the stampede protection.
    """
    if _REFRESH_LOCK.locked():
        print("[X_CONSENSUS] Refresh already in progress, skipping duplicate trigger")
        return
    async with _REFRESH_LOCK:
        try:
            await _run_refresh(data_service)
        except Exception as e:
            print(f"[X_CONSENSUS] Background refresh failed: {e}")


def _public_payload(
    raw: Optional[dict],
    *,
    refresh_in_progress: bool,
    window_open: bool,
) -> dict:
    """Build the outward-facing Home payload from a raw disk snapshot."""
    next_refresh = _next_window_open_iso() if not window_open else None

    if not raw:
        return {
            "generated_at": None,
            "top_tickers": [],
            "key_themes": [],
            "notable_accounts": [],
            "is_stale": True,
            "stale": True,
            "data_state": "no_data_yet",
            "refresh_in_progress": False,
            "available": False,
            "refresh_window_open": window_open,
            "next_allowed_refresh_at": next_refresh,
            "timezone": "America/Chicago",
        }
    age_s = 0.0
    try:
        age_s = time.time() - float(raw.get("_saved_at") or 0)
    except Exception:
        age_s = 0.0
    is_stale = age_s >= _CACHE_TTL_SECONDS
    return {
        "generated_at": raw.get("generated_at"),
        "top_tickers": raw.get("top_tickers") or [],
        "key_themes": raw.get("key_themes") or [],
        "notable_accounts": raw.get("notable_accounts") or [],
        "is_stale": is_stale,
        "stale": is_stale,
        "data_state": "stale" if is_stale else "available",
        "age_seconds": int(age_s) if age_s else None,
        "refresh_in_progress": refresh_in_progress,
        "available": True,
        "refresh_window_open": window_open,
        "next_allowed_refresh_at": next_refresh,
        "timezone": "America/Chicago",
    }


async def get_weekly_snapshot(data_service=None, *, allow_refresh: bool = True) -> dict:
    """Return the current daily snapshot for the Home page.

    Rules:
      - Cache is considered fresh for 23 hours after each noon refresh, so this
        function will almost never trigger a background refresh during the day.
      - If the cache is somehow older than 23 hours (e.g. the server was down
        at noon), a background refresh is kicked off immediately as a catch-up.
      - Never blocks the caller — Grok always runs in the background.
    """
    raw = _load_disk_cache()
    fresh = _is_fresh(raw)

    refresh_in_progress = False
    if allow_refresh and not fresh and data_service is not None:
        # Cache is >23 h stale — noon refresh was missed; catch up now.
        refresh_in_progress = not _REFRESH_LOCK.locked()
        try:
            asyncio.create_task(_trigger_background_refresh(data_service))
        except RuntimeError:
            refresh_in_progress = False

    return _public_payload(raw, refresh_in_progress=refresh_in_progress, window_open=True)


async def trigger_manual_refresh(data_service) -> dict:
    """Explicit user-initiated X consensus refresh.

    Unlike the automatic background loop this function:
      - Bypasses the 08:00–20:00 America/Chicago quiet-hours gate entirely.
      - Still enforces the module-level _REFRESH_LOCK (single-flight: if a
        refresh is already running, we return immediately rather than stacking
        a second one).
      - Enforces a 30-minute per-process cooldown (_MANUAL_COOLDOWN_SECONDS)
        so a user cannot hammer Grok overnight.

    Returns a metadata dict suitable for a JSON response:
      accepted                    bool
      refresh_in_progress         bool
      last_updated_at             Optional[str]  (ISO-8601 UTC)
      next_manual_refresh_allowed_at  Optional[str]
      manual_refresh_available    bool
      reason                      Optional[str]  — present when not accepted
    """
    global _last_manual_refresh_at

    raw = _load_disk_cache()
    last_updated_at = raw.get("generated_at") if raw else None

    # ── Guard 1: single-flight (another refresh already running) ───────────
    if _REFRESH_LOCK.locked():
        return {
            "accepted": False,
            "refresh_in_progress": True,
            "last_updated_at": last_updated_at,
            "next_manual_refresh_allowed_at": _next_manual_allowed_iso(),
            "manual_refresh_available": False,
            "reason": "refresh_already_running",
        }

    # ── Guard 2: cooldown window ───────────────────────────────────────────
    if not _manual_refresh_available():
        return {
            "accepted": False,
            "refresh_in_progress": False,
            "last_updated_at": last_updated_at,
            "next_manual_refresh_allowed_at": _next_manual_allowed_iso(),
            "manual_refresh_available": False,
            "reason": "cooldown",
        }

    # ── Guard 3: no xAI provider available ────────────────────────────────
    if not data_service or not getattr(data_service, "xai", None):
        return {
            "accepted": False,
            "refresh_in_progress": False,
            "last_updated_at": last_updated_at,
            "next_manual_refresh_allowed_at": _next_manual_allowed_iso(),
            "manual_refresh_available": False,
            "reason": "xai_provider_unavailable",
        }

    # ── All guards passed — stamp cooldown and fire background refresh ─────
    _last_manual_refresh_at = time.time()
    print(
        f"[X_CONSENSUS] Manual refresh accepted — bypassing quiet-hours gate. "
        f"Next manual allowed: {_next_manual_allowed_iso()}"
    )
    try:
        asyncio.create_task(_trigger_background_refresh(data_service))
        refresh_kicked_off = True
    except RuntimeError:
        # Called outside an async context — fall back to awaiting directly.
        refresh_kicked_off = False
        await _trigger_background_refresh(data_service)

    return {
        "accepted": True,
        "refresh_in_progress": refresh_kicked_off or True,
        "last_updated_at": last_updated_at,
        "next_manual_refresh_allowed_at": _next_manual_allowed_iso(),
        "manual_refresh_available": False,
        "reason": None,
    }
