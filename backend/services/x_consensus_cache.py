"""
Bi-hourly cached X "Select Trader Consensus" snapshot.

Exposes the same account universe and extraction logic used by the Social page
`POST /api/social/query` endpoint (preset_intent="x_select_trader_consensus"),
but persists a single snapshot to disk and refreshes it at most once per 2 hours.

Used by the Home page so it never runs a live X scan on page load. If no
snapshot exists yet, the function returns a stale/empty payload and kicks off
exactly one background refresh (lock-guarded to prevent stampede).

Refresh window: 08:00–20:00 America/Chicago only.  Outside that window the
function never triggers a Grok/XAI call — it serves the last cached snapshot
(or a no_data_yet state) regardless of staleness.

Account tiers and weights:
  top_trader          1.0  — Highest conviction; strongest influence on consensus_picks
  above_average_trader 0.8  — Strong trade signal quality
  breaking_news       0.75 — High urgency/recency; amplifies thesis, not primary signal
  thematic_investor   0.5  — Thematic context + broad market reads; NOT direct conviction
  theme_datapoints    0.33 — Discovery + stock list datapoints; informs hype_radar only
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # Python <3.9 fallback

# ── Canonical account universe with category + weight metadata ─────────────
# Single source of truth for both the refresh flow and the Social page prompt.
# The Social page `/api/social/query` imports X_SELECT_HANDLES (derived below)
# to guarantee Home and Social use the EXACT same universe.
X_SELECT_ACCOUNTS: list[dict] = [
    # ── Breaking News / Research (weight 0.75) ──────────────────────────────
    {"handle": "MikeTrap_TNM",      "category": "breaking_news",        "weight": 0.75,
     "notes": "Breaking ticker news + market research"},
    # ── Top Traders — highest conviction (weight 1.0) ───────────────────────
    {"handle": "Prophets0Stocks",   "category": "top_trader",           "weight": 1.0},
    {"handle": "MattCKleinlein",    "category": "top_trader",           "weight": 1.0},
    {"handle": "SysVslt",           "category": "top_trader",           "weight": 1.0},
    {"handle": "MarketBlogger",     "category": "top_trader",           "weight": 1.0},
    {"handle": "UncleAlpha007",     "category": "top_trader",           "weight": 1.0},
    {"handle": "MikeCon07163",      "category": "top_trader",           "weight": 1.0},
    # ── Above Average Traders — second tier (weight 0.8) ───────────────────
    {"handle": "HyperTechInvest",   "category": "above_average_trader", "weight": 0.8},
    {"handle": "ThematicTrader",    "category": "above_average_trader", "weight": 0.8},
    {"handle": "VortexTraders",     "category": "above_average_trader", "weight": 0.8},
    {"handle": "Ben_aram6",         "category": "above_average_trader", "weight": 0.8},
    # ── Thematic / Retail Investors (weight 0.5) ────────────────────────────
    # Useful for thematic context + broad market reads; NOT for direct conviction
    {"handle": "Thomas_james_1",    "category": "thematic_investor",    "weight": 0.5},
    {"handle": "Pokemdollars_",     "category": "thematic_investor",    "weight": 0.5},
    {"handle": "BlackPantherCap",   "category": "thematic_investor",    "weight": 0.5},
    {"handle": "BussinBiotech",     "category": "thematic_investor",    "weight": 0.5},
    {"handle": "TuffCap",           "category": "thematic_investor",    "weight": 0.5},
    {"handle": "Venu_7_",           "category": "thematic_investor",    "weight": 0.5},
    {"handle": "futurist_lens",     "category": "thematic_investor",    "weight": 0.5},
    {"handle": "DougVaccaroBagger", "category": "thematic_investor",    "weight": 0.5},
    {"handle": "nundab",            "category": "thematic_investor",    "weight": 0.5},
    {"handle": "AlexfromBabylon",   "category": "thematic_investor",    "weight": 0.5},
    {"handle": "StableKopek",       "category": "thematic_investor",    "weight": 0.5},
    # ── Investment Themes + Datapoints (weight 0.33) ────────────────────────
    # Useful for discovery and hype_radar themes; NOT for consensus_picks scores
    {"handle": "mrephd",            "category": "theme_datapoints",     "weight": 0.33},
    {"handle": "Speculator_io",     "category": "theme_datapoints",     "weight": 0.33},
    {"handle": "StockVision",       "category": "theme_datapoints",     "weight": 0.33},
]

# Flat handle list derived from the structured config — preserves backward
# compatibility with all code that imports X_SELECT_HANDLES.
X_SELECT_HANDLES: list[str] = [a["handle"] for a in X_SELECT_ACCOUNTS]

# Category weight lookup for prompt injection
_ACCOUNT_WEIGHT_BY_HANDLE: dict[str, float] = {
    a["handle"]: a["weight"] for a in X_SELECT_ACCOUNTS
}
_ACCOUNT_CATEGORY_BY_HANDLE: dict[str, str] = {
    a["handle"]: a["category"] for a in X_SELECT_ACCOUNTS
}

# Human-readable weighting context injected into the synthesis prompt.
# Kept here (not in prompts.py) so it travels with the account list.
_SYNTHESIS_WEIGHT_CONTEXT: str = """
ACCOUNT TIER WEIGHTING — apply these rules when scoring consensus_picks:

Tiers (highest → lowest influence on consensus_picks hype_score / conviction):
  top_trader (1.0):           @Prophets0Stocks @MattCKleinlein @SysVslt @MarketBlogger @UncleAlpha007 @MikeCon07163
  above_average_trader (0.8): @HyperTechInvest @ThematicTrader @VortexTraders @Ben_aram6
  breaking_news (0.75):       @MikeTrap_TNM — adds urgency/recency; amplifies existing thesis, NOT a primary conviction signal on its own
  thematic_investor (0.5):    @Thomas_james_1 @Pokemdollars_ @BlackPantherCap @BussinBiotech @TuffCap @Venu_7_ @futurist_lens @DougVaccaroBagger @nundab @AlexfromBabylon @StableKopek — thematic/broad context only
  theme_datapoints (0.33):    @mrephd @Speculator_io @StockVision — discovery + stock list datapoints; informs hype_radar, NOT consensus_picks scores

SCORING RULES:
- consensus_picks hype_score: weight each mention by the account's tier weight above.
- A single top_trader pick outweighs multiple thematic_investor mentions for hype_score.
- consensus_strength of 'High' or 'Very High' requires at least one top_trader OR above_average_trader mention.
- theme_datapoints accounts should inform hype_radar themes and key_themes ONLY — do not inflate consensus_picks hype_score from them.
- breaking_news accounts contribute to market_pulse freshness and recency — do not independently drive consensus_picks conviction unless corroborated by top_trader or above_average_trader.
- thematic_investor accounts contribute to hype_radar buzz_level, portfolio_bias context, and medium-term theme validation — not fast-entry conviction scoring.
- trader_count should reflect only top_trader + above_average_trader account mentions for accurate conviction signal.
"""

# Disk cache paths — current snapshot + immediately prior snapshot for delta math.
_CACHE_PATH       = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_PRIOR_CACHE_PATH = Path(__file__).parent.parent / "data" / "x_consensus_weekly_prior.json"
_CACHE_TTL_SECONDS = 2 * 3600  # 2 hours (120 minutes)
_BATCH_SIZE = 8

# Module-level lock so only one background refresh runs at a time across the
# whole process, regardless of how many Home requests land simultaneously.
_REFRESH_LOCK = asyncio.Lock()

# ── Refresh window: 08:00–20:00 America/Chicago, DST-safe ─────────────────
_REFRESH_TZ = ZoneInfo("America/Chicago")
_WINDOW_START_HOUR = 8   # 08:00 Chicago
_WINDOW_END_HOUR   = 20  # 20:00 Chicago (exclusive)

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


def _save_disk_cache(data: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        # Rotate: current → prior before writing new current.
        if _CACHE_PATH.exists():
            try:
                _PRIOR_CACHE_PATH.write_text(_CACHE_PATH.read_text())
            except Exception as e:
                print(f"[X_CONSENSUS] Prior cache rotate error: {e}")
        data["_saved_at"] = time.time()
        _CACHE_PATH.write_text(json.dumps(data, indent=2))
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
    data_service,
    batch_accounts: list[dict],
    batch_num: int,
    total_batches: int,
) -> str:
    """Phase-1 helper — fetch raw post data for one batch of accounts.

    batch_accounts: list of account dicts from X_SELECT_ACCOUNTS
    Each entry has: handle, category, weight.  The category label is included
    in the prompt so Grok knows the tier of each account it's reading.
    """
    handles = [a["handle"] for a in batch_accounts]
    # Include category label so Grok can weight accounts correctly in Phase 1
    handle_labels = ", ".join(
        f"@{a['handle']} [{a['category']}]" for a in batch_accounts
    )
    batch_prompt = (
        "Search the last 20 posts from EACH of these accounts: "
        + handle_labels
        + ". For each account, list the tickers/assets they mention with "
        "bullish/bearish context, their thesis, conviction level, and any "
        "catalysts they cite. Include the account handle with each finding. "
        "Note the account tier in brackets — top_trader and above_average_trader "
        "posts are highest conviction signals; thematic_investor posts provide "
        "broad thematic context; theme_datapoints posts identify themes/stocks "
        "for discovery; breaking_news posts add urgency/recency context. "
        "Be thorough and specific — quote or closely paraphrase their actual posts."
    )
    try:
        result = await data_service.xai._call_grok_with_x_search(
            prompt=batch_prompt,
            raw_mode=True,
            use_deep_model=False,
            timeout=60.0,
            x_search_config={"allowed_x_handles": handles},
        )
    except Exception as e:
        print(f"[X_CONSENSUS] Batch {batch_num + 1}/{total_batches} exception: {e}")
        return ""

    text = ""
    if isinstance(result, dict):
        text = result.get("_raw_analysis", "") or result.get("error", "")
    print(f"[X_CONSENSUS] Batch {batch_num + 1}/{total_batches}: {len(handles)} handles -> {len(text)} chars")
    return text


def _normalize_consensus(raw_result: Any) -> dict:
    """Convert the Grok synthesis response into the Home-shaped snapshot.

    The synthesis schema (X_SELECT_TRADER_CONSENSUS_CONTRACT) returns fields:
      consensus_picks[], fresh_trades[], hype_radar[], spotlight{}, market_pulse,
      portfolio_bias, accounts_analyzed[]
    We flatten to a Home-friendly shape:
      top_tickers[{symbol, mentions, sentiment, rationale}]
      key_themes[str]
      notable_accounts[str]
    """
    if not isinstance(raw_result, dict):
        return {"top_tickers": [], "key_themes": [], "notable_accounts": []}

    picks = raw_result.get("consensus_picks") or []
    top_tickers: list[dict] = []
    for p in picks[:20]:
        if not isinstance(p, dict):
            continue
        symbol = p.get("ticker") or p.get("symbol") or p.get("asset")
        if not symbol:
            continue
        top_tickers.append({
            "symbol": str(symbol).upper().lstrip("$"),
            "mentions": p.get("mention_count") or p.get("mentions") or p.get("count"),
            "sentiment": p.get("sentiment") or p.get("bias") or p.get("direction"),
            "rationale": p.get("thesis") or p.get("rationale") or p.get("summary") or "",
            "accounts": p.get("accounts") or p.get("traders") or [],
        })

    key_themes_raw = raw_result.get("market_pulse") or raw_result.get("key_themes") or []
    if isinstance(key_themes_raw, str):
        key_themes = [key_themes_raw]
    elif isinstance(key_themes_raw, list):
        key_themes = [str(t) for t in key_themes_raw if t][:6]
    else:
        key_themes = []

    accounts = raw_result.get("accounts_analyzed") or []
    if isinstance(accounts, list):
        notable_accounts = [str(a) for a in accounts if a][:25]
    else:
        notable_accounts = []

    return {
        "top_tickers": top_tickers,
        "key_themes": key_themes,
        "notable_accounts": notable_accounts,
        # Preserve the full raw result for clients that want deeper detail.
        "raw": raw_result,
    }


async def _run_refresh(data_service) -> Optional[dict]:
    """Actually execute the 2-phase X consensus scan and persist the result."""
    try:
        from agent.prompts import X_SELECT_TRADER_CONSENSUS_CONTRACT
    except Exception as e:
        print(f"[X_CONSENSUS] Could not import contract: {e}")
        return None

    if not data_service or not getattr(data_service, "xai", None):
        print("[X_CONSENSUS] No xAI provider — skipping refresh")
        return None

    # Build batches from the structured account config (not the flat handle list)
    batches: list[list[dict]] = [
        X_SELECT_ACCOUNTS[i:i + _BATCH_SIZE]
        for i in range(0, len(X_SELECT_ACCOUNTS), _BATCH_SIZE)
    ]
    print(
        f"[X_CONSENSUS] Refresh starting — {len(X_SELECT_ACCOUNTS)} accounts "
        f"in {len(batches)} batches "
        f"({sum(1 for a in X_SELECT_ACCOUNTS if a['category']=='top_trader')} top_trader, "
        f"{sum(1 for a in X_SELECT_ACCOUNTS if a['category']=='above_average_trader')} above_avg, "
        f"{sum(1 for a in X_SELECT_ACCOUNTS if a['category']=='thematic_investor')} thematic, "
        f"{sum(1 for a in X_SELECT_ACCOUNTS if a['category']=='theme_datapoints')} datapoints, "
        f"{sum(1 for a in X_SELECT_ACCOUNTS if a['category']=='breaking_news')} news)"
    )

    # Phase 1: parallel batched fetch (category labels included in each prompt)
    batch_results = await asyncio.gather(
        *[_fetch_batch(data_service, batch, i, len(batches))
          for i, batch in enumerate(batches)],
        return_exceptions=True,
    )
    combined_data: list[str] = []
    for i, res in enumerate(batch_results):
        if isinstance(res, Exception):
            print(f"[X_CONSENSUS] Batch {i + 1} failed: {res}")
            continue
        if res and isinstance(res, str) and not res.startswith("xAI"):
            batch_labels = ", ".join(
                f"@{a['handle']} [{a['category']}]" for a in batches[i]
            )
            combined_data.append(
                f"=== Batch {i + 1} ({batch_labels}) ===\n{res}"
            )

    if not combined_data:
        print("[X_CONSENSUS] All batches failed — aborting refresh (keep existing cache)")
        return None

    # Phase 2: synthesis with deep reasoning model.
    # The system_text (X_SELECT_TRADER_CONSENSUS_CONTRACT) is the canonical output
    # schema contract — NOT modified here.  Category weighting instructions are
    # injected into the USER-side synthesis prompt so the model scores correctly
    # without touching prompts.py.
    combined_text = "\n\n".join(combined_data)
    print(f"[X_CONSENSUS] Synthesis phase: {len(combined_text):,} chars")
    synthesis_prompt = (
        f"Below is raw data from X/Twitter posts by {len(X_SELECT_ACCOUNTS)} accounts "
        f"spanning {len(set(a['category'] for a in X_SELECT_ACCOUNTS))} tiers "
        "(top_trader, above_average_trader, breaking_news, thematic_investor, theme_datapoints).\n\n"
        + _SYNTHESIS_WEIGHT_CONTEXT.strip()
        + "\n\nRAW X DATA (each batch annotated with account tier):\n"
        + combined_text
        + "\n\nNow synthesize ALL of this data into the exact JSON schema from your system "
        "instructions, applying the tier weighting rules above. "
        "Return ONLY valid JSON — no markdown, no backticks, no extra text."
    )
    try:
        result = await data_service.xai._call_grok_with_x_search(
            prompt=synthesis_prompt,
            raw_mode=False,
            use_deep_model=True,
            timeout=90.0,
            system_text=X_SELECT_TRADER_CONSENSUS_CONTRACT,
        )
    except Exception as e:
        print(f"[X_CONSENSUS] Synthesis exception: {e}")
        return None

    if not isinstance(result, dict) or result.get("error"):
        err = result.get("error", "unknown") if isinstance(result, dict) else str(result)
        print(f"[X_CONSENSUS] Synthesis error: {err}")
        return None

    normalized = _normalize_consensus(result)

    # Account config snapshot: categories + handle list for downstream consumers
    accounts_meta = [
        {"handle": a["handle"], "category": a["category"], "weight": a["weight"]}
        for a in X_SELECT_ACCOUNTS
    ]
    snapshot = {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "handles":        X_SELECT_HANDLES,           # flat list (backward compat)
        "accounts":       accounts_meta,              # structured config (new)
        "top_tickers":    normalized["top_tickers"],
        "key_themes":     normalized["key_themes"],
        "notable_accounts": normalized["notable_accounts"],
        "raw":            normalized.get("raw"),
    }
    _save_disk_cache(snapshot)
    print(f"[X_CONSENSUS] Refresh complete — {len(snapshot['top_tickers'])} tickers saved")
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
    """Return the current weekly snapshot for the Home page.

    Rules:
      - Refreshes only between 08:00–20:00 America/Chicago (DST-safe).
      - Outside that window: serve stale cache or no_data_yet — zero Grok calls.
      - During the window: if cache is stale (>2 h), trigger one background refresh.
      - Never blocks the caller — Grok always runs in the background.
    """
    raw = _load_disk_cache()
    fresh = _is_fresh(raw)

    # ── Time-window gate — the single place where Grok calls are allowed ──
    window_open = _in_refresh_window()
    if not window_open:
        # Overnight / early morning: never touch Grok/XAI, just serve cache.
        next_open = _next_window_open_iso()
        print(
            f"[X_CONSENSUS] Refresh window closed (Chicago time). "
            f"Serving cached snapshot. Next open: {next_open}"
        )
        return _public_payload(raw, refresh_in_progress=False, window_open=False)

    # ── Within the 08:00–20:00 Chicago window ─────────────────────────────
    refresh_in_progress = False
    if allow_refresh and not fresh and data_service is not None:
        # Don't await the refresh — run it in the background so Home renders now.
        refresh_in_progress = not _REFRESH_LOCK.locked()
        try:
            asyncio.create_task(_trigger_background_refresh(data_service))
        except RuntimeError:
            # No running event loop — unusual but stay safe.
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
