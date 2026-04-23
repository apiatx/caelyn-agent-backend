"""
Bi-hourly cached X "Select Trader Consensus" snapshot.

Exposes the same account universe and extraction logic used by the Social page
`POST /api/social/query` endpoint (preset_intent="x_select_trader_consensus"),
but persists a single snapshot to disk and refreshes it at most once per 2 hours.

Used by the Home page so it never runs a live X scan on page load. If no
snapshot exists yet, the function returns a stale/empty payload and kicks off
exactly one background refresh (lock-guarded to prevent stampede).
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Canonical 25-account universe. The Social page `/api/social/query`
# (preset: x_select_trader_consensus) imports this same list to guarantee
# Home and Social use the EXACT same handles.
X_SELECT_HANDLES: list[str] = [
    "aleabitoreddit", "KobeissiLetter", "HyperTechInvest", "crux_capital_",
    "SJCapitalInvest", "BlackPantherCap", "Kaizen_Investor", "Venu_7_",
    "DrJebaim", "CKCapitalxx", "TheTape_TNM", "equitydd",
    "Speculator_io", "StonkValue", "stamatoudism", "yianisz",
    "sunxliao", "futurist_lens", "Thomas_james_1", "DeepValueBagger",
    "ConnorJBates_", "BussinBiotech", "BambroughKevin", "AlexfromBabylon",
    "UncleAlpha007",
]

# Disk cache path — mirrors sector_rotation/gemini_analysis.py layout.
_CACHE_PATH = Path(__file__).parent.parent / "data" / "x_consensus_weekly.json"
_CACHE_TTL_SECONDS = 2 * 3600  # 2 hours (120 minutes)
_BATCH_SIZE = 8

# Module-level lock so only one background refresh runs at a time across the
# whole process, regardless of how many Home requests land simultaneously.
_REFRESH_LOCK = asyncio.Lock()


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


def _save_disk_cache(data: dict) -> None:
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
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


async def _fetch_batch(data_service, handles: list[str], batch_num: int, total_batches: int) -> str:
    """Phase-1 helper — fetch raw post data for one batch of handles.

    Mirrors the logic inside `/api/social/query` line ~2293 so Home and Social
    behave identically.
    """
    batch_prompt = (
        "Search the last 20 posts from EACH of these accounts: "
        + ", ".join(f"@{h}" for h in handles)
        + ". For each account, list the tickers/assets they mention with "
        "bullish/bearish context, their thesis, conviction level, and any "
        "catalysts they cite. Include the account handle with each finding. "
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

    batches = [X_SELECT_HANDLES[i:i + _BATCH_SIZE]
               for i in range(0, len(X_SELECT_HANDLES), _BATCH_SIZE)]
    print(f"[X_CONSENSUS] Refresh starting — {len(X_SELECT_HANDLES)} handles "
          f"in {len(batches)} batches")

    # Phase 1: parallel batched fetch
    batch_results = await asyncio.gather(
        *[_fetch_batch(data_service, batch, i, len(batches)) for i, batch in enumerate(batches)],
        return_exceptions=True,
    )
    combined_data: list[str] = []
    for i, res in enumerate(batch_results):
        if isinstance(res, Exception):
            print(f"[X_CONSENSUS] Batch {i + 1} failed: {res}")
            continue
        if res and isinstance(res, str) and not res.startswith("xAI"):
            combined_data.append(
                f"=== Batch {i + 1} ({', '.join('@' + h for h in batches[i])}) ===\n{res}"
            )

    if not combined_data:
        print("[X_CONSENSUS] All batches failed — aborting refresh (keep existing cache)")
        return None

    # Phase 2: synthesis with deep reasoning model
    combined_text = "\n\n".join(combined_data)
    print(f"[X_CONSENSUS] Synthesis phase: {len(combined_text):,} chars")
    synthesis_prompt = (
        "Below is raw data from X/Twitter posts by 25 select trader accounts. "
        "Analyze ALL of this data and produce the consensus JSON output per your schema.\n\n"
        "RAW X DATA:\n" + combined_text + "\n\n"
        "Now synthesize this into the exact JSON schema from your system instructions. "
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
    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "handles": X_SELECT_HANDLES,
        "top_tickers": normalized["top_tickers"],
        "key_themes": normalized["key_themes"],
        "notable_accounts": normalized["notable_accounts"],
        "raw": normalized.get("raw"),
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


def _public_payload(raw: Optional[dict], *, refresh_in_progress: bool) -> dict:
    """Build the outward-facing Home payload from a raw disk snapshot."""
    if not raw:
        return {
            "generated_at": None,
            "top_tickers": [],
            "key_themes": [],
            "notable_accounts": [],
            "is_stale": True,
            "refresh_in_progress": refresh_in_progress,
            "available": False,
        }
    age_s = 0.0
    try:
        age_s = time.time() - float(raw.get("_saved_at") or 0)
    except Exception:
        age_s = 0.0
    return {
        "generated_at": raw.get("generated_at"),
        "top_tickers": raw.get("top_tickers") or [],
        "key_themes": raw.get("key_themes") or [],
        "notable_accounts": raw.get("notable_accounts") or [],
        "is_stale": age_s >= _CACHE_TTL_SECONDS,
        "age_seconds": int(age_s) if age_s else None,
        "refresh_in_progress": refresh_in_progress,
        "available": True,
    }


async def get_weekly_snapshot(data_service=None, *, allow_refresh: bool = True) -> dict:
    """Return the current weekly snapshot for the Home page.

    Rules:
      - If a fresh (<7d) snapshot exists on disk, return it with is_stale=False.
      - If a stale (>=7d) snapshot exists, return it immediately (is_stale=True)
        and kick off a single background refresh.
      - If no snapshot exists, return an empty payload and kick off a refresh.
      - Never blocks the Home page request waiting for Grok.
    """
    raw = _load_disk_cache()
    fresh = _is_fresh(raw)

    refresh_in_progress = False
    if allow_refresh and not fresh and data_service is not None:
        # Don't await the refresh — run it in the background so Home renders now.
        refresh_in_progress = not _REFRESH_LOCK.locked()
        try:
            asyncio.create_task(_trigger_background_refresh(data_service))
        except RuntimeError:
            # No running loop — unusual for the Home code path, but stay safe.
            refresh_in_progress = False

    return _public_payload(raw, refresh_in_progress=refresh_in_progress)
