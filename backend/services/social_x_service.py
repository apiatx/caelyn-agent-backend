"""
Social page X-dashboard service.

Derives 4 Social sections from the existing x_consensus_cache snapshot —
NO additional Grok / XAI calls are made here.

Sections:
  A. x_consensus         — from raw.consensus_picks
  B. freshest_alpha      — from raw.fresh_trades + raw.spotlight.freshest_alpha
  C. theme_leadership    — from raw.hype_radar + raw.market_pulse
  D. sentiment_accel     — deterministic delta between current and prior snapshots

All reads go through the same cached snapshot used by Home `Trending on X`.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Optional


# ── Snapshot loaders ────────────────────────────────────────────────────────

def _load_snapshots() -> tuple[Optional[dict], Optional[dict]]:
    """Return (current_raw, prior_raw).  Either may be None if not on disk."""
    from services.x_consensus_cache import (
        _load_disk_cache,
        _load_prior_cache,
        _CACHE_TTL_SECONDS,
    )
    current = _load_disk_cache()
    prior   = _load_prior_cache()
    return current, prior


def _raw(snapshot: Optional[dict]) -> dict:
    """Extract the raw Grok dict from a snapshot; return {} if absent."""
    if not snapshot or not isinstance(snapshot, dict):
        return {}
    r = snapshot.get("raw")
    return r if isinstance(r, dict) else {}


# ── Section A — X Consensus ─────────────────────────────────────────────────

def _build_x_consensus(raw: dict) -> list[dict]:
    """
    Normalise consensus_picks into the Social-page row format.
    All fields come directly from the Grok contract output.
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


# ── Section B — Freshest Alpha ───────────────────────────────────────────────

def _build_freshest_alpha(raw: dict) -> dict:
    """
    Return fresh_trades rows plus a spotlight badge when applicable.
    Source: raw.fresh_trades (direct) + raw.spotlight.freshest_alpha (badge).
    """
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
            "ticker":            ticker,
            "name":              t.get("name") or "",
            "tradingview_symbol": t.get("tradingview_symbol") or "",
            "first_mentioned_by": t.get("first_mentioned_by") or [],
            "why_fresh":         t.get("why_fresh") or "",
            "entry_thesis":      t.get("entry_thesis") or "",
            "spotlight_badge":   is_spotlight,
            "spotlight_signal":  spotlight_fa.get("signal") if is_spotlight else None,
        })

    return {
        "trades": trades,
        "spotlight": {
            "ticker":  spotlight_ticker or None,
            "signal":  spotlight_fa.get("signal") or None,
        } if spotlight_ticker else None,
    }


# ── Section C — Theme Leadership ─────────────────────────────────────────────

def _build_theme_leadership(raw: dict) -> dict:
    """
    Return hype_radar rows enriched with market_pulse context.
    Source: raw.hype_radar + raw.market_pulse.
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

    # Sort highest buzz first
    themes.sort(key=lambda t: t.pop("buzz_rank"))

    return {
        "themes": themes,
        "market_pulse": {
            "verdict": market_pulse.get("verdict") or "",
            "summary": market_pulse.get("summary") or "",
            "regime":  market_pulse.get("regime") or "",
        },
    }


# ── Section D — Sentiment Acceleration ──────────────────────────────────────

def _build_sentiment_accel(current_raw: dict, prior_raw: dict) -> list[dict]:
    """
    Deterministic delta: compare current consensus_picks vs prior consensus_picks.

    For each ticker in current snapshot:
      - If it existed in prior: compute hype_score delta, trader_count delta.
      - If it's new (not in prior): treat prior values as 0.

    Tickers are ranked by hype_delta DESC, then by is_new DESC.
    No Grok calls are made.
    """
    cur_picks  = current_raw.get("consensus_picks") or []
    prev_picks = prior_raw.get("consensus_picks") if prior_raw else []
    prev_picks = prev_picks or []

    # Build prior lookup by ticker
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

        hype_delta    = cur_score - prev_score
        trader_delta  = cur_count - prev_count

        # Skip tickers that haven't improved (unless they're brand new)
        if not is_new and hype_delta <= 0 and trader_delta <= 0:
            continue

        buzz_trend = p.get("buzz_trend") or ""
        rationale_parts = []
        if is_new:
            rationale_parts.append("New entry — not in prior snapshot")
        if hype_delta > 0:
            rationale_parts.append(f"Hype score up {hype_delta:+.0f}")
        if trader_delta > 0:
            rationale_parts.append(f"Trader count up {trader_delta:+d}")
        if buzz_trend in ("Accelerating", "New Mention"):
            rationale_parts.append(f"Buzz trend: {buzz_trend}")

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
            "why_now":                  "; ".join(rationale_parts) if rationale_parts else "Continued momentum",
        })

    # Rank: new entries first by hype_score, then by hype_delta
    out.sort(key=lambda x: (not x["is_new_entry"], -x["hype_delta"], -x["current_hype_score"]))
    return out


# ── Metadata helpers ─────────────────────────────────────────────────────────

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

    # manual_refresh_available is independent of the auto schedule window.
    # A user can always trigger a manual refresh after hours UNLESS:
    #   a) a refresh is already running (single-flight)
    #   b) the 30-minute manual cooldown hasn't elapsed
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
            # ── auto-schedule fields ──────────────────────────────────────
            "auto_refresh_window_open":      window_open,
            "refresh_window_open":           window_open,
            "next_allowed_refresh_at":       _next_window_open_iso() if not window_open else None,
            # ── manual-refresh fields ─────────────────────────────────────
            "manual_refresh_available":      manual_available,
            "next_manual_refresh_allowed_at": next_manual_iso,
            "manual_refresh_reason":         manual_reason,
            "source":                        "x_consensus_cache",
            "timezone":                      "America/Chicago",
        }

    saved_at = snapshot.get("_saved_at") or 0
    age_s    = time.time() - float(saved_at)
    is_stale = age_s >= _CACHE_TTL_SECONDS

    return {
        "updated_at":                    snapshot.get("generated_at"),
        "data_state":                    "stale" if is_stale else "available",
        "stale":                         is_stale,
        "age_seconds":                   int(age_s),
        "refresh_in_progress":           lock_held,
        # ── auto-schedule fields ──────────────────────────────────────────
        "auto_refresh_window_open":      window_open,
        "refresh_window_open":           window_open,
        "next_allowed_refresh_at":       _next_window_open_iso() if not window_open else None,
        # ── manual-refresh fields ─────────────────────────────────────────
        "manual_refresh_available":      manual_available,
        "next_manual_refresh_allowed_at": next_manual_iso,
        "manual_refresh_reason":         manual_reason,
        "source":                        "x_consensus_cache",
        "timezone":                      "America/Chicago",
        "handles_count":                 len(snapshot.get("handles") or []),
    }


# ── Public entry point ───────────────────────────────────────────────────────

def build_x_dashboard() -> dict:
    """
    Build the Social X-dashboard payload from cached snapshots only.
    Zero Grok/XAI calls.

    Shape contract — the response is a MERGE of:

    A. The existing Home-style consensus payload (flat, unchanged):
         generated_at, top_tickers, key_themes, notable_accounts,
         is_stale, stale, data_state, age_seconds, refresh_in_progress,
         available, refresh_window_open, next_allowed_refresh_at, timezone
       These keys are produced by _public_payload() — identical to the shape
       the Home page and the existing Social consensus section already consume.
       They must remain byte-for-byte identical so the existing frontend
       consensus rendering path does not break.

    B. Three Social-only sibling sections (new, additive):
         freshest_alpha       — from raw.fresh_trades + spotlight.freshest_alpha
         theme_leadership     — from raw.hype_radar + raw.market_pulse
         sentiment_acceleration — deterministic delta vs prior snapshot

    C. Extra convenience keys from the raw snapshot:
         market_pulse, portfolio_bias, spotlight

    D. Social-specific metadata (richer than the Home subset):
         updated_at, data_state, stale, refresh_in_progress,
         auto_refresh_window_open, refresh_window_open,
         next_allowed_refresh_at, manual_refresh_available,
         next_manual_refresh_allowed_at, manual_refresh_reason,
         timezone, handles_count
    """
    from services.x_consensus_cache import (
        _load_disk_cache as _ldc,
        _public_payload,
        _in_refresh_window,
        _REFRESH_LOCK,
    )

    current_snap, prior_snap = _load_snapshots()
    cur_raw  = _raw(current_snap)
    prev_raw = _raw(prior_snap)

    # ── A. Existing Home-style flat payload (MUST NOT change) ─────────────
    window_open         = _in_refresh_window()
    refresh_in_progress = _REFRESH_LOCK.locked()
    home_payload = _public_payload(
        current_snap,
        refresh_in_progress=refresh_in_progress,
        window_open=window_open,
    )
    # home_payload now contains:
    #   generated_at, top_tickers, key_themes, notable_accounts, is_stale,
    #   stale, data_state, age_seconds, refresh_in_progress, available,
    #   refresh_window_open, next_allowed_refresh_at, timezone

    if not cur_raw:
        # No snapshot on disk yet — return Home-style empty payload plus empty
        # Social-only sections so the frontend can render gracefully.
        return {
            **home_payload,
            # ── Social-only additions ─────────────────────────────────
            "market_pulse":           None,
            "portfolio_bias":         None,
            "spotlight":              None,
            "freshest_alpha":         {"trades": [], "spotlight": None},
            "theme_leadership":       {"themes": [], "market_pulse": None},
            "sentiment_acceleration": [],
            "metadata":               _build_metadata(None),
        }

    # ── B+C+D. Merge Social-only sections alongside unchanged Home payload ─
    return {
        # ── A. Home-style flat keys (byte-for-byte identical to Home shape) ─
        **home_payload,
        # ── C. Extra convenience keys ──────────────────────────────────────
        "market_pulse":   cur_raw.get("market_pulse"),
        "portfolio_bias": cur_raw.get("portfolio_bias"),
        "spotlight":      cur_raw.get("spotlight"),
        # ── B. Social-only sections (new, additive) ────────────────────────
        "freshest_alpha":         _build_freshest_alpha(cur_raw),
        "theme_leadership":       _build_theme_leadership(cur_raw),
        "sentiment_acceleration": _build_sentiment_accel(cur_raw, prev_raw),
        # ── D. Social-specific metadata (richer than Home subset) ──────────
        "metadata":               _build_metadata(current_snap),
    }
