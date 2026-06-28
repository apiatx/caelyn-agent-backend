"""
Kalshi Public Market Scanner.

Fetches daily binary direction markets and year-end milestone markets from
Kalshi's public REST API.  No authentication is required for public display
data — auth failure is reported in diagnostics only and never blocks display.

Public entry point
------------------
scan_kalshi()
    async → dict[family_key → normalized_row, "_diagnostics" → dict]

Normalized rows carry the same field shape consumed by odds_scanner._do_scan()
for injection into live_pre (provider: "kalshi" field distinguishes them).

Probability rule
----------------
1. Midpoint of yes_bid_dollars and yes_ask_dollars if both present and valid.
2. last_price_dollars if midpoint unavailable.
3. Either bid or ask as last fallback.
All values normalized to 0–1.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("kalshi_scanner")

# ── Constants ─────────────────────────────────────────────────────────────────

KALSHI_BASE    = "https://external-api.kalshi.com/trade-api/v2"
_FETCH_TIMEOUT = 12          # seconds per HTTP request
_SCAN_TIMEOUT  = 30          # seconds for the entire Kalshi scan

# Kalshi series → family_key for daily binary direction markets
_DAILY_SERIES: dict[str, str] = {
    "KXINXDUD":    "spx_daily_direction",
    "KXNASDAQDUD": "nasdaq_daily_direction",
}

# Kalshi series → family_key for year-end milestone ladders
_YEAREND_SERIES: dict[str, str] = {
    "KXINXDIRY": "spx_dec31_milestone",
}

# All family keys that Kalshi scanner handles
KALSHI_PRIMARY_FAMILIES: frozenset = frozenset(
    list(_DAILY_SERIES.values()) + list(_YEAREND_SERIES.values())
)


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def _fetch(path: str) -> dict:
    """
    Fetch a Kalshi public API path and return parsed JSON.
    Runs the blocking urllib call in a thread executor.
    """
    url = KALSHI_BASE + path
    loop = asyncio.get_running_loop()

    def _do_get() -> dict:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT) as r:
            return json.loads(r.read())

    return await loop.run_in_executor(None, _do_get)


# ── Probability extraction ────────────────────────────────────────────────────

def _kalshi_prob(market: dict) -> Optional[float]:
    """
    Return 0–1 probability from a Kalshi market dict.
    Prefers bid/ask midpoint → last_price → single side fallback.
    """
    bid_raw = market.get("yes_bid_dollars")
    ask_raw = market.get("yes_ask_dollars")
    if bid_raw is not None and ask_raw is not None:
        try:
            mid = (float(bid_raw) + float(ask_raw)) / 2.0
            if 0.0 < mid < 1.0:
                return round(mid, 4)
        except (TypeError, ValueError):
            pass

    last_raw = market.get("last_price_dollars")
    if last_raw is not None:
        try:
            p = float(last_raw)
            if 0.0 < p <= 1.0:
                return round(p, 4)
        except (TypeError, ValueError):
            pass

    for v in (bid_raw, ask_raw):
        if v is not None:
            try:
                p = float(v)
                if 0.0 < p <= 1.0:
                    return round(p, 4)
            except (TypeError, ValueError):
                pass
    return None


# ── Event-selection helpers ───────────────────────────────────────────────────

def _select_nearest_open_event(events: list[dict]) -> Optional[dict]:
    """
    From a list of open Kalshi events, select the one that expires soonest
    (nearest-dated daily direction market).  Falls back to the first event.
    """
    now = datetime.now(timezone.utc)
    best: Optional[dict] = None
    best_dt: Optional[datetime] = None

    for e in events:
        close_raw = (
            e.get("close_time") or e.get("close_at") or
            e.get("end_date") or e.get("end_time") or ""
        )
        if close_raw:
            try:
                dt = datetime.fromisoformat(str(close_raw).replace("Z", "+00:00"))
                if dt > now:
                    if best_dt is None or dt < best_dt:
                        best_dt = dt
                        best = e
                    continue
            except Exception:
                pass
        if best is None:
            best = e

    return best or (events[0] if events else None)


def _select_dec31_event(events: list[dict]) -> Optional[dict]:
    """
    From a list of open KXINXDIRY events, select the Dec 31 year-end event.
    Matches on ticker containing 'DEC31' or title containing 'year-end' / 'dec 31'.
    """
    for e in events:
        ticker = (e.get("event_ticker") or "").upper()
        title  = (e.get("title") or "").lower()
        if "DEC31" in ticker or "DEC-31" in ticker:
            return e
        if "dec 31" in title or "year-end" in title or "year end" in title:
            return e
    return events[0] if events else None


# ── Row builders ──────────────────────────────────────────────────────────────

def _build_daily_row(
    family_key: str,
    event: dict,
    markets: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Normalize a Kalshi daily binary direction event → tracked-odds row shape.

    The returned dict carries all fields expected by odds_scanner._do_scan()
    live_pre entries, plus Kalshi-specific _kalshi_* id fields.
    """
    if not markets:
        return None

    # Only consider active/open markets
    active = [m for m in markets if m.get("status") in ("active", "open", None)]
    if not active:
        active = list(markets)

    # Pick highest-volume market (there is typically exactly one per daily event)
    mkt = max(active, key=lambda m: float(m.get("volume_fp") or 0))

    # Check staleness: skip if market closed > 1 h ago
    close_raw = mkt.get("close_time") or mkt.get("expiration_time") or ""
    end_date: Optional[str] = None
    if close_raw:
        try:
            close_dt = datetime.fromisoformat(str(close_raw).replace("Z", "+00:00"))
            delta_h = (close_dt - now_dt).total_seconds() / 3600.0
            if delta_h < -1.0:
                return None          # past expiry by more than 1 hour
            end_date = close_dt.isoformat()
        except Exception:
            end_date = close_raw

    prob = _kalshi_prob(mkt)
    if prob is None:
        return None

    event_ticker = event.get("event_ticker") or ""
    mkt_ticker   = mkt.get("ticker") or ""
    yes_sub      = (mkt.get("yes_sub_title") or mkt.get("yes_subtitle") or "").strip()
    event_title  = (event.get("title") or "").strip()
    sub_title    = (event.get("sub_title") or event.get("subtitle") or "").strip()

    # Infer instrument label
    if family_key == "spx_daily_direction":
        instrument = "S&P 500"
    elif family_key == "nasdaq_daily_direction":
        instrument = "NASDAQ-100"
    else:
        instrument = event_title or family_key.replace("_", " ").title()

    # Strike label from yes_sub_title ("Above 7354.02" → use as-is)
    strike_label = yes_sub or "Up"
    date_label   = sub_title or (
        datetime.fromisoformat(end_date.replace("Z", "+00:00")).strftime("%b %d")
        if end_date else ""
    )

    question = (
        mkt.get("title")
        or f"Will the {instrument} be {strike_label} on {date_label}?"
    )

    yes_pct  = round(prob * 100.0, 1)
    no_prob  = round(1.0 - prob, 4)
    no_pct   = round((1.0 - prob) * 100.0, 1)

    vol_total = float(mkt.get("volume_fp") or 0)
    vol_24h   = float(mkt.get("volume_24h_fp") or 0)
    oi        = float(mkt.get("open_interest_fp") or 0)
    liq       = float(mkt.get("liquidity_dollars") or 0)

    outcomes = [
        {
            "label":        "Yes",
            "display_label": f"Up · {strike_label}",
            "probability":   prob,
            "pct":           yes_pct,
            "volume_24h":    vol_24h,
            "open_interest": oi,
        },
        {
            "label":        "No",
            "display_label": "Down",
            "probability":   no_prob,
            "pct":           no_pct,
            "volume_24h":    None,
            "open_interest": None,
        },
    ]
    outcome_summary = f"Up {yes_pct}% · Down {no_pct}%"
    url = f"https://kalshi.com/markets/{mkt_ticker}"

    quality: str
    if vol_total >= 1000:
        quality = "high"
    elif vol_total >= 100:
        quality = "moderate"
    else:
        quality = "low"

    return {
        # ── Provider / identity ──────────────────────────────────────────────
        "provider":              "kalshi",
        "family_key":            family_key,
        "_kalshi_market_ticker": mkt_ticker,
        "_kalshi_event_ticker":  event_ticker,
        "_kalshi_series_ticker": event.get("series_ticker") or "",
        # ── Display fields ───────────────────────────────────────────────────
        "question":              question,
        "market_question":       question,
        "event_title":           event_title,
        "display_title":         f"{instrument} Daily Direction",
        "display_subtitle":      date_label,
        "contract_context":      date_label,
        # ── Outcome fields ───────────────────────────────────────────────────
        "priced_outcome":        "Yes",
        "priced_outcome_label":  f"Up · {strike_label}",
        "priced_probability":    prob,
        "yes_probability":       prob,
        "yes_pct":               yes_pct,
        "outcomes":              outcomes,
        "outcome_summary":       outcome_summary,
        # ── Market metadata ──────────────────────────────────────────────────
        "end_date":              end_date,
        "url":                   url,
        "volume_24h":            vol_24h,
        "volume_total":          vol_total,
        "liquidity":             liq,
        "open_interest":         oi,
        "quality":               quality,
        # ── Staging fields for odds_scanner integration ──────────────────────
        "condition_id":          None,
        "slug":                  mkt_ticker,
        "market_slug":           mkt_ticker,
        "event_slug":            event_ticker,
        "clob_token_ids":        [],
        "neg_risk":              False,
        "candidate_count":       1,
        "driver_markets":        [],
        "delta_1h_pp":           None,
        "delta_24h_pp":          None,
        "delta_7d_pp":           None,
        "market_read":           None,
        "exposure":              None,
        "_yes_pct_raw":          yes_pct,
        "_api_1h":               None,
        "_api_24h":              None,
        "_api_7d":               None,
    }


def _build_dec31_row(
    family_key: str,
    event: dict,
    markets: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Normalize a Kalshi Dec 31 year-end ladder event → tracked-odds row shape.

    Iterates all strike markets, builds a full outcomes[] ladder, and picks
    the highest-volume / highest-OI strike as the priced outcome.
    """
    if not markets:
        return None

    active = [m for m in markets if m.get("status") in ("active", "open", None)]
    if not active:
        active = list(markets)

    # Sort by strike value (extracted from ticker suffix -TNNNN)
    def _strike_val(m: dict) -> float:
        try:
            return float((m.get("ticker") or "").split("-T")[-1])
        except Exception:
            return 0.0

    active_sorted = sorted(active, key=_strike_val)

    # Build outcomes[]
    outcomes: list[dict] = []
    for m in active_sorted:
        prob = _kalshi_prob(m)
        if prob is None:
            continue
        yes_sub    = (m.get("yes_sub_title") or m.get("yes_subtitle") or "").strip()
        strike_val = _strike_val(m)
        label      = yes_sub or f"Above {strike_val:,.0f}"
        outcomes.append({
            "label":         "Yes",
            "display_label": label,
            "strike":        strike_val,
            "probability":   prob,
            "pct":           round(prob * 100.0, 1),
            "volume_24h":    float(m.get("volume_24h_fp") or 0),
            "open_interest": float(m.get("open_interest_fp") or 0),
            "market_ticker": m.get("ticker") or "",
        })

    if not outcomes:
        return None

    # Pick priced outcome: highest volume, break ties by highest OI
    best_outcome = max(
        outcomes,
        key=lambda o: (o.get("volume_24h") or 0, o.get("open_interest") or 0),
    )

    event_ticker = event.get("event_ticker") or ""
    event_title  = (event.get("title") or "S&P price at year-end?").strip()
    sub_title    = (event.get("sub_title") or event.get("subtitle") or "Dec 31, 2026").strip()

    # end_date from first active market's close_time
    end_date: Optional[str] = None
    for m in active_sorted:
        close_raw = m.get("close_time") or m.get("expiration_time") or ""
        if close_raw:
            try:
                close_dt = datetime.fromisoformat(str(close_raw).replace("Z", "+00:00"))
                end_date = close_dt.isoformat()
            except Exception:
                end_date = close_raw
            break

    prob         = best_outcome["probability"]
    yes_pct      = best_outcome["pct"]
    strike_label = best_outcome["display_label"]
    question     = f"Will the S&P 500 be {strike_label} on Dec 31?"

    vol_total    = sum(float(m.get("volume_fp") or 0) for m in active_sorted)
    vol_24h      = sum(o.get("volume_24h") or 0 for o in outcomes)
    oi           = sum(o.get("open_interest") or 0 for o in outcomes)

    outcome_summary = (
        f"{len(outcomes)}-strike ladder · "
        f"Priced: {strike_label} @ {yes_pct}%"
    )
    url = f"https://kalshi.com/events/{event_ticker}"

    quality = "moderate" if vol_total >= 50 else "low"

    return {
        "provider":              "kalshi",
        "family_key":            family_key,
        "_kalshi_market_ticker": best_outcome.get("market_ticker") or "",
        "_kalshi_event_ticker":  event_ticker,
        "_kalshi_series_ticker": event.get("series_ticker") or "",
        "question":              question,
        "market_question":       question,
        "event_title":           event_title,
        "display_title":         "S&P 500 Year-End Level",
        "display_subtitle":      sub_title,
        "contract_context":      "Dec 31, 2026",
        "priced_outcome":        "Yes",
        "priced_outcome_label":  strike_label,
        "priced_probability":    prob,
        "yes_probability":       prob,
        "yes_pct":               yes_pct,
        "outcomes":              outcomes,
        "outcome_summary":       outcome_summary,
        "end_date":              end_date,
        "url":                   url,
        "volume_24h":            vol_24h,
        "volume_total":          vol_total,
        "liquidity":             0.0,
        "open_interest":         oi,
        "quality":               quality,
        "condition_id":          None,
        "slug":                  event_ticker,
        "market_slug":           event_ticker,
        "event_slug":            event_ticker,
        "clob_token_ids":        [],
        "neg_risk":              False,
        "candidate_count":       len(outcomes),
        "driver_markets":        [],
        "delta_1h_pp":           None,
        "delta_24h_pp":          None,
        "delta_7d_pp":           None,
        "market_read":           None,
        "exposure":              None,
        "_yes_pct_raw":          yes_pct,
        "_api_1h":               None,
        "_api_24h":              None,
        "_api_7d":               None,
    }


# ── Auth diagnostic (non-blocking) ───────────────────────────────────────────

def _check_auth_env() -> tuple[bool, str]:
    """
    Return (credentials_present, error_type) without performing an actual auth
    attempt (no HTTP call — key validation is expensive and can 401).
    Surfaces key presence/format only.
    """
    key_id  = os.environ.get("KALSHI_API_KEY_ID", "")
    pem_key = os.environ.get("KALSHI_PRIVATE_KEY", "")

    if not key_id or not pem_key:
        return False, "credentials_missing"

    # Basic format checks — no secrets are logged
    if len(key_id) < 30:
        return False, "key_id_malformed"
    if "BEGIN" not in pem_key and "END" not in pem_key and len(pem_key) < 200:
        return False, "private_key_malformed"

    return True, "credentials_present_unverified"


# ── Main scan ─────────────────────────────────────────────────────────────────

async def scan_kalshi() -> dict:
    """
    Fetch all Kalshi primary-family markets and return normalized rows.

    Returns a dict:
      {
        "spx_daily_direction":    {...row...},
        "nasdaq_daily_direction": {...row...},
        "spx_dec31_milestone":    {...row...},
        "_diagnostics":           {...},
      }

    Always returns without raising — failures are captured in _diagnostics.
    """
    t0 = time.time()
    now_dt = datetime.now(timezone.utc)

    rows: dict[str, dict] = {}
    diag: dict[str, Any] = {
        "kalshi_public_api_ok":         False,
        "kalshi_auth_ok":               False,
        "kalshi_auth_error_type":       "not_attempted",
        "kalshi_spx_daily_matched":     False,
        "kalshi_nasdaq_daily_matched":  False,
        "kalshi_spx_dec31_matched":     False,
        "kalshi_rows_returned":         0,
        "kalshi_scan_ms":               None,
        "kalshi_error":                 None,
    }

    # Credentials presence (non-blocking, no secrets logged)
    creds_ok, creds_status = _check_auth_env()
    diag["kalshi_auth_error_type"] = creds_status if not creds_ok else "credentials_present_unverified"

    try:
        # ── Fetch events list for all tracked series in parallel ──────────
        series_list = list(_DAILY_SERIES.items()) + list(_YEAREND_SERIES.items())

        event_list_tasks = [
            _fetch(f"/events?series_ticker={series}&status=open&limit=10")
            for series, _ in series_list
        ]
        event_list_results = await asyncio.gather(*event_list_tasks, return_exceptions=True)

        any_api_ok = any(
            not isinstance(r, Exception)
            for r in event_list_results
        )
        diag["kalshi_public_api_ok"] = any_api_ok

        if not any_api_ok:
            first_err = next(
                (r for r in event_list_results if isinstance(r, Exception)), None
            )
            diag["kalshi_error"] = str(first_err) if first_err else "all_fetches_failed"
            diag["kalshi_scan_ms"] = round((time.time() - t0) * 1000)
            rows["_diagnostics"] = diag
            return rows

        # ── Select events and fetch full detail ───────────────────────────
        detail_tasks   = []
        detail_mapping = []   # (series_ticker, family_key, is_yearend)

        for (series, family_key), result in zip(series_list, event_list_results):
            if isinstance(result, Exception):
                log.warning(
                    "[kalshi_scanner] events fetch failed for %s (%s): %s",
                    series, family_key, result,
                )
                continue

            events = result.get("events") or []
            if not events:
                log.info("[kalshi_scanner] no open events for series=%s", series)
                continue

            is_yearend = series in _YEAREND_SERIES

            if is_yearend:
                chosen = _select_dec31_event(events)
            else:
                chosen = _select_nearest_open_event(events)

            if chosen is None:
                continue

            event_ticker = chosen.get("event_ticker") or ""
            if not event_ticker:
                continue

            detail_tasks.append(_fetch(f"/events/{event_ticker}"))
            detail_mapping.append((series, family_key, is_yearend, chosen))

        if not detail_tasks:
            diag["kalshi_scan_ms"] = round((time.time() - t0) * 1000)
            rows["_diagnostics"] = diag
            return rows

        detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

        for (series, family_key, is_yearend, event_shell), detail_result in zip(
            detail_mapping, detail_results
        ):
            if isinstance(detail_result, Exception):
                log.warning(
                    "[kalshi_scanner] event detail fetch failed for %s: %s",
                    family_key, detail_result,
                )
                continue

            event_full = detail_result.get("event") or event_shell
            markets    = detail_result.get("markets") or []

            if is_yearend:
                row = _build_dec31_row(family_key, event_full, markets, now_dt)
                if row:
                    rows[family_key] = row
                    diag["kalshi_spx_dec31_matched"] = True
                    log.info(
                        "[kalshi_scanner] %s matched: %d-strike ladder",
                        family_key, len(row.get("outcomes") or []),
                    )
            else:
                row = _build_daily_row(family_key, event_full, markets, now_dt)
                if row:
                    rows[family_key] = row
                    if family_key == "spx_daily_direction":
                        diag["kalshi_spx_daily_matched"] = True
                    elif family_key == "nasdaq_daily_direction":
                        diag["kalshi_nasdaq_daily_matched"] = True
                    log.info(
                        "[kalshi_scanner] %s matched: prob=%.1f%% vol=%.0f",
                        family_key,
                        (row.get("yes_pct") or 0),
                        (row.get("volume_total") or 0),
                    )

        diag["kalshi_rows_returned"] = len(rows)

    except Exception as exc:
        diag["kalshi_error"] = str(exc)
        log.warning("[kalshi_scanner] scan_kalshi unexpected error: %s", exc)

    diag["kalshi_scan_ms"] = round((time.time() - t0) * 1000)
    rows["_diagnostics"] = diag
    return rows
