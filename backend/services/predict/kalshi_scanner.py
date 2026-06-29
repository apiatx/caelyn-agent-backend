"""
Kalshi Public Market Scanner.

Fetches daily binary direction markets, year-end milestone ladders, range
ladders, one-touch ladders, and matchup markets from Kalshi's public REST API.
No authentication is required for public display data — auth failure is
reported in diagnostics only and never blocks display.

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

Series coverage
---------------
Binary daily:   KXINXDUD (SPX), KXNASDAQDUD (Nasdaq)
Yearend ladder: KXINXDIRY (SPX Dec31 milestone)
Range ladders:  KXINX (SPX tomorrow close), KXINXY (SPX year-end range),
                KXNASDAQ100Y (Nasdaq year-end range)
One-touch:      KXINXMAXY (SPX year high), KXINXMAXMM (SPX month-end high)
Matchup:        KXINXVSGOLD (SPX vs Gold annual return)
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

KALSHI_BASE             = "https://external-api.kalshi.com/trade-api/v2"
_FETCH_TIMEOUT          = 12   # seconds per HTTP request
_MAX_MARKETS_PER_SERIES = 40   # cap market list per series (paginated)
_MAX_CONCURRENT_DETAIL  = 4    # max concurrent individual market-detail fetches
_FETCH_MAX_RETRIES      = 3    # retries on 429 / transient errors
_FETCH_RETRY_BASE_S     = 1.5  # base sleep seconds (doubles each retry)

# ── Series configs ────────────────────────────────────────────────────────────

# Binary daily direction (original) — scanned via /events path
_DAILY_SERIES: dict[str, str] = {
    "KXINXDUD":    "spx_daily_direction",
    "KXNASDAQDUD": "nasdaq_daily_direction",
}

# Year-end milestone ladders (original) — scanned via /events path
_YEAREND_SERIES: dict[str, str] = {
    "KXINXDIRY": "spx_dec31_milestone",
}

# New series — must use /markets?series_ticker=X path because
# /events/{event_ticker} returns empty markets[] for them.
# Each value: (family_key, row_type)
#   row_type: "range" | "onetouch" | "matchup"
_MARKETS_PATH_SERIES: dict[str, tuple[str, str]] = {
    "KXINX":        ("spx_tomorrow_close_ladder",  "range"),
    "KXINXY":       ("spx_year_end_close_range",   "range"),
    "KXINXMAXY":    ("spx_year_high_ladder",       "onetouch"),
    "KXINXMAXMM":   ("spx_month_end_high_ladder",  "onetouch"),
    "KXINXVSGOLD":  ("spx_vs_gold_annual_return",  "matchup"),
    "KXNASDAQ100Y": ("nasdaq_year_end_close_range", "range"),
}

# Human-readable instrument label per family
_INSTRUMENT_LABELS: dict[str, str] = {
    "spx_daily_direction":        "S&P 500",
    "nasdaq_daily_direction":     "Nasdaq-100",
    "spx_dec31_milestone":        "S&P 500",
    "spx_tomorrow_close_ladder":  "S&P 500",
    "spx_year_end_close_range":   "S&P 500",
    "spx_year_high_ladder":       "S&P 500",
    "spx_month_end_high_ladder":  "S&P 500",
    "spx_vs_gold_annual_return":  "S&P 500",
    "nasdaq_year_end_close_range": "Nasdaq-100",
}

# Display titles per family
_DISPLAY_TITLES: dict[str, str] = {
    "spx_daily_direction":        "S&P 500 Daily Direction",
    "nasdaq_daily_direction":     "Nasdaq-100 Daily Direction",
    "spx_dec31_milestone":        "S&P 500 Year-End Level",
    "spx_tomorrow_close_ladder":  "S&P 500 Tomorrow Close",
    "spx_year_end_close_range":   "S&P 500 Year-End Close Range",
    "spx_year_high_ladder":       "S&P 500 Year High",
    "spx_month_end_high_ladder":  "S&P 500 Month-End High",
    "spx_vs_gold_annual_return":  "S&P 500 vs. Gold (Annual Return)",
    "nasdaq_year_end_close_range": "Nasdaq-100 Year-End Close Range",
}

# All family keys that Kalshi scanner owns (exported to odds_scanner)
KALSHI_PRIMARY_FAMILIES: frozenset = frozenset(
    list(_DAILY_SERIES.values())
    + list(_YEAREND_SERIES.values())
    + [fk for fk, _ in _MARKETS_PATH_SERIES.values()]
)


# ── HTTP helpers ───────────────────────────────────────────────────────────────

async def _fetch(path: str) -> dict:
    """
    Fetch a Kalshi public API path and return parsed JSON.
    Runs the blocking urllib call in a thread executor.
    Retries up to _FETCH_MAX_RETRIES times on 429 or transient errors,
    with exponential backoff starting at _FETCH_RETRY_BASE_S seconds.
    """
    url = KALSHI_BASE + path
    loop = asyncio.get_running_loop()

    def _do_get() -> dict:
        last_exc: Exception | None = None
        for attempt in range(_FETCH_MAX_RETRIES + 1):
            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT) as r:
                    return json.loads(r.read())
            except urllib.error.HTTPError as exc:
                last_exc = exc
                if exc.code == 429 and attempt < _FETCH_MAX_RETRIES:
                    sleep_s = _FETCH_RETRY_BASE_S * (2 ** attempt)
                    log.debug(
                        "[kalshi_scanner] 429 on %s — retry %d/%d in %.1fs",
                        path, attempt + 1, _FETCH_MAX_RETRIES, sleep_s,
                    )
                    time.sleep(sleep_s)
                    continue
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < _FETCH_MAX_RETRIES:
                    sleep_s = _FETCH_RETRY_BASE_S * (2 ** attempt)
                    time.sleep(sleep_s)
                    continue
                raise
        raise last_exc  # type: ignore[misc]

    return await loop.run_in_executor(None, _do_get)


async def _fetch_market_detail(ticker: str) -> dict:
    """
    Fetch a single Kalshi market by ticker from /markets/{ticker}.
    Returns the market dict (with full pricing fields) or {} on error.
    """
    try:
        result = await _fetch(f"/markets/{ticker}")
        return result.get("market") or {}
    except Exception as exc:
        log.debug("[kalshi_scanner] market detail fetch failed %s: %s", ticker, exc)
        return {}


async def _fetch_markets_list(
    series_ticker: str,
    max_markets: int = _MAX_MARKETS_PER_SERIES,
) -> list[dict]:
    """
    Fetch the flat list of markets for a series via /markets?series_ticker=X.
    Returns list of dicts with at least 'ticker' field (no pricing).
    """
    try:
        result = await _fetch(
            f"/markets?series_ticker={series_ticker}&status=open&limit={max_markets}"
        )
        return result.get("markets") or []
    except Exception as exc:
        log.warning("[kalshi_scanner] markets list failed for %s: %s", series_ticker, exc)
        return []


async def _fetch_markets_with_pricing(
    tickers: list[str],
) -> list[dict]:
    """
    Concurrently fetch individual market details for a list of tickers.
    Respects _MAX_CONCURRENT_DETAIL concurrency limit.
    Returns list of market dicts in the same order (failed ones are {}).
    """
    sem = asyncio.Semaphore(_MAX_CONCURRENT_DETAIL)

    async def _guarded_fetch(ticker: str) -> dict:
        async with sem:
            return await _fetch_market_detail(ticker)

    results = await asyncio.gather(*[_guarded_fetch(t) for t in tickers])
    return list(results)


# ── Probability extraction ─────────────────────────────────────────────────────

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


# ── Event-selection helpers (used by /events path) ────────────────────────────

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


# ── Market selection helpers ───────────────────────────────────────────────────

def _event_ticker_from_market(ticker: str) -> str:
    """
    Derive the event_ticker from a market ticker by stripping the last segment.
    e.g. KXINXY-26DEC31H1600-B7900 → KXINXY-26DEC31H1600
    """
    return ticker.rsplit("-", 1)[0]


def _select_best_event_markets(
    all_markets: list[dict],
) -> tuple[str, list[dict]]:
    """
    Group markets by derived event_ticker, select the event group that has
    the most markets with live pricing (non-zero yes_bid_dollars or
    last_price_dollars). Returns (event_ticker, markets_for_that_event).

    For series like KXINXMAXY that have a dead event (31DEC2026, all-zero)
    and an active event (01JAN2027, priced), this picks the active one.
    """
    from collections import defaultdict

    groups: dict[str, list[dict]] = defaultdict(list)
    for m in all_markets:
        t = m.get("ticker") or ""
        ev = _event_ticker_from_market(t)
        groups[ev].append(m)

    if not groups:
        return "", []

    if len(groups) == 1:
        ev, mks = next(iter(groups.items()))
        return ev, mks

    # Prefer event with most priced markets
    def _priced_count(mks: list[dict]) -> int:
        return sum(
            1 for m in mks
            if m.get("yes_bid_dollars") not in (None, "0.0000", "0.00", "0")
            or m.get("last_price_dollars") not in (None, "0.0000", "0.00", "0")
            or float(m.get("volume_fp") or 0) > 0
        )

    best_ev, best_mks = max(groups.items(), key=lambda kv: _priced_count(kv[1]))
    return best_ev, best_mks


def _extract_strike_value(ticker: str) -> float:
    """
    Extract the numeric strike value from the last segment of a market ticker.
    e.g. KXINXY-26DEC31H1600-B7900 → 7900.0
         KXINXMAXY-01JAN2027-7799.99 → 7799.99
         KXINXDIRY-26DEC31H1600-T8500 → 8500.0 (strips T/B prefix)
    """
    seg = (ticker or "").rsplit("-", 1)[-1]
    # Strip optional T/B prefix (threshold / bucket)
    if seg and seg[0] in ("T", "B", "t", "b"):
        seg = seg[1:]
    try:
        return float(seg)
    except (ValueError, TypeError):
        return 0.0


def _quality(vol_total: float, vol_24h: float) -> dict:
    if vol_total >= 50_000:
        score, label, reason = 0.95, "high", f"high lifetime volume ({int(vol_total):,} contracts)"
    elif vol_total >= 10_000 or vol_24h >= 5_000:
        score, label, reason = 0.75, "high", "solid volume"
    elif vol_total >= 1_000 or vol_24h >= 100:
        score, label, reason = 0.50, "medium", "moderate volume"
    elif vol_total > 0:
        score, label, reason = 0.25, "low", "low volume, treat signals with caution"
    else:
        score, label, reason = 0.10, "low", "nascent market, no volume"
    return {"quality_score": score, "quality_label": label, "quality_reason": reason}


# ── Shared row template ────────────────────────────────────────────────────────

def _base_row(
    family_key: str,
    event_ticker: str,
    series_ticker: str,
    market_ticker: str,
    question: str,
    display_subtitle: str,
    contract_context: str,
    priced_outcome: str,
    priced_outcome_label: str,
    prob: float,
    yes_pct: float,
    outcomes: list[dict],
    outcome_summary: str,
    end_date: Optional[str],
    url: str,
    vol_24h: float,
    vol_total: float,
    oi: float,
    liq: float,
    candidate_count: int = 1,
    most_likely_outcome_label: Optional[str] = None,
    most_likely_probability: Optional[float] = None,
) -> dict:
    """Return a normalized row dict with the standard shape for odds_scanner."""
    instrument = _INSTRUMENT_LABELS.get(family_key, "S&P 500")
    display_title = _DISPLAY_TITLES.get(family_key, family_key.replace("_", " ").title())

    return {
        "provider":              "kalshi",
        "family_key":            family_key,
        "_kalshi_market_ticker": market_ticker,
        "_kalshi_event_ticker":  event_ticker,
        "_kalshi_series_ticker": series_ticker,
        "question":              question,
        "market_question":       question,
        "event_title":           display_title,
        "display_title":         display_title,
        "display_subtitle":      display_subtitle,
        "contract_context":      contract_context,
        "priced_outcome":        priced_outcome,
        "priced_outcome_label":  priced_outcome_label,
        "priced_probability":    prob,
        "yes_probability":       prob,
        "yes_pct":               yes_pct,
        "outcomes":              outcomes,
        "outcome_summary":       outcome_summary,
        "end_date":              end_date,
        "url":                   url,
        "volume_24h":            vol_24h,
        "volume_total":          vol_total,
        "liquidity":             liq,
        "open_interest":         oi,
        "quality":               _quality(vol_total, vol_24h),
        "most_likely_outcome_label": (
            most_likely_outcome_label if most_likely_outcome_label is not None
            else priced_outcome_label
        ),
        "most_likely_probability": (
            most_likely_probability if most_likely_probability is not None
            else prob
        ),
        # Staging fields for odds_scanner integration
        "condition_id":          None,
        "slug":                  market_ticker or event_ticker,
        "market_slug":           market_ticker or event_ticker,
        "event_slug":            event_ticker,
        "clob_token_ids":        [],
        "neg_risk":              False,
        "candidate_count":       candidate_count,
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


# ── Row builders (original) ───────────────────────────────────────────────────

def _build_daily_row(
    family_key: str,
    event: dict,
    markets: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Normalize a Kalshi daily binary direction event → tracked-odds row shape.
    """
    if not markets:
        return None

    active = [m for m in markets if m.get("status") in ("active", "open", None)]
    if not active:
        active = list(markets)

    mkt = max(active, key=lambda m: float(m.get("volume_fp") or 0))

    close_raw = mkt.get("close_time") or mkt.get("expiration_time") or ""
    end_date: Optional[str] = None
    if close_raw:
        try:
            close_dt = datetime.fromisoformat(str(close_raw).replace("Z", "+00:00"))
            delta_h = (close_dt - now_dt).total_seconds() / 3600.0
            if delta_h < -1.0:
                return None
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

    if family_key == "spx_daily_direction":
        instrument = "S&P 500"
    elif family_key == "nasdaq_daily_direction":
        instrument = "Nasdaq-100"
    else:
        instrument = event_title or family_key.replace("_", " ").title()

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
            "label":         "Yes",
            "display_label": f"Up · {strike_label}",
            "probability":   prob,
            "pct":           yes_pct,
            "volume_24h":    vol_24h,
            "open_interest": oi,
        },
        {
            "label":         "No",
            "display_label": "Down",
            "probability":   no_prob,
            "pct":           no_pct,
            "volume_24h":    None,
            "open_interest": None,
        },
    ]
    outcome_summary = f"Up {yes_pct}% · Down {no_pct}%"
    url = f"https://kalshi.com/markets/{mkt_ticker}"

    return _base_row(
        family_key=family_key,
        event_ticker=event_ticker,
        series_ticker=event.get("series_ticker") or "",
        market_ticker=mkt_ticker,
        question=question,
        display_subtitle=date_label,
        contract_context=date_label,
        priced_outcome="Yes",
        priced_outcome_label=f"Up · {strike_label}",
        prob=prob,
        yes_pct=yes_pct,
        outcomes=outcomes,
        outcome_summary=outcome_summary,
        end_date=end_date,
        url=url,
        vol_24h=vol_24h,
        vol_total=vol_total,
        oi=oi,
        liq=liq,
        most_likely_outcome_label=(f"Up · {strike_label}" if prob >= 0.50 else "Down"),
        most_likely_probability=(prob if prob >= 0.50 else round(1.0 - prob, 4)),
    )


def _build_dec31_row(
    family_key: str,
    event: dict,
    markets: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Normalize a Kalshi Dec 31 year-end ladder event → tracked-odds row shape.
    """
    if not markets:
        return None

    active = [m for m in markets if m.get("status") in ("active", "open", None)]
    if not active:
        active = list(markets)

    def _strike_val(m: dict) -> float:
        try:
            return float((m.get("ticker") or "").split("-T")[-1])
        except Exception:
            return 0.0

    active_sorted = sorted(active, key=_strike_val)

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

    # Headline: highest probability bucket (most likely outcome for year-end close).
    best_outcome = max(
        outcomes,
        key=lambda o: (o.get("probability") or 0, o.get("volume_24h") or 0, o.get("open_interest") or 0),
    )

    event_ticker = event.get("event_ticker") or ""
    event_title  = (event.get("title") or "S&P price at year-end?").strip()
    sub_title    = (event.get("sub_title") or event.get("subtitle") or "Dec 31, 2026").strip()

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

    return _base_row(
        family_key=family_key,
        event_ticker=event_ticker,
        series_ticker=event.get("series_ticker") or "",
        market_ticker=best_outcome.get("market_ticker") or "",
        question=question,
        display_subtitle=sub_title,
        contract_context="Dec 31, 2026",
        priced_outcome="Yes",
        priced_outcome_label=strike_label,
        prob=prob,
        yes_pct=yes_pct,
        outcomes=outcomes,
        outcome_summary=outcome_summary,
        end_date=end_date,
        url=url,
        vol_24h=vol_24h,
        vol_total=vol_total,
        oi=oi,
        liq=0.0,
        candidate_count=len(outcomes),
        most_likely_outcome_label=strike_label,
        most_likely_probability=prob,
    )


# ── Row builders (new series via /markets path) ────────────────────────────────

def _build_range_row(
    family_key: str,
    series_ticker: str,
    event_ticker: str,
    markets_with_pricing: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Build a row for a range/bucket ladder market.
    Each market is a distinct price range like "7,675 to 7,699.9999".

    Headline: bucket with highest total volume; fall back to highest OI;
    fall back to market with probability closest to 0.25 (most "interesting"
    for a range ladder where max-prob bucket might be an outlier).
    """
    priced = []
    for m in markets_with_pricing:
        if not m:
            continue
        prob = _kalshi_prob(m)
        if prob is None:
            continue
        sub  = (m.get("subtitle") or "").strip()
        vol  = float(m.get("volume_fp") or 0)
        v24  = float(m.get("volume_24h_fp") or 0)
        oi   = float(m.get("open_interest_fp") or 0)
        liq  = float(m.get("liquidity_dollars") or 0)
        priced.append({
            "market":   m,
            "prob":     prob,
            "pct":      round(prob * 100.0, 1),
            "subtitle": sub,
            "vol":      vol,
            "vol_24h":  v24,
            "oi":       oi,
            "liq":      liq,
            "strike":   _extract_strike_value(m.get("ticker") or ""),
        })

    if not priced:
        return None

    # Sort by strike descending (highest range first)
    priced_sorted = sorted(priced, key=lambda x: x["strike"], reverse=True)

    outcomes = [
        {
            "label":         p["subtitle"] or f"Range {p['strike']:,.0f}",
            "display_label": p["subtitle"] or f"Range {p['strike']:,.0f}",
            "strike":        p["strike"],
            "probability":   p["prob"],
            "pct":           p["pct"],
            "volume_24h":    p["vol_24h"],
            "open_interest": p["oi"],
            "market_ticker": p["market"].get("ticker") or "",
        }
        for p in priced_sorted
    ]

    # Headline: highest probability bucket (most likely outcome).
    # Ties broken by volume then OI; ensures the displayed range reflects
    # where the market actually expects the index to close, not where
    # the most early speculative volume happened to accumulate.
    best = max(priced_sorted, key=lambda x: (x["prob"], x["vol"], x["oi"]))

    headline_label = best["subtitle"] or f"Range {best['strike']:,.0f}"
    prob  = best["prob"]
    yes_pct = best["pct"]
    mkt_ticker = best["market"].get("ticker") or ""

    vol_total = sum(p["vol"] for p in priced_sorted)
    vol_24h   = sum(p["vol_24h"] for p in priced_sorted)
    oi        = sum(p["oi"] for p in priced_sorted)
    liq       = sum(p["liq"] for p in priced_sorted)

    instrument = _INSTRUMENT_LABELS.get(family_key, "Index")

    # Derive end_date from market close_time
    end_date: Optional[str] = None
    for pm in priced_sorted:
        ct = pm["market"].get("close_time") or pm["market"].get("expiration_time") or ""
        if ct:
            try:
                end_date = datetime.fromisoformat(ct.replace("Z", "+00:00")).isoformat()
            except Exception:
                end_date = ct
            break

    # Derive date context from event_ticker
    date_ctx = ""
    if "DEC31" in event_ticker.upper():
        date_ctx = "Dec 31, 2026"
    elif "JUN2026" in event_ticker.upper() or "JUN29" in event_ticker.upper():
        date_ctx = "Jun 29, 2026"
    elif "JAN2027" in event_ticker.upper():
        date_ctx = "Year-End 2026"

    question = f"What range will {instrument} close in? ({headline_label})"
    outcome_summary = (
        f"{len(outcomes)}-bucket range · Priced: {headline_label} @ {yes_pct}%"
    )
    url = f"https://kalshi.com/events/{event_ticker}"

    return _base_row(
        family_key=family_key,
        event_ticker=event_ticker,
        series_ticker=series_ticker,
        market_ticker=mkt_ticker,
        question=question,
        display_subtitle=date_ctx,
        contract_context=date_ctx,
        priced_outcome="Yes",
        priced_outcome_label=headline_label,
        prob=prob,
        yes_pct=yes_pct,
        outcomes=outcomes,
        outcome_summary=outcome_summary,
        end_date=end_date,
        url=url,
        vol_24h=vol_24h,
        vol_total=vol_total,
        oi=oi,
        liq=liq,
        candidate_count=len(outcomes),
        most_likely_outcome_label=headline_label,
        most_likely_probability=prob,
    )


def _build_onetouch_row(
    family_key: str,
    series_ticker: str,
    event_ticker: str,
    markets_with_pricing: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Build a row for a one-touch / cumulative 'X or above' ladder.

    Headline: strike with probability nearest to 0.50 (most informative).
    If no strike is near 50%, use the one with highest volume.
    """
    priced = []
    for m in markets_with_pricing:
        if not m:
            continue
        prob = _kalshi_prob(m)
        if prob is None:
            continue
        # For one-touch, skip fully-resolved strikes (prob very near 0 or 1)
        if prob <= 0.005 or prob >= 0.995:
            continue
        sub  = (m.get("subtitle") or "").strip()
        vol  = float(m.get("volume_fp") or 0)
        v24  = float(m.get("volume_24h_fp") or 0)
        oi   = float(m.get("open_interest_fp") or 0)
        liq  = float(m.get("liquidity_dollars") or 0)
        strike = _extract_strike_value(m.get("ticker") or "")
        # Build label: "X,XXX or above"
        label = sub or f"{strike:,.0f} or above"
        priced.append({
            "market":   m,
            "prob":     prob,
            "pct":      round(prob * 100.0, 1),
            "label":    label,
            "strike":   strike,
            "vol":      vol,
            "vol_24h":  v24,
            "oi":       oi,
            "liq":      liq,
        })

    if not priced:
        # Fallback: include all priced markets, even near 0/1
        for m in markets_with_pricing:
            if not m:
                continue
            prob = _kalshi_prob(m)
            if prob is None:
                continue
            sub    = (m.get("subtitle") or "").strip()
            vol    = float(m.get("volume_fp") or 0)
            v24    = float(m.get("volume_24h_fp") or 0)
            oi     = float(m.get("open_interest_fp") or 0)
            liq    = float(m.get("liquidity_dollars") or 0)
            strike = _extract_strike_value(m.get("ticker") or "")
            label  = sub or f"{strike:,.0f} or above"
            priced.append({
                "market":   m, "prob":  prob, "pct": round(prob*100,1),
                "label":    label, "strike": strike,
                "vol":  vol, "vol_24h": v24, "oi": oi, "liq": liq,
            })

    if not priced:
        return None

    # Sort by strike ascending
    priced_sorted = sorted(priced, key=lambda x: x["strike"])

    outcomes = [
        {
            "label":         p["label"],
            "display_label": p["label"],
            "strike":        p["strike"],
            "probability":   p["prob"],
            "pct":           p["pct"],
            "volume_24h":    p["vol_24h"],
            "open_interest": p["oi"],
            "market_ticker": p["market"].get("ticker") or "",
        }
        for p in priced_sorted
    ]

    # Headline: nearest to 50%, break ties by volume
    best = min(priced_sorted, key=lambda x: (abs(x["prob"] - 0.50), -x["vol"]))

    prob      = best["prob"]
    yes_pct   = best["pct"]
    hl_label  = best["label"]
    mkt_ticker = best["market"].get("ticker") or ""

    vol_total = sum(p["vol"] for p in priced_sorted)
    vol_24h   = sum(p["vol_24h"] for p in priced_sorted)
    oi        = sum(p["oi"] for p in priced_sorted)
    liq       = sum(p["liq"] for p in priced_sorted)

    instrument = _INSTRUMENT_LABELS.get(family_key, "Index")

    end_date: Optional[str] = None
    for pm in priced_sorted:
        ct = pm["market"].get("close_time") or pm["market"].get("expiration_time") or ""
        if ct:
            try:
                end_date = datetime.fromisoformat(ct.replace("Z", "+00:00")).isoformat()
            except Exception:
                end_date = ct
            break

    date_ctx = ""
    if "JAN2027" in event_ticker.upper() or "2027" in event_ticker:
        date_ctx = "Year-End 2026"
    elif "JUN2026" in event_ticker.upper() or "JUN" in event_ticker.upper():
        date_ctx = "June 30, 2026"

    question = f"Will {instrument} reach {hl_label}?"
    outcome_summary = (
        f"{len(outcomes)}-strike ladder · Priced: {hl_label} @ {yes_pct}%"
    )
    url = f"https://kalshi.com/events/{event_ticker}"

    return _base_row(
        family_key=family_key,
        event_ticker=event_ticker,
        series_ticker=series_ticker,
        market_ticker=mkt_ticker,
        question=question,
        display_subtitle=date_ctx,
        contract_context=date_ctx,
        priced_outcome="Yes",
        priced_outcome_label=hl_label,
        prob=prob,
        yes_pct=yes_pct,
        outcomes=outcomes,
        outcome_summary=outcome_summary,
        end_date=end_date,
        url=url,
        vol_24h=vol_24h,
        vol_total=vol_total,
        oi=oi,
        liq=liq,
        candidate_count=len(outcomes),
        most_likely_outcome_label=hl_label,
        most_likely_probability=prob,
    )


def _build_matchup_row(
    family_key: str,
    series_ticker: str,
    event_ticker: str,
    markets_with_pricing: list[dict],
    now_dt: datetime,
) -> Optional[dict]:
    """
    Build a row for a two-sided matchup market (e.g. S&P 500 vs Gold).

    Both sides are independent binary markets (not mutually exclusive).
    Primary outcome is the S&P/equity side (INXTR ticker suffix).
    Probabilities are preserved as-is (not normalized to sum to 1).
    """
    priced: list[dict] = []
    for m in markets_with_pricing:
        if not m:
            continue
        prob = _kalshi_prob(m)
        sub  = (m.get("subtitle") or "").strip()
        ticker = m.get("ticker") or ""
        vol  = float(m.get("volume_fp") or 0)
        v24  = float(m.get("volume_24h_fp") or 0)
        oi   = float(m.get("open_interest_fp") or 0)
        liq  = float(m.get("liquidity_dollars") or 0)
        priced.append({
            "market":   m,
            "prob":     prob,
            "pct":      round((prob or 0) * 100.0, 1),
            "label":    sub or ticker.rsplit("-", 1)[-1],
            "ticker":   ticker,
            "is_spx":   "INXTR" in ticker.upper() or "SPX" in ticker.upper() or "INX" in ticker.upper(),
            "vol":      vol,
            "vol_24h":  v24,
            "oi":       oi,
            "liq":      liq,
        })

    if not priced:
        return None

    outcomes = [
        {
            "label":         p["label"],
            "display_label": p["label"],
            "probability":   p["prob"],
            "pct":           p["pct"],
            "volume_24h":    p["vol_24h"],
            "open_interest": p["oi"],
            "market_ticker": p["ticker"],
            "is_primary":    p["is_spx"],
        }
        for p in priced
    ]

    # Primary (SPX) side
    spx_sides = [p for p in priced if p["is_spx"]]
    primary   = spx_sides[0] if spx_sides else priced[0]

    prob    = primary["prob"]
    yes_pct = primary["pct"]
    hl_label = primary["label"]
    mkt_ticker = primary["ticker"]

    # Fallback if SPX side has no prob
    if prob is None:
        prob    = 0.5
        yes_pct = 50.0

    vol_total = sum(p["vol"] for p in priced)
    vol_24h   = sum(p["vol_24h"] for p in priced)
    oi        = sum(p["oi"] for p in priced)
    liq       = sum(p["liq"] for p in priced)

    end_date: Optional[str] = None
    for pm in priced:
        ct = pm["market"].get("close_time") or pm["market"].get("expiration_time") or ""
        if ct:
            try:
                end_date = datetime.fromisoformat(ct.replace("Z", "+00:00")).isoformat()
            except Exception:
                end_date = ct
            break

    non_spx = [p["label"] for p in priced if not p["is_spx"]]
    vs_label = non_spx[0] if non_spx else "Gold"

    date_ctx = "Year-End 2026"
    question = f"Will S&P 500 outperform {vs_label} in 2026?"
    outcome_summary = (
        f"S&P 500 {yes_pct}% · {vs_label} "
        f"{[p['pct'] for p in priced if not p['is_spx']][0] if any(not p['is_spx'] for p in priced) else '?'}%"
    )
    url = f"https://kalshi.com/events/{event_ticker}"

    # most_likely: whichever side the market currently favours
    _mol_label = hl_label if (prob is not None and prob >= 0.50) else vs_label
    _mol_prob  = prob if (prob is not None and prob >= 0.50) else (
        round(1.0 - prob, 4) if prob is not None else None
    )

    return _base_row(
        family_key=family_key,
        event_ticker=event_ticker,
        series_ticker=series_ticker,
        market_ticker=mkt_ticker,
        question=question,
        display_subtitle=date_ctx,
        contract_context=date_ctx,
        priced_outcome="S&P 500",
        priced_outcome_label=hl_label,
        prob=prob,
        yes_pct=yes_pct,
        outcomes=outcomes,
        outcome_summary=outcome_summary,
        end_date=end_date,
        url=url,
        vol_24h=vol_24h,
        vol_total=vol_total,
        oi=oi,
        liq=liq,
        candidate_count=len(outcomes),
        most_likely_outcome_label=_mol_label,
        most_likely_probability=_mol_prob,
    )


# ── Per-series scanner (markets path) ─────────────────────────────────────────

async def _scan_markets_series(
    series_ticker: str,
    family_key: str,
    row_type: str,
    now_dt: datetime,
) -> Optional[dict]:
    """
    Scan a single Kalshi series via the /markets?series_ticker=X path.

    Steps:
    1. List all open markets for the series (flat list, no pricing).
    2. Select the best event group (filter out dead/inactive events).
    3. Batch-fetch individual market details for pricing.
    4. Build the appropriate row type.
    """
    # Step 1: list markets
    raw_markets = await _fetch_markets_list(series_ticker)
    if not raw_markets:
        log.info("[kalshi_scanner] %s: no open markets from list endpoint", family_key)
        return None

    # Step 2: select best event group
    event_ticker, event_markets = _select_best_event_markets(raw_markets)
    if not event_markets:
        log.info("[kalshi_scanner] %s: no event markets after selection", family_key)
        return None

    # Limit to reasonable count (trim to most liquid/relevant end of list)
    tickers = [m.get("ticker") for m in event_markets if m.get("ticker")]
    tickers = tickers[:_MAX_MARKETS_PER_SERIES]

    # Step 3: batch-fetch individual market pricing
    markets_with_pricing = await _fetch_markets_with_pricing(tickers)

    # Step 4: build row
    if row_type == "range":
        row = _build_range_row(
            family_key, series_ticker, event_ticker,
            markets_with_pricing, now_dt,
        )
    elif row_type == "onetouch":
        row = _build_onetouch_row(
            family_key, series_ticker, event_ticker,
            markets_with_pricing, now_dt,
        )
    elif row_type == "matchup":
        row = _build_matchup_row(
            family_key, series_ticker, event_ticker,
            markets_with_pricing, now_dt,
        )
    else:
        row = None

    if row:
        log.info(
            "[kalshi_scanner] %s (%s): matched event=%s outcomes=%d prob=%.1f%% vol=%.0f",
            family_key, row_type, event_ticker,
            len(row.get("outcomes") or []),
            row.get("yes_pct") or 0,
            row.get("volume_total") or 0,
        )
    else:
        log.info(
            "[kalshi_scanner] %s (%s): no row built (tickers=%d, priced=%d)",
            family_key, row_type, len(tickers),
            sum(1 for m in markets_with_pricing if m and _kalshi_prob(m) is not None),
        )

    return row


# ── Auth diagnostic (non-blocking) ────────────────────────────────────────────

def _check_auth_env() -> tuple[bool, str]:
    """
    Return (credentials_present, error_type) without performing an actual auth
    attempt (no HTTP call — key validation is expensive and can 401).
    """
    key_id  = os.environ.get("KALSHI_API_KEY_ID", "")
    pem_key = os.environ.get("KALSHI_PRIVATE_KEY", "")

    if not key_id or not pem_key:
        return False, "credentials_missing"

    if len(key_id) < 30:
        return False, "key_id_malformed"
    if "BEGIN" not in pem_key and "END" not in pem_key and len(pem_key) < 200:
        return False, "private_key_malformed"

    return True, "credentials_present_unverified"


# ── Main scan ──────────────────────────────────────────────────────────────────

async def scan_kalshi() -> dict:
    """
    Fetch all Kalshi primary-family markets and return normalized rows.

    Returns a dict:
      {
        "spx_daily_direction":       {...row...},
        "nasdaq_daily_direction":    {...row...},
        "spx_dec31_milestone":       {...row...},
        "spx_tomorrow_close_ladder": {...row...},
        ...
        "_diagnostics": {...},
      }

    Always returns without raising — failures are captured in _diagnostics.
    """
    t0 = time.time()
    now_dt = datetime.now(timezone.utc)

    rows: dict[str, dict] = {}
    diag: dict[str, Any] = {
        "kalshi_public_api_ok":              False,
        "kalshi_auth_ok":                    False,
        "kalshi_auth_error_type":            "not_attempted",
        "kalshi_spx_daily_matched":          False,
        "kalshi_nasdaq_daily_matched":       False,
        "kalshi_spx_dec31_matched":          False,
        "kalshi_spx_tomorrow_matched":       False,
        "kalshi_spx_year_end_range_matched": False,
        "kalshi_spx_year_high_matched":      False,
        "kalshi_spx_month_end_matched":      False,
        "kalshi_spx_vs_gold_matched":        False,
        "kalshi_nasdaq_year_end_matched":    False,
        "kalshi_rows_returned":              0,
        "kalshi_scan_ms":                    None,
        "kalshi_error":                      None,
        "kalshi_series_missing_by_family":   [],
        "kalshi_finance_rows_seen":          0,
        "kalshi_finance_rows_matched":       0,
    }

    creds_ok, creds_status = _check_auth_env()
    diag["kalshi_auth_error_type"] = (
        creds_status if not creds_ok else "credentials_present_unverified"
    )

    try:
        # ── Path A: /events-based scan (KXINXDUD, KXNASDAQDUD, KXINXDIRY) ──

        events_series_list = list(_DAILY_SERIES.items()) + list(_YEAREND_SERIES.items())
        diag["kalshi_finance_rows_seen"] += len(events_series_list)

        event_list_tasks = [
            _fetch(f"/events?series_ticker={series}&status=open&limit=10")
            for series, _ in events_series_list
        ]
        event_list_results = await asyncio.gather(*event_list_tasks, return_exceptions=True)

        any_api_ok = any(
            not isinstance(r, Exception) for r in event_list_results
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

        detail_tasks: list = []
        detail_mapping: list = []

        for (series, family_key), result in zip(events_series_list, event_list_results):
            if isinstance(result, Exception):
                log.warning(
                    "[kalshi_scanner] events fetch failed for %s (%s): %s",
                    series, family_key, result,
                )
                diag["kalshi_series_missing_by_family"].append(family_key)
                continue

            events = result.get("events") or []
            if not events:
                log.info("[kalshi_scanner] no open events for series=%s", series)
                diag["kalshi_series_missing_by_family"].append(family_key)
                continue

            is_yearend = series in _YEAREND_SERIES
            chosen = _select_dec31_event(events) if is_yearend else _select_nearest_open_event(events)

            if chosen is None:
                diag["kalshi_series_missing_by_family"].append(family_key)
                continue

            event_ticker = chosen.get("event_ticker") or ""
            if not event_ticker:
                diag["kalshi_series_missing_by_family"].append(family_key)
                continue

            detail_tasks.append(_fetch(f"/events/{event_ticker}"))
            detail_mapping.append((series, family_key, is_yearend, chosen))

        if detail_tasks:
            detail_results = await asyncio.gather(*detail_tasks, return_exceptions=True)

            for (series, family_key, is_yearend, event_shell), detail_result in zip(
                detail_mapping, detail_results
            ):
                if isinstance(detail_result, Exception):
                    log.warning(
                        "[kalshi_scanner] event detail fetch failed for %s: %s",
                        family_key, detail_result,
                    )
                    diag["kalshi_series_missing_by_family"].append(family_key)
                    continue

                event_full = detail_result.get("event") or event_shell
                markets    = detail_result.get("markets") or []

                if is_yearend:
                    row = _build_dec31_row(family_key, event_full, markets, now_dt)
                    if row:
                        rows[family_key] = row
                        diag["kalshi_spx_dec31_matched"] = True
                        diag["kalshi_finance_rows_matched"] += 1
                        log.info(
                            "[kalshi_scanner] %s matched: %d-strike ladder",
                            family_key, len(row.get("outcomes") or []),
                        )
                    else:
                        diag["kalshi_series_missing_by_family"].append(family_key)
                else:
                    row = _build_daily_row(family_key, event_full, markets, now_dt)
                    if row:
                        rows[family_key] = row
                        diag["kalshi_finance_rows_matched"] += 1
                        if family_key == "spx_daily_direction":
                            diag["kalshi_spx_daily_matched"] = True
                        elif family_key == "nasdaq_daily_direction":
                            diag["kalshi_nasdaq_daily_matched"] = True
                        log.info(
                            "[kalshi_scanner] %s matched: prob=%.1f%% vol=%.0f",
                            family_key, (row.get("yes_pct") or 0),
                            (row.get("volume_total") or 0),
                        )
                    else:
                        diag["kalshi_series_missing_by_family"].append(family_key)

        # ── Path B: /markets-based scan (new series) ───────────────────────

        diag["kalshi_finance_rows_seen"] += len(_MARKETS_PATH_SERIES)

        markets_tasks = [
            _scan_markets_series(series, family_key, row_type, now_dt)
            for series, (family_key, row_type) in _MARKETS_PATH_SERIES.items()
        ]
        markets_results = await asyncio.gather(*markets_tasks, return_exceptions=True)

        _diag_flag_map = {
            "spx_tomorrow_close_ladder":  "kalshi_spx_tomorrow_matched",
            "spx_year_end_close_range":   "kalshi_spx_year_end_range_matched",
            "spx_year_high_ladder":       "kalshi_spx_year_high_matched",
            "spx_month_end_high_ladder":  "kalshi_spx_month_end_matched",
            "spx_vs_gold_annual_return":  "kalshi_spx_vs_gold_matched",
            "nasdaq_year_end_close_range": "kalshi_nasdaq_year_end_matched",
        }

        for (series, (family_key, row_type)), result in zip(
            _MARKETS_PATH_SERIES.items(), markets_results
        ):
            if isinstance(result, Exception):
                log.warning(
                    "[kalshi_scanner] markets-path scan failed for %s: %s",
                    family_key, result,
                )
                diag["kalshi_series_missing_by_family"].append(family_key)
                continue

            if result:
                rows[family_key] = result
                diag["kalshi_finance_rows_matched"] += 1
                flag = _diag_flag_map.get(family_key)
                if flag:
                    diag[flag] = True
            else:
                diag["kalshi_series_missing_by_family"].append(family_key)

        diag["kalshi_public_api_ok"] = True  # at least events worked
        diag["kalshi_rows_returned"] = len(rows)

    except Exception as exc:
        diag["kalshi_error"] = str(exc)
        log.warning("[kalshi_scanner] scan_kalshi unexpected error: %s", exc)

    diag["kalshi_scan_ms"] = round((time.time() - t0) * 1000)
    rows["_diagnostics"] = diag
    return rows
