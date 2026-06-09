"""
Alert Signal Bus
================
Centralized backend service that detects unusual ticker activity
from data already being fetched for Home, Watchlist, Portfolio,
Options Flow, and Hyperliquid pages.

Key rules:
  * Zero new recurring provider calls.
  * Source-aware scoring — no universal model.
  * Missing metrics are never treated as zero.
  * Options-only tickers can fire without VolX / Vol-MC.
  * 15-min cooldown per (user, ticker, alert_type).
  * Score >= 75 required to fire.
  * full_activity lane requires >= 2 independent signals.
"""

from __future__ import annotations

import asyncio
import json
import math
import statistics
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

# ─────────────────────────────────────────────────────────────────────────────
# In-memory state
# ─────────────────────────────────────────────────────────────────────────────

# SSE queues: user_id → list of Queue objects (one per open connection)
_alert_queues: Dict[str, List[asyncio.Queue]] = defaultdict(list)

# Cooldown: (user_id, ticker, alert_type) → last-fired unix ts
_cooldown_map: Dict[Tuple, float] = {}
_COOLDOWN_SECS = 15 * 60

# Per-(ticker, source) snapshot debounce — avoid flooding DB on frequent polls
_last_snapshot_ts: Dict[Tuple[str, str], float] = {}
_SNAPSHOT_DEBOUNCE_SECS = 5 * 60

# Recent signal windows for cross-confirmed detection
_recent_options_ts: Dict[str, float] = {}   # ticker → ts of last options signal
_recent_activity_ts: Dict[str, float] = {}  # ticker → ts of last activity signal
_CROSS_WINDOW_SECS = 15 * 60

# Diagnostics
_diag: Dict[str, Any] = {
    "snapshots_by_source": defaultdict(int),
    "alerts_by_lane": defaultdict(int),
    "suppressed": [],
    "last_alert_ts_by_key": {},
    "provider_calls": 0,
}
_MAX_SUPPRESSED = 200

# Severity escalation: tracks last-fired severity per cooldown key so a
# medium→high or high→critical signal can bypass the 15-min cooldown.
_cooldown_severity: Dict[Tuple, str] = {}
_SEVERITY_ORDER: Dict[str, int] = {"medium": 0, "high": 1, "critical": 2}

# Tracks last SSE alert emitted (for diagnostics)
_last_sse_emit: Dict[str, Any] = {"alert_id": None, "ts": None}


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers (psycopg2 sync → called via asyncio.to_thread)
# ─────────────────────────────────────────────────────────────────────────────

def _pg():
    try:
        from data.pg_storage import _get_conn, _put_conn
        conn = _get_conn()
        return conn, _put_conn
    except Exception:
        return None, None


def _write_snapshot_sync(
    user_id: str, ticker: str, source: str,
    metrics: dict, coverage: dict, raw: dict | None,
) -> int | None:
    conn, put = _pg()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.ticker_signal_snapshots
                (user_id, ticker, source,
                 price, price_change_pct, volume, rel_volume, volx,
                 market_cap, vol_marketcap,
                 options_score, options_rank, previous_options_rank, rank_delta,
                 call_put_bias, call_volume, put_volume, call_put_ratio,
                 iv, expected_move,
                 hyperliquid_trade_usd, hyperliquid_liq_usd,
                 hyperliquid_funding, hyperliquid_oi,
                 metric_coverage, raw)
            VALUES
                (%(user_id)s, %(ticker)s, %(source)s,
                 %(price)s, %(price_change_pct)s, %(volume)s, %(rel_volume)s, %(volx)s,
                 %(market_cap)s, %(vol_marketcap)s,
                 %(options_score)s, %(options_rank)s, %(previous_options_rank)s, %(rank_delta)s,
                 %(call_put_bias)s, %(call_volume)s, %(put_volume)s, %(call_put_ratio)s,
                 %(iv)s, %(expected_move)s,
                 %(hyperliquid_trade_usd)s, %(hyperliquid_liq_usd)s,
                 %(hyperliquid_funding)s, %(hyperliquid_oi)s,
                 %(metric_coverage)s::jsonb, %(raw)s::jsonb)
            RETURNING id
        """, {
            "user_id": user_id,
            "ticker": ticker,
            "source": source,
            "price":                   metrics.get("price"),
            "price_change_pct":        metrics.get("price_change_pct"),
            "volume":                  metrics.get("volume"),
            "rel_volume":              metrics.get("rel_volume"),
            "volx":                    metrics.get("volx"),
            "market_cap":              metrics.get("market_cap"),
            "vol_marketcap":           metrics.get("vol_marketcap"),
            "options_score":           metrics.get("options_score"),
            "options_rank":            metrics.get("options_rank"),
            "previous_options_rank":   metrics.get("previous_options_rank"),
            "rank_delta":              metrics.get("rank_delta"),
            "call_put_bias":           metrics.get("call_put_bias"),
            "call_volume":             metrics.get("call_volume"),
            "put_volume":              metrics.get("put_volume"),
            "call_put_ratio":          metrics.get("call_put_ratio"),
            "iv":                      metrics.get("iv"),
            "expected_move":           metrics.get("expected_move"),
            "hyperliquid_trade_usd":   metrics.get("hyperliquid_trade_usd"),
            "hyperliquid_liq_usd":     metrics.get("hyperliquid_liq_usd"),
            "hyperliquid_funding":     metrics.get("hyperliquid_funding"),
            "hyperliquid_oi":          metrics.get("hyperliquid_oi"),
            "metric_coverage": json.dumps(coverage, default=str),
            "raw":             json.dumps(raw or {}, default=str),
        })
        row = cur.fetchone()
        conn.commit()
        cur.close()
        return row[0] if row else None
    except Exception as exc:
        print(f"[ALERT_BUS] snapshot write error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None
    finally:
        put(conn)


def _fetch_recent_snapshots_sync(ticker: str, limit: int = 20) -> list[dict]:
    conn, put = _pg()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT price, price_change_pct, volume, rel_volume, volx,
                   market_cap, vol_marketcap, options_score, options_rank, ts
            FROM public.ticker_signal_snapshots
            WHERE ticker = %s
            ORDER BY ts DESC
            LIMIT %s
        """, (ticker, limit))
        cols = ["price", "price_change_pct", "volume", "rel_volume", "volx",
                "market_cap", "vol_marketcap", "options_score", "options_rank", "ts"]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    except Exception as exc:
        print(f"[ALERT_BUS] fetch snapshots error: {exc}")
        return []
    finally:
        put(conn)


def _write_alert_sync(record: dict) -> Tuple[Optional[int], Optional[str]]:
    """Returns (id, created_at_iso) from DB, or (None, None) on failure."""
    conn, put = _pg()
    if conn is None:
        return None, None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO public.ticker_alert_events
                (user_id, ticker, alert_type, alert_lane, severity,
                 title, short_label, coverage_label, summary, score,
                 reasons, source_metrics, source_tags)
            VALUES
                (%(user_id)s, %(ticker)s, %(alert_type)s, %(alert_lane)s, %(severity)s,
                 %(title)s, %(short_label)s, %(coverage_label)s, %(summary)s, %(score)s,
                 %(reasons)s::jsonb, %(source_metrics)s::jsonb, %(source_tags)s::jsonb)
            RETURNING id, created_at
        """, {
            "user_id":        record["user_id"],
            "ticker":         record["ticker"],
            "alert_type":     record["alert_type"],
            "alert_lane":     record["alert_lane"],
            "severity":       record["severity"],
            "title":          record["title"],
            "short_label":    record["short_label"],
            "coverage_label": record["coverage_label"],
            "summary":        record.get("summary", ""),
            "score":          record["score"],
            "reasons":        json.dumps(record.get("reasons", []), default=str),
            "source_metrics": json.dumps(record.get("source_metrics", {}), default=str),
            "source_tags":    json.dumps(record.get("source_tags", []), default=str),
        })
        row = cur.fetchone()
        conn.commit()
        cur.close()
        if row:
            alert_id   = row[0]
            created_at = row[1].isoformat() if row[1] else None
            return alert_id, created_at
        return None, None
    except Exception as exc:
        print(f"[ALERT_BUS] alert write error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return None, None
    finally:
        put(conn)


def _get_recent_alerts_sync(
    user_id: str,
    limit: int,
    since: Optional[str] = None,
    include_acknowledged: bool = True,
    include_dismissed: bool = False,
) -> list[dict]:
    conn, put = _pg()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        # Build WHERE dynamically — all conditions are either static or parameterized
        conditions = ["user_id = %s"]
        params: list = [user_id]

        if not include_dismissed:
            conditions.append("dismissed_at IS NULL")
        if not include_acknowledged:
            conditions.append("acknowledged_at IS NULL")
        if since:
            conditions.append("created_at > %s::timestamptz")
            params.append(since)

        where = " AND ".join(conditions)
        params.append(limit)

        cur.execute(f"""
            SELECT id, ticker, alert_type, alert_lane, severity, title, short_label,
                   coverage_label, summary, score, reasons, source_metrics, source_tags,
                   created_at, acknowledged_at, dismissed_at
            FROM public.ticker_alert_events
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT %s
        """, params)
        cols = [
            "id", "ticker", "alert_type", "alert_lane", "severity", "title", "short_label",
            "coverage_label", "summary", "score", "reasons", "source_metrics", "source_tags",
            "created_at", "acknowledged_at", "dismissed_at",
        ]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            # Boolean helpers — compute before isoformat conversion
            row["is_acknowledged"] = row["acknowledged_at"] is not None
            row["is_dismissed"]    = row["dismissed_at"] is not None
            for tf in ("created_at", "acknowledged_at", "dismissed_at"):
                if row[tf] is not None:
                    row[tf] = row[tf].isoformat()
            for jf in ("reasons", "source_metrics", "source_tags"):
                if isinstance(row[jf], str):
                    try:
                        row[jf] = json.loads(row[jf])
                    except Exception:
                        row[jf] = []
            rows.append(row)
        cur.close()
        return rows
    except Exception as exc:
        print(f"[ALERT_BUS] get_recent_alerts error: {exc}")
        return []
    finally:
        put(conn)


def _get_alert_by_id_sync(alert_id: int) -> dict | None:
    conn, put = _pg()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, ticker, alert_type, alert_lane, severity, title, short_label,
                   coverage_label, summary, score, reasons, source_metrics, source_tags,
                   created_at, acknowledged_at, dismissed_at
            FROM public.ticker_alert_events
            WHERE id = %s
        """, (alert_id,))
        cols = [
            "id", "user_id", "ticker", "alert_type", "alert_lane", "severity", "title",
            "short_label", "coverage_label", "summary", "score", "reasons",
            "source_metrics", "source_tags", "created_at", "acknowledged_at", "dismissed_at",
        ]
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        record = dict(zip(cols, row))
        record["is_acknowledged"] = record["acknowledged_at"] is not None
        record["is_dismissed"]    = record["dismissed_at"] is not None
        for tf in ("created_at", "acknowledged_at", "dismissed_at"):
            if record[tf] is not None:
                record[tf] = record[tf].isoformat()
        for jf in ("reasons", "source_metrics", "source_tags"):
            if isinstance(record[jf], str):
                try:
                    record[jf] = json.loads(record[jf])
                except Exception:
                    record[jf] = []
        return record
    except Exception as exc:
        print(f"[ALERT_BUS] get_alert_by_id error: {exc}")
        return None
    finally:
        put(conn)


def _get_alert_history_sync(
    user_id: str,
    days: int = 7,
    limit: int = 100,
    offset: int = 0,
    ticker: Optional[str] = None,
    alert_lane: Optional[str] = None,
    severity: Optional[str] = None,
    include_acknowledged: bool = True,
    include_dismissed: bool = True,
) -> list[dict]:
    """
    Read-only history query from ticker_alert_events.
    No provider calls.  Returns only list-view fields (no chart/news payload).
    """
    conn, put = _pg()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        days  = max(1, min(days, 30))
        limit = max(1, min(limit, 500))
        offset = max(0, offset)

        conditions = ["user_id = %s", "created_at >= NOW() - (%s || ' days')::interval"]
        params: list = [user_id, str(days)]

        if not include_acknowledged:
            conditions.append("acknowledged_at IS NULL")
        if not include_dismissed:
            conditions.append("dismissed_at IS NULL")
        if ticker:
            conditions.append("ticker = %s")
            params.append(ticker.upper().strip())
        if alert_lane:
            conditions.append("alert_lane = %s")
            params.append(alert_lane.strip())
        if severity:
            conditions.append("severity = %s")
            params.append(severity.strip().lower())

        where = " AND ".join(conditions)
        params += [limit, offset]

        cur.execute(f"""
            SELECT id, ticker, alert_type, alert_lane, severity, title, short_label,
                   coverage_label, summary, score, source_tags,
                   created_at, acknowledged_at, dismissed_at
            FROM public.ticker_alert_events
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, params)

        cols = [
            "id", "ticker", "alert_type", "alert_lane", "severity", "title", "short_label",
            "coverage_label", "summary", "score", "source_tags",
            "created_at", "acknowledged_at", "dismissed_at",
        ]
        rows = []
        for r in cur.fetchall():
            row = dict(zip(cols, r))
            row["is_acknowledged"] = row["acknowledged_at"] is not None
            row["is_dismissed"]    = row["dismissed_at"] is not None
            for tf in ("created_at", "acknowledged_at", "dismissed_at"):
                if row[tf] is not None:
                    row[tf] = row[tf].isoformat()
            if isinstance(row["source_tags"], str):
                try:
                    row["source_tags"] = json.loads(row["source_tags"])
                except Exception:
                    row["source_tags"] = []
            rows.append(row)
        cur.close()
        return rows
    except Exception as exc:
        print(f"[ALERT_BUS] get_alert_history error: {exc}")
        return []
    finally:
        put(conn)


def _get_history_counts_sync(user_id: str) -> dict:
    """Return history_count_7d, history_count_24h, dismissed_count_7d, acknowledged_count_7d."""
    conn, put = _pg()
    if conn is None:
        return {}
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*)                                                         AS total_7d,
                COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 day')  AS total_24h,
                COUNT(*) FILTER (WHERE dismissed_at IS NOT NULL)                 AS dismissed_7d,
                COUNT(*) FILTER (WHERE acknowledged_at IS NOT NULL)              AS acknowledged_7d
            FROM public.ticker_alert_events
            WHERE user_id = %s
              AND created_at >= NOW() - INTERVAL '7 days'
        """, (user_id,))
        row = cur.fetchone()
        cur.close()
        if row:
            return {
                "history_count_7d":      int(row[0]),
                "history_count_24h":     int(row[1]),
                "dismissed_count_7d":    int(row[2]),
                "acknowledged_count_7d": int(row[3]),
            }
        return {}
    except Exception as exc:
        print(f"[ALERT_BUS] get_history_counts error: {exc}")
        return {}
    finally:
        put(conn)


def _update_alert_field_sync(alert_id: int, user_id: str, field: str) -> bool:
    """Generic SET {field} = NOW() for ack/dismiss."""
    conn, put = _pg()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            f"UPDATE public.ticker_alert_events SET {field} = NOW() "
            "WHERE id = %s AND user_id = %s",
            (alert_id, user_id),
        )
        count = cur.rowcount
        conn.commit()
        cur.close()
        return count > 0
    except Exception as exc:
        print(f"[ALERT_BUS] update_alert_field error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        put(conn)


def _cleanup_retention_sync() -> dict:
    conn, put = _pg()
    if conn is None:
        return {"ok": False, "error": "no_db"}
    try:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM public.ticker_signal_snapshots WHERE ts < NOW() - INTERVAL '7 days'"
        )
        snap_deleted = cur.rowcount
        cur.execute(
            "DELETE FROM public.ticker_alert_events WHERE created_at < NOW() - INTERVAL '90 days'"
        )
        alert_deleted = cur.rowcount
        conn.commit()
        cur.close()
        return {"ok": True, "snapshots_deleted": snap_deleted, "alerts_deleted": alert_deleted}
    except Exception as exc:
        print(f"[ALERT_BUS] cleanup error: {exc}")
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "error": str(exc)}
    finally:
        put(conn)


# ─────────────────────────────────────────────────────────────────────────────
# Metric coverage
# ─────────────────────────────────────────────────────────────────────────────

def _metric_coverage(metrics: dict) -> dict:
    def has(k):
        v = metrics.get(k)
        return v is not None and v != ""

    return {
        "price":         has("price") or has("price_change_pct"),
        "volume":        has("volume"),
        "rel_volume":    has("rel_volume"),
        "volx":          has("volx"),
        "vol_marketcap": has("vol_marketcap"),
        "options":       has("options_score") or has("call_put_bias"),
        "hyperliquid":   has("hyperliquid_trade_usd") or has("hyperliquid_oi"),
        "news":          False,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Scoring helpers
# ─────────────────────────────────────────────────────────────────────────────

def _safe(v, default=None):
    """Cast to float; return default on None / NaN / Inf."""
    try:
        if v is None:
            return default
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return default


def _zscore(value: float, history: list) -> float | None:
    valid = [float(x) for x in history if x is not None]
    if len(valid) < 3:
        return None
    try:
        mu = statistics.mean(valid)
        sd = statistics.stdev(valid)
        return None if sd < 1e-9 else (value - mu) / sd
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# options_first lane
# ─────────────────────────────────────────────────────────────────────────────

def _score_options_first(
    ticker: str, metrics: dict, history: list[dict]
) -> Tuple[float, List[dict]]:
    """
    Score based only on available options/quote fields.
    Base: composite_score (0-100) × 0.85 → up to 85 pts.
    Boosters add up to ~55 pts (capped at 100).
    Does NOT penalise missing VolX / rel-vol / Vol-MC.
    """
    base = _safe(metrics.get("options_score"))
    if base is None:
        return 0.0, []

    score = base * 0.85
    reasons: List[dict] = [
        {"label": "Options Score", "value": str(round(base, 1)), "why": "composite options signal strength"}
    ]

    # Rank boost
    rank = _safe(metrics.get("options_rank"))
    if rank is not None:
        if rank <= 3:
            score += 12
            reasons.append({"label": "Rank", "value": f"#{int(rank)}", "why": "top-3 options ticker"})
        elif rank <= 5:
            score += 8
            reasons.append({"label": "Rank", "value": f"#{int(rank)}", "why": "top-5 options ticker"})
        elif rank <= 10:
            score += 4
            reasons.append({"label": "Rank", "value": f"#{int(rank)}", "why": "top-10 options ticker"})

    # Rank improvement vs prior snapshot
    rank_delta = _safe(metrics.get("rank_delta"))
    if rank_delta is not None and rank_delta >= 2:
        boost = min(10, int(rank_delta) * 2)
        score += boost
        reasons.append({"label": "Rank Δ", "value": f"+{int(rank_delta)}", "why": "rising fast in options list"})

    # Directional bias
    bias = (metrics.get("call_put_bias") or "").lower()
    if bias in ("bullish", "bearish"):
        score += 5
        label = "Calls Hot" if bias == "bullish" else "Puts Hot"
        reasons.append({"label": "Bias", "value": label, "why": "directional options commitment"})

    # Call/put ratio
    cpr = _safe(metrics.get("call_put_ratio"))
    if cpr is not None:
        if cpr >= 5:
            score += 8
            reasons.append({"label": "C/P Ratio", "value": f"{cpr:.1f}×", "why": "extreme call skew"})
        elif cpr >= 3:
            score += 5
            reasons.append({"label": "C/P Ratio", "value": f"{cpr:.1f}×", "why": "heavy call bias"})
        elif 0 < cpr <= 0.25:
            score += 5
            reasons.append({"label": "C/P Ratio", "value": f"{cpr:.2f}×", "why": "heavy put bias"})

    # Implied volatility
    iv = _safe(metrics.get("iv"))
    if iv is not None and iv > 0.5:
        score += 5
        reasons.append({"label": "IV", "value": f"{iv * 100:.0f}%", "why": "elevated implied volatility"})

    # Expected move
    em = _safe(metrics.get("expected_move"))
    if em is not None and em > 0.05:
        score += 5
        reasons.append({"label": "Exp Move", "value": f"{em * 100:.1f}%", "why": "large expected price swing"})

    # Repeated appearance (seen in options scan in last 20 min)
    now = time.time()
    last_opt = _recent_options_ts.get(ticker)
    if last_opt is not None and (now - last_opt) < 20 * 60:
        score += 10
        reasons.append({"label": "Repeat", "value": "Yes", "why": "appeared in multiple recent options scans"})

    # Historical score jump vs prior snapshots
    if history:
        prior_scores = [_safe(s.get("options_score")) for s in history if _safe(s.get("options_score")) is not None]
        if len(prior_scores) >= 2:
            z = _zscore(base, prior_scores)
            if z is not None and z >= 1.5:
                score += 6
                reasons.append({"label": "Score Jump", "value": f"z={z:.1f}", "why": "unusually high vs recent history"})

    return min(100.0, round(score, 1)), reasons


# ─────────────────────────────────────────────────────────────────────────────
# full_activity lane
# ─────────────────────────────────────────────────────────────────────────────

def _score_full_activity(
    ticker: str, metrics: dict, history: list[dict]
) -> Tuple[float, List[dict], int]:
    """
    Points-based scoring — only available metrics contribute.
    Returns (score, reasons, signal_count).
    Requires >= 2 independent signals AND score >= 75 to fire.
    """
    score = 0.0
    reasons: List[dict] = []
    signal_count = 0

    # ── Price change ──────────────────────────────────────────────────────────
    chg = _safe(metrics.get("price_change_pct"))
    if chg is not None:
        abs_chg = abs(chg)
        direction = "+" if chg >= 0 else ""
        if abs_chg >= 8:
            score += 40
            signal_count += 1
            reasons.append({"label": "Price", "value": f"{direction}{chg:.1f}%", "why": "extreme intraday move"})
        elif abs_chg >= 5:
            score += 30
            signal_count += 1
            reasons.append({"label": "Price", "value": f"{direction}{chg:.1f}%", "why": "large intraday move"})
        elif abs_chg >= 3:
            score += 20
            signal_count += 1
            reasons.append({"label": "Price", "value": f"{direction}{chg:.1f}%", "why": "notable intraday move"})

    # ── VolX ──────────────────────────────────────────────────────────────────
    volx = _safe(metrics.get("volx"))
    if volx is not None:
        if volx >= 10:
            score += 45
            signal_count += 1
            reasons.append({"label": "VolX", "value": f"{volx:.1f}×", "why": "extreme volume surge"})
        elif volx >= 5:
            score += 35
            signal_count += 1
            reasons.append({"label": "VolX", "value": f"{volx:.1f}×", "why": "strong volume surge"})
        elif volx >= 2:
            score += 20
            signal_count += 1
            reasons.append({"label": "VolX", "value": f"{volx:.1f}×", "why": "above-average volume"})

    # ── Vol / Market-Cap ──────────────────────────────────────────────────────
    vol_mc = _safe(metrics.get("vol_marketcap"))
    if vol_mc is not None:
        pct = vol_mc * 100
        if pct >= 15:
            score += 30
            signal_count += 1
            reasons.append({"label": "Vol/MC", "value": f"{pct:.1f}%", "why": "extreme dollar flow vs market cap"})
        elif pct >= 10:
            score += 25
            signal_count += 1
            reasons.append({"label": "Vol/MC", "value": f"{pct:.1f}%", "why": "very high dollar flow"})
        elif pct >= 5:
            score += 15
            signal_count += 1
            reasons.append({"label": "Vol/MC", "value": f"{pct:.1f}%", "why": "elevated dollar flow"})

    # ── Relative volume vs history ────────────────────────────────────────────
    rel_vol = _safe(metrics.get("rel_volume"))
    if rel_vol is not None:
        if history:
            hist_rv = [_safe(s.get("rel_volume")) for s in history]
            z = _zscore(rel_vol, hist_rv)
            if z is not None:
                if z >= 3:
                    score += 30
                    signal_count += 1
                    reasons.append({"label": "Rel Vol", "value": f"{rel_vol:.2f}× (z={z:.1f})", "why": "3σ jump vs recent baseline"})
                elif z >= 2:
                    score += 20
                    signal_count += 1
                    reasons.append({"label": "Rel Vol", "value": f"{rel_vol:.2f}× (z={z:.1f})", "why": "2σ jump vs recent baseline"})
                # below 2σ — no points, not unusual enough
            elif rel_vol >= 3:
                # No history yet — use absolute threshold as fallback
                score += 20
                signal_count += 1
                reasons.append({"label": "Rel Vol", "value": f"{rel_vol:.2f}×", "why": "high relative volume (no history baseline)"})
        elif rel_vol >= 3:
            score += 20
            signal_count += 1
            reasons.append({"label": "Rel Vol", "value": f"{rel_vol:.2f}×", "why": "high relative volume"})

    # ── Options confirmation ──────────────────────────────────────────────────
    now = time.time()
    last_opt = _recent_options_ts.get(ticker)
    if last_opt is not None and (now - last_opt) < 30 * 60:
        score += 15
        reasons.append({"label": "Options", "value": "Confirmed", "why": "also appeared in recent options scan"})

    # ── Microcap / illiquidity guardrail ──────────────────────────────────────
    mc = _safe(metrics.get("market_cap"))
    vol = _safe(metrics.get("volume"))
    price = _safe(metrics.get("price"))
    if mc is not None and mc > 0 and vol is not None and price is not None:
        dollar_vol = vol * price
        if mc < 50_000_000 and dollar_vol < 500_000:
            score = round(score * 0.70, 1)
            reasons.append({"label": "Guardrail", "value": "micro-cap", "why": "tiny float + low dollar volume reduces confidence"})

    return min(100.0, round(score, 1)), reasons, signal_count


# ─────────────────────────────────────────────────────────────────────────────
# Hyperliquid lane
# ─────────────────────────────────────────────────────────────────────────────

def _score_hyperliquid(
    ticker: str, metrics: dict, history: list[dict]
) -> Tuple[float, List[dict]]:
    score = 0.0
    reasons: List[dict] = []

    trade_usd = _safe(metrics.get("hyperliquid_trade_usd"))
    liq_usd   = _safe(metrics.get("hyperliquid_liq_usd"))
    funding   = _safe(metrics.get("hyperliquid_funding"))
    oi        = _safe(metrics.get("hyperliquid_oi"))

    if trade_usd is not None:
        if trade_usd >= 20_000_000:
            score += 40
            reasons.append({"label": "Perp Volume", "value": f"${trade_usd / 1e6:.1f}M", "why": "very high perp dollar volume"})
        elif trade_usd >= 5_000_000:
            score += 25
            reasons.append({"label": "Perp Volume", "value": f"${trade_usd / 1e6:.1f}M", "why": "elevated perp dollar volume"})

    if liq_usd is not None:
        if liq_usd >= 5_000_000:
            score += 45
            reasons.append({"label": "Liquidation", "value": f"${liq_usd / 1e6:.1f}M", "why": "large liquidation burst"})
        elif liq_usd >= 1_000_000:
            score += 30
            reasons.append({"label": "Liquidation", "value": f"${liq_usd / 1e6:.1f}M", "why": "notable liquidation wave"})

    if funding is not None and abs(funding) > 0.001:
        score += 20
        reasons.append({"label": "Funding", "value": f"{funding * 100:.3f}%/hr", "why": "extreme funding rate"})

    if oi is not None and history:
        hist_oi = [_safe(s.get("hyperliquid_oi")) for s in history]
        z = _zscore(oi, hist_oi)
        if z is not None and z >= 2:
            score += 20
            reasons.append({"label": "OI", "value": f"z={z:.1f}", "why": "OI spike vs recent baseline"})

    return min(100.0, round(score, 1)), reasons


# ─────────────────────────────────────────────────────────────────────────────
# Alert record building
# ─────────────────────────────────────────────────────────────────────────────

_COVERAGE_LABELS = {
    "options_first":   "Options-only signal",
    "full_activity":   "Full activity signal",
    "cross_confirmed": "Cross-confirmed signal",
    "hyperliquid":     "Hyperliquid signal",
}

_SEVERITY_THRESHOLDS = {
    "options_first":   [(90, "critical"), (80, "high"), (75, "medium")],
    "full_activity":   [(90, "critical"), (80, "high"), (75, "medium")],
    "cross_confirmed": [(85, "critical"), (75, "high"),  (0,  "medium")],
    "hyperliquid":     [(90, "critical"), (80, "high"), (75, "medium")],
}


def _severity(lane: str, score: float) -> str:
    for threshold, label in _SEVERITY_THRESHOLDS.get(lane, [(75, "medium")]):
        if score >= threshold:
            return label
    return "medium"


def _alert_type_and_label(lane: str, reasons: list[dict]) -> Tuple[str, str]:
    values = {r.get("value", "") for r in reasons}
    labels = {r.get("label", "") for r in reasons}
    if lane == "options_first":
        if "Puts Hot" in values:
            return "puts_hot", "Puts Hot"
        if "Calls Hot" in values:
            return "calls_hot", "Calls Hot"
        return "options_spike", "Options Spike"
    elif lane == "full_activity":
        if "VolX" in labels and "Price" in labels:
            return "vol_surge", "Vol Surge"
        if "VolX" in labels:
            return "volx_spike", "VolX Spike"
        if "Price" in labels:
            return "price_spike", "Price Spike"
        if "Vol/MC" in labels:
            return "flow_surge", "Flow Surge"
        return "unusual_flow", "Unusual Flow"
    elif lane == "cross_confirmed":
        return "cross_confirmed", "Cross Confirmed"
    elif lane == "hyperliquid":
        if "Liquidation" in labels:
            return "liq_wave", "Liq Wave"
        return "perp_surge", "Perp Surge"
    return "unusual_flow", "Unusual Flow"


def _build_summary(ticker: str, lane: str, reasons: list[dict]) -> str:
    parts = [
        f"{r['label']} {r['value']}"
        for r in reasons[:4]
        if r.get("value") and r.get("label") not in ("Guardrail",)
    ]
    snippet = ", ".join(parts) if parts else "unusual activity detected"
    if lane == "cross_confirmed":
        return f"{ticker}: Options activity confirmed by price/volume — {snippet}."
    return f"{ticker}: {snippet}."


def _build_alert_record(
    user_id: str, ticker: str, lane: str, score: float,
    reasons: list[dict], source_tags: list[str], metrics: dict,
) -> dict:
    alert_type, short_label = _alert_type_and_label(lane, reasons)
    sev   = _severity(lane, score)
    cov   = _COVERAGE_LABELS.get(lane, "Signal")
    title = f"{ticker} {short_label}"
    return {
        "user_id":        user_id,
        "ticker":         ticker,
        "alert_type":     alert_type,
        "alert_lane":     lane,
        "severity":       sev,
        "title":          title,
        "short_label":    short_label,
        "coverage_label": cov,
        "summary":        _build_summary(ticker, lane, reasons),
        "score":          score,
        "reasons":        reasons,
        "source_metrics": {k: v for k, v in metrics.items() if v is not None},
        "source_tags":    source_tags,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Cooldown helpers
# ─────────────────────────────────────────────────────────────────────────────

def _in_cooldown(user_id: str, ticker: str, alert_type: str) -> bool:
    key = (user_id, ticker, alert_type)
    last = _cooldown_map.get(key)
    return last is not None and (time.time() - last) < _COOLDOWN_SECS


def _set_cooldown(user_id: str, ticker: str, alert_type: str, severity: str = "medium"):
    key = (user_id, ticker, alert_type)
    _cooldown_map[key]     = time.time()
    _cooldown_severity[key] = severity


def _log_suppressed(
    ticker: str, lane: str, score: float, reason: str,
    alert_type: str = "",
    cooldown_remaining: float = 0.0,
    last_alert_id: Optional[int] = None,
):
    entry = {
        "ticker":                ticker,
        "lane":                  lane,
        "alert_type":            alert_type,
        "score":                 score,
        "reason":                reason,
        "cooldown_remaining_secs": round(cooldown_remaining, 0),
        "last_alert_id":         last_alert_id,
        "ts":                    time.time(),
    }
    _diag["suppressed"].append(entry)
    if len(_diag["suppressed"]) > _MAX_SUPPRESSED:
        _diag["suppressed"].pop(0)


# ─────────────────────────────────────────────────────────────────────────────
# Lane routing
# ─────────────────────────────────────────────────────────────────────────────

_OPTIONS_SOURCES  = {"options_flow", "home_unusual_options", "portfolio_options"}
_ACTIVITY_SOURCES = {"watchlist", "portfolio", "portfolio_relvol"}
_HL_SOURCES       = {"hyperliquid"}


def _determine_lane(source: str, metrics: dict) -> str:
    if source in _HL_SOURCES:
        return "hyperliquid"
    if source in _OPTIONS_SOURCES:
        return "options_first"
    if source in _ACTIVITY_SOURCES:
        return "full_activity"
    # Heuristic for unlabelled sources
    if metrics.get("options_score") is not None:
        return "options_first"
    return "full_activity"


# ─────────────────────────────────────────────────────────────────────────────
# SSE helpers
# ─────────────────────────────────────────────────────────────────────────────

async def subscribe_alerts(user_id: str) -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue(maxsize=100)
    _alert_queues[user_id].append(q)
    return q


async def unsubscribe_alerts(user_id: str, q: asyncio.Queue):
    try:
        _alert_queues[user_id].remove(q)
    except ValueError:
        pass


async def _push_to_queues(user_id: str, event: dict):
    """Push to user-specific queues AND to any open "default" listeners."""
    targets = list(_alert_queues.get(user_id, []))
    if user_id != "default":
        targets += list(_alert_queues.get("default", []))
    for q in targets:
        try:
            q.put_nowait(event)
        except asyncio.QueueFull:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# Main public API
# ─────────────────────────────────────────────────────────────────────────────

async def record_signal_snapshot(
    source: str,
    user_id: str,
    ticker: str,
    metrics: dict,
    raw: dict | None = None,
) -> None:
    """
    Ingest a normalised signal snapshot for a ticker.

    Flow:
      1. Debounce — skip if this (ticker, source) was seen < 5 min ago.
      2. Build metric_coverage dict.
      3. Write snapshot to DB + fetch recent history (in one thread call).
      4. Update in-memory lane trackers.
      5. Score — lane-specific, null-safe.
      6. Check cross-confirmed window → possibly upgrade lane.
      7. Cooldown check — skip if already fired within 15 min.
      8. If score >= 75 (and signal_count >= 2 for full_activity): fire alert.
      9. Persist alert record, push to SSE queues.

    Zero provider calls — only reuses data passed in via `metrics`.
    """
    ticker = ticker.upper().strip()
    if not ticker:
        return

    # ── Debounce ────────────────────────────────────────────────────────────
    debounce_key = (ticker, source)
    now = time.time()
    if now - _last_snapshot_ts.get(debounce_key, 0) < _SNAPSHOT_DEBOUNCE_SECS:
        return
    _last_snapshot_ts[debounce_key] = now

    _diag["snapshots_by_source"][source] += 1

    coverage = _metric_coverage(metrics)
    lane     = _determine_lane(source, metrics)

    # ── DB: write snapshot + fetch history ──────────────────────────────────
    def _db_block():
        sid = _write_snapshot_sync(user_id, ticker, source, metrics, coverage, raw)
        hist = _fetch_recent_snapshots_sync(ticker, limit=20)
        return sid, hist

    _, history = await asyncio.to_thread(_db_block)

    # ── Update in-memory lane trackers ───────────────────────────────────────
    if lane == "options_first":
        _recent_options_ts[ticker] = now
    elif lane in ("full_activity", "hyperliquid"):
        _recent_activity_ts[ticker] = now

    # ── Score ────────────────────────────────────────────────────────────────
    signal_count = 1
    if lane == "options_first":
        score, reasons = _score_options_first(ticker, metrics, history)
        source_tags = [_source_label(source)]
    elif lane == "full_activity":
        score, reasons, signal_count = _score_full_activity(ticker, metrics, history)
        source_tags = [_source_label(source)]
        if signal_count < 2:
            _log_suppressed(ticker, lane, score, f"only {signal_count} signal (need ≥2)")
            return
    elif lane == "hyperliquid":
        score, reasons = _score_hyperliquid(ticker, metrics, history)
        source_tags = ["Hyperliquid"]
    else:
        return

    # ── Cross-confirmed upgrade ──────────────────────────────────────────────
    in_opts = ticker in _recent_options_ts and (now - _recent_options_ts[ticker]) < _CROSS_WINDOW_SECS
    in_act  = ticker in _recent_activity_ts and (now - _recent_activity_ts[ticker]) < _CROSS_WINDOW_SECS
    if in_opts and in_act:
        lane   = "cross_confirmed"
        score  = min(100.0, score + 20)
        source_tags = list({
            _source_label(source),
            "Options Flow" if in_opts else "",
            "Watchlist" if in_act else "",
        } - {""})
        reasons.append({
            "label": "Cross", "value": "Confirmed",
            "why": "options flow + price/volume activity aligned",
        })

    # ── Score threshold ──────────────────────────────────────────────────────
    if score < 75:
        _log_suppressed(ticker, lane, score, f"score {score:.1f} < 75")
        return

    # ── Alert type → cooldown key ────────────────────────────────────────────
    alert_type, _ = _alert_type_and_label(lane, reasons)
    new_severity   = _severity(lane, score)

    if _in_cooldown(user_id, ticker, alert_type):
        ck = (user_id, ticker, alert_type)
        last_sev = _cooldown_severity.get(ck, "medium")
        if _SEVERITY_ORDER.get(new_severity, 0) > _SEVERITY_ORDER.get(last_sev, 0):
            # Severity escalated (e.g. medium → high/critical) — bypass cooldown
            print(
                f"[ALERT_BUS] Severity escalation bypass: {ticker} "
                f"{last_sev}→{new_severity} (score={score})"
            )
        else:
            last_ts   = _cooldown_map.get(ck, 0)
            remaining = max(0.0, _COOLDOWN_SECS - (now - last_ts))
            _log_suppressed(
                ticker, lane, score,
                f"cooldown active ({remaining:.0f}s remaining)",
                alert_type=alert_type,
                cooldown_remaining=remaining,
            )
            return

    # ── Build, persist, push ─────────────────────────────────────────────────
    record = _build_alert_record(user_id, ticker, lane, score, reasons, source_tags, metrics)

    alert_id, created_at_iso = await asyncio.to_thread(_write_alert_sync, record)
    if alert_id:
        record["id"]              = alert_id
        record["created_at"]      = created_at_iso
        record["acknowledged_at"] = None
        record["dismissed_at"]    = None
        record["is_acknowledged"] = False
        record["is_dismissed"]    = False
        _set_cooldown(user_id, ticker, alert_type, severity=new_severity)
        _diag["alerts_by_lane"][lane] += 1
        _diag["last_alert_ts_by_key"][f"{ticker}:{alert_type}"] = now
        _last_sse_emit["alert_id"] = alert_id
        _last_sse_emit["ts"]       = now
        await _push_to_queues(user_id, record)
        print(
            f"[ALERT_BUS] FIRED ticker={ticker} lane={lane} "
            f"score={score} severity={new_severity} type={alert_type} id={alert_id}"
        )


def _source_label(source: str) -> str:
    _map = {
        "options_flow":           "Options Flow",
        "home_unusual_options":   "Home Unusual Options",
        "portfolio_options":      "Portfolio Options",
        "watchlist":              "Watchlist",
        "portfolio":              "Portfolio",
        "portfolio_relvol":       "Portfolio",
        "hyperliquid":            "Hyperliquid",
    }
    return _map.get(source, source.replace("_", " ").title())


# ─────────────────────────────────────────────────────────────────────────────
# Async public helpers (called from endpoints)
# ─────────────────────────────────────────────────────────────────────────────

async def get_recent_alerts(
    user_id: str,
    limit: int = 25,
    since: Optional[str] = None,
    include_acknowledged: bool = True,
    include_dismissed: bool = False,
) -> list[dict]:
    return await asyncio.to_thread(
        _get_recent_alerts_sync,
        user_id, limit, since, include_acknowledged, include_dismissed,
    )


async def get_alert_by_id(alert_id: int) -> dict | None:
    return await asyncio.to_thread(_get_alert_by_id_sync, alert_id)


async def ack_alert(alert_id: int, user_id: str) -> bool:
    return await asyncio.to_thread(_update_alert_field_sync, alert_id, user_id, "acknowledged_at")


async def dismiss_alert(alert_id: int, user_id: str) -> bool:
    return await asyncio.to_thread(_update_alert_field_sync, alert_id, user_id, "dismissed_at")


async def get_alert_history(
    user_id: str,
    days: int = 7,
    limit: int = 100,
    offset: int = 0,
    ticker: Optional[str] = None,
    alert_lane: Optional[str] = None,
    severity: Optional[str] = None,
    include_acknowledged: bool = True,
    include_dismissed: bool = True,
) -> list[dict]:
    return await asyncio.to_thread(
        _get_alert_history_sync,
        user_id, days, limit, offset,
        ticker, alert_lane, severity,
        include_acknowledged, include_dismissed,
    )


async def get_history_counts(user_id: str) -> dict:
    return await asyncio.to_thread(_get_history_counts_sync, user_id)


async def run_retention_cleanup() -> dict:
    return await asyncio.to_thread(_cleanup_retention_sync)


def get_diagnostics() -> dict:
    from datetime import datetime, timezone as _tz
    now = time.time()

    # Prune stale cooldowns from memory
    stale_keys = [k for k, ts in list(_cooldown_map.items()) if now - ts > _COOLDOWN_SECS]
    for k in stale_keys:
        _cooldown_map.pop(k, None)
        _cooldown_severity.pop(k, None)

    # Prune stale cross-window entries
    for d in (_recent_options_ts, _recent_activity_ts):
        stale = [k for k, ts in list(d.items()) if now - ts > _CROSS_WINDOW_SECS * 2]
        for k in stale:
            d.pop(k, None)

    one_hour_ago = now - 3600
    suppressed_last_hour = sum(
        1 for s in _diag["suppressed"] if s.get("ts", 0) >= one_hour_ago
    )

    active_cooldown_details = []
    for (uid, ticker, atype), ts in list(_cooldown_map.items()):
        remaining = _COOLDOWN_SECS - (now - ts)
        if remaining > 0:
            active_cooldown_details.append({
                "ticker":                  ticker,
                "alert_type":              atype,
                "cooldown_remaining_secs": round(remaining),
                "last_severity":           _cooldown_severity.get((uid, ticker, atype), "unknown"),
            })
    active_cooldown_details.sort(key=lambda x: x["cooldown_remaining_secs"], reverse=True)

    last_emit = _last_sse_emit
    last_emit_ts = last_emit.get("ts")
    return {
        "snapshots_by_source":      dict(_diag["snapshots_by_source"]),
        "alerts_by_lane":           dict(_diag["alerts_by_lane"]),
        "suppressed_candidates":    _diag["suppressed"][-20:],
        "suppressed_last_hour":     suppressed_last_hour,
        "last_alert_ts_by_key":     dict(_diag["last_alert_ts_by_key"]),
        "provider_calls":           _diag["provider_calls"],
        "active_cooldowns":         len(_cooldown_map),
        "active_cooldown_details":  active_cooldown_details[:25],
        "last_sse_emit": {
            "alert_id": last_emit.get("alert_id"),
            "ts": (
                datetime.fromtimestamp(last_emit_ts, tz=_tz.utc).isoformat(timespec="seconds")
                if last_emit_ts else None
            ),
        },
        "recent_options_tickers":   len(_recent_options_ts),
        "recent_activity_tickers":  len(_recent_activity_ts),
        "sse_subscribers":          {uid: len(qs) for uid, qs in _alert_queues.items() if qs},
        "note": "provider_calls must always be 0 — alert generation reuses cached data only",
    }
