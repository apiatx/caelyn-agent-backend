"""
Portfolio Review Service

Fast, signal-based portfolio context building + deterministic fallback.
Zero Finnhub calls. Zero EDGAR calls.

Data sources:
  - POST body holdings (cost basis, shares, weights)
  - Portfolio options cache  (Tradier prices where available)
  - Macro snapshot cache     (VIX, Fear/Greed, regime)
  - XAI Grok social ranking  (single batched call, 30-min cache)
  - Watchlist service        (swap candidates)
  - X consensus disk cache   (background Grok social scan)
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from datetime import datetime, timezone
from typing import Any, Optional


# ── JSON-safe helper ──────────────────────────────────────────────────────────

def _safe(v: Any, default=None) -> Any:
    if v is None:
        return default
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    try:
        from decimal import Decimal
        if isinstance(v, Decimal):
            return _safe(float(v), default)
    except ImportError:
        pass
    if isinstance(v, datetime):
        return v.isoformat()
    return v


def _holdings_sig(tickers: list[str]) -> str:
    key = ",".join(sorted(t.upper() for t in tickers))
    return hashlib.md5(key.encode()).hexdigest()[:12]


# ── XAI Portfolio Social Ranking ──────────────────────────────────────────────

_SOCIAL_CACHE: dict[str, tuple[float, dict]] = {}
_SOCIAL_TTL = 1800


async def get_portfolio_social_ranking(xai_provider, tickers: list[str]) -> dict:
    """
    One Grok call ranking ALL portfolio holdings by X/social sentiment.
    Uses xai_provider._call_grok_with_x_search (Responses API, correct format).
    Cached 30 minutes per unique holdings set. Falls back silently — never raises.
    """
    if not xai_provider or not tickers:
        return {"status": "unavailable", "ranked": [], "source": "none"}

    sig = _holdings_sig(tickers)
    now = time.time()
    if sig in _SOCIAL_CACHE:
        ts, cached = _SOCIAL_CACHE[sig]
        if now - ts < _SOCIAL_TTL:
            print("[PORTFOLIO_REVIEW] XAI social ranking: cache hit", flush=True)
            return cached

    ticker_list = " ".join(f"${t}" for t in tickers[:20])
    prompt = (
        f"Search X for recent market conversation about these portfolio holdings: {ticker_list}\n\n"
        f"Rank ALL {len(tickers)} by CURRENT social sentiment and momentum on X. "
        "Return ONLY valid JSON:\n\n"
        '{\n'
        '  "ranked": [\n'
        '    {"ticker":"SYMBOL","rank":1,"sentiment":"bullish|neutral|bearish|mixed",'
        '"momentum":"accelerating|stable|fading|unknown","crowding_risk":"low|medium|high|unknown",'
        '"reason":"1 sentence X narrative summary"}\n'
        '  ],\n'
        '  "portfolio_social_read": "1 sentence on overall portfolio social setup"\n'
        '}\n\n'
        "Include ALL tickers. Tickers with no meaningful X discussion: rank last, sentiment=neutral, "
        "momentum=unknown. Focus on social acceleration, crowding risk, sentiment vs price action."
    )

    try:
        raw = await xai_provider._call_grok_with_x_search(prompt, timeout=18.0, raw_mode=False)
        if isinstance(raw, dict) and raw.get("error"):
            raise ValueError(raw["error"])
        # _call_grok_with_x_search(raw_mode=False) returns already-parsed dict via _parse_json_response
        if isinstance(raw, dict):
            parsed = raw
        else:
            clean = str(raw).strip()
            if clean.startswith("```"):
                clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            start_idx = clean.find("{")
            end_idx   = clean.rfind("}") + 1
            if start_idx < 0 or end_idx <= start_idx:
                raise ValueError("No JSON object in Grok response")
            parsed = json.loads(clean[start_idx:end_idx])
        if not parsed.get("ranked"):
            raise ValueError("No ranked list in Grok response")
        result = {
            "status": "ok",
            "source": "xai_grok",
            "ranked": parsed.get("ranked", []),
            "portfolio_social_read": parsed.get("portfolio_social_read", ""),
            "as_of": datetime.now(timezone.utc).isoformat(),
        }
        _SOCIAL_CACHE[sig] = (now, result)
        print(f"[PORTFOLIO_REVIEW] XAI social ranking OK: {len(result['ranked'])} tickers ranked", flush=True)
        return result

    except Exception as e:
        print(f"[PORTFOLIO_REVIEW] XAI social ranking failed: {type(e).__name__}: {e}", flush=True)
        return {
            "status": "unavailable",
            "ranked": [],
            "source": "xai_grok",
            "as_of": datetime.now(timezone.utc).isoformat(),
        }


# ── Context Builder ───────────────────────────────────────────────────────────

def build_portfolio_context(
    holdings: list[dict],
    options_data: dict,
    macro_snapshot: dict,
    social_ranking: dict,
    watchlist_tickers: list[str],
    x_consensus: dict,
) -> dict:
    """
    Compact, LLM-ready context. No Finnhub, no EDGAR.
    Prices come from Tradier (via options scan) where available.
    """
    social_by_ticker: dict[str, dict] = {}
    for row in (social_ranking.get("ranked") or []):
        t = (row.get("ticker") or "").upper()
        if t:
            social_by_ticker[t] = row

    xc_by_ticker: dict[str, dict] = {}
    for row in (
        x_consensus.get("backend_ranked")
        or x_consensus.get("_backend_ranked")
        or []
    ):
        t = (row.get("ticker") or row.get("symbol") or "").upper()
        if t:
            xc_by_ticker[t] = row

    total_cost = 0.0
    for h in holdings:
        total_cost += float(h.get("shares", 0) or 0) * float(
            h.get("avg_cost", 0) or h.get("avgCost", 0) or 0
        )

    rows: list[dict] = []
    for h in holdings:
        ticker = (h.get("ticker") or "").upper().strip()
        if not ticker:
            continue
        shares = float(h.get("shares", 0) or 0)
        avg_cost = float(h.get("avg_cost", 0) or h.get("avgCost", 0) or 0)
        cost_basis = round(shares * avg_cost, 2)
        weight = round(cost_basis / total_cost * 100, 1) if total_cost else 0

        row: dict[str, Any] = {
            "ticker": ticker,
            "shares": _safe(shares),
            "avg_cost": _safe(avg_cost),
            "cost_basis": _safe(cost_basis),
            "weight_pct": _safe(weight),
            "asset_type": h.get("asset_type") or h.get("type") or "stock",
        }

        opt = (options_data or {}).get(ticker, {})
        if opt.get("data_available"):
            row["options"] = {
                "score":          _safe(opt.get("score")),
                "signal":         opt.get("signal"),
                "iv":             _safe(opt.get("iv")),
                "em_pct":         _safe(opt.get("em")),
                "put_call_ratio": _safe(opt.get("p_c")),
                "vol":            _safe(opt.get("vol")),
            }
            price = _safe(
                opt.get("price")
                or opt.get("underlying_price")
                or opt.get("stock_price")
            )
            if price and price > 0 and avg_cost > 0:
                mv = round(shares * price, 2)
                row["current_price"] = price
                row["market_value"]  = mv
                row["pnl"]           = round((price - avg_cost) * shares, 2)
                row["pnl_pct"]       = round((price - avg_cost) / avg_cost * 100, 1)

        soc = social_by_ticker.get(ticker)
        if soc:
            row["social"] = {
                "rank":         soc.get("rank"),
                "sentiment":    soc.get("sentiment"),
                "momentum":     soc.get("momentum"),
                "crowding_risk": soc.get("crowding_risk"),
                "reason":       (soc.get("reason") or "")[:150],
                "risk_note":    (soc.get("risk_note") or "")[:120],
            }

        xc = xc_by_ticker.get(ticker)
        if xc:
            row["x_consensus"] = {
                "raw_score": _safe(xc.get("raw_score") or xc.get("score")),
                "sentiment": xc.get("sentiment") or xc.get("label"),
            }

        rows.append(row)

    rows.sort(key=lambda r: r.get("weight_pct") or 0, reverse=True)

    total_mv = sum(
        (r.get("market_value") or r.get("cost_basis") or 0) for r in rows
    )
    total_pnl = round(total_mv - total_cost, 2)
    total_ret  = round(total_pnl / total_cost * 100, 1) if total_cost else 0
    weights    = [r.get("weight_pct") or 0 for r in rows]
    hhi        = round(sum(w**2 for w in weights), 1)
    max_weight = max(weights) if weights else 0

    macro_ctx: dict = {}
    if macro_snapshot and isinstance(macro_snapshot, dict):
        vix = macro_snapshot.get("vix")
        fg  = macro_snapshot.get("fear_greed")
        tr  = macro_snapshot.get("treasury_rates")
        macro_ctx = {
            "vix":              _safe(vix if not isinstance(vix, dict) else (vix.get("value") or vix.get("current"))),
            "fear_greed":       (fg.get("value") if isinstance(fg, dict) else _safe(fg)),
            "fear_greed_label": (fg.get("label") or fg.get("classification") if isinstance(fg, dict) else None),
            "treasury_10y":     _safe(tr.get("10y") if isinstance(tr, dict) else None),
            "regime":           macro_snapshot.get("regime"),
        }

    options_available = sum(1 for r in rows if r.get("options"))

    return {
        "portfolio": {
            "count":              len(rows),
            "total_cost_basis":   _safe(total_cost),
            "total_market_value": _safe(total_mv) if total_mv != total_cost else None,
            "total_pnl":          _safe(total_pnl) if total_mv != total_cost else None,
            "total_return_pct":   _safe(total_ret) if total_mv != total_cost else None,
            "hhi":                _safe(hhi),
            "max_weight_pct":     _safe(max_weight),
        },
        "holdings":              rows,
        "options_coverage":      f"{options_available}/{len(rows)}",
        "macro":                 macro_ctx,
        "social_portfolio_read": (social_ranking.get("portfolio_social_read") or "") if isinstance(social_ranking, dict) else "",
        "social_status":         (social_ranking.get("status") or "unavailable") if isinstance(social_ranking, dict) else "unavailable",
        "watchlist_candidates":  [t.upper() for t in (watchlist_tickers or [])[:15]],
    }


# ── Deterministic Fallback ────────────────────────────────────────────────────

def build_deterministic_review(context: dict) -> dict:
    """
    Metric-based review when LLM unavailable.
    Uses: weights, options signals, social sentiment, macro, PnL.
    """
    rows      = context.get("holdings", [])
    portfolio = context.get("portfolio", {})
    macro     = context.get("macro", {})

    hhi        = float(portfolio.get("hhi") or 0)
    max_weight = float(portfolio.get("max_weight_pct") or 0)
    total_ret  = float(portfolio.get("total_return_pct") or 0)
    count      = int(portfolio.get("count") or len(rows))

    if hhi > 3000 or max_weight > 50:
        risk_level = "aggressive"
    elif hhi > 2000 or max_weight > 35:
        risk_level = "high"
    elif hhi > 1200:
        risk_level = "moderate"
    else:
        risk_level = "low"

    strengths: list[str] = []
    if total_ret > 15:
        strengths.append(f"Portfolio up {total_ret:.1f}% vs cost basis")
    strong_opts = [r for r in rows if (r.get("options") or {}).get("score") and r["options"]["score"] > 60]
    if strong_opts:
        strengths.append(f"Strong options flow: {', '.join(r['ticker'] for r in strong_opts[:3])}")
    bullish_accel = [
        r for r in rows
        if (r.get("social") or {}).get("sentiment") == "bullish"
        and (r.get("social") or {}).get("momentum") == "accelerating"
    ]
    if bullish_accel:
        strengths.append(f"Accelerating X momentum: {', '.join(r['ticker'] for r in bullish_accel[:3])}")
    if not strengths:
        strengths = ["Holding pattern — no strong buy signals from available data"]

    risks: list[str] = []
    if hhi > 2000:
        risks.append(f"High concentration (HHI {hhi:.0f}) — portfolio is correlated")
    if max_weight > 30:
        top_r = sorted(rows, key=lambda r: r.get("weight_pct") or 0, reverse=True)
        if top_r:
            risks.append(f"{top_r[0]['ticker']} at {top_r[0].get('weight_pct',0):.1f}% — dominant single-name risk")
    bearish_fading = [
        r for r in rows
        if (r.get("social") or {}).get("sentiment") == "bearish"
    ]
    if bearish_fading:
        risks.append(f"Bearish social signal: {', '.join(r['ticker'] for r in bearish_fading[:2])}")
    crowded = [r for r in rows if (r.get("social") or {}).get("crowding_risk") == "high"]
    if crowded:
        risks.append(f"High crowding risk on X: {', '.join(r['ticker'] for r in crowded[:2])}")
    if not risks:
        risks = ["No critical risks flagged by available metrics"]

    weighting: list[dict] = []
    for r in rows:
        ticker   = r["ticker"]
        weight   = float(r.get("weight_pct") or 0)
        pnl_pct  = float(r.get("pnl_pct") or 0)
        opts     = r.get("options") or {}
        soc      = r.get("social") or {}
        opt_score = float(opts.get("score") or 0)
        soc_sent  = soc.get("sentiment", "")
        crowding  = soc.get("crowding_risk", "")

        if pnl_pct < -25 and (soc_sent in ("bearish", "") or not soc_sent):
            action, conf = "reduce_risk", "medium"
            reason = f"Down {abs(pnl_pct):.0f}% from cost with weak social signal — elevated risk"
        elif pnl_pct > 80 and weight > 15:
            action, conf = "trim_watch", "medium"
            reason = f"Up {pnl_pct:.0f}% with {weight:.0f}% weight — partial profit-take worth considering"
        elif opt_score > 60 and (opts.get("put_call_ratio") or 1.0) < 0.5:
            action, conf = "add_on_confirmation", "medium"
            reason = f"Options score {opt_score:.0f} with call-side bias — bullish setup on confirmation"
        elif crowding == "high":
            action, conf = "trim_watch", "low"
            reason = "High social crowding on X — watch for momentum reversal"
        elif weight > 25:
            action, conf = "monitor", "low"
            reason = f"Large {weight:.0f}% position — monitor closely before adding"
        else:
            action, conf = "keep_core", "low"
            reason = "No strong directional signal — maintain current position"

        weighting.append({
            "ticker":           ticker,
            "current_weight":   weight,
            "suggested_action": action,
            "confidence":       conf,
            "reason":           reason,
        })

    asset_reviews: list[dict] = []
    for r in rows:
        ticker = r["ticker"]
        opts   = r.get("options") or {}
        soc    = r.get("social") or {}
        pnl_pct = float(r.get("pnl_pct") or 0)

        bull: list[str] = []
        bear: list[str] = []

        if pnl_pct > 10:
            bull.append(f"Up {pnl_pct:.1f}% from cost basis")
        if (opts.get("score") or 0) > 60:
            bull.append(f"Strong options flow (score {opts.get('score'):.0f}, {opts.get('signal','?')})")
        if soc.get("sentiment") == "bullish":
            bull.append(f"Bullish X sentiment ({soc.get('momentum','?')} momentum)")
        if soc.get("reason"):
            bull.append(soc["reason"][:120])

        if pnl_pct < -15:
            bear.append(f"Down {abs(pnl_pct):.1f}% from cost")
        if soc.get("crowding_risk") == "high":
            bear.append("High crowding — prone to sharp reversal if narrative shifts")
        if soc.get("sentiment") == "bearish":
            bear.append("Bearish X sentiment")
        if soc.get("risk_note"):
            bear.append(soc["risk_note"][:120])

        opt_sig = opts.get("signal", "unavailable") if opts else "unavailable"
        soc_sig = (
            f"{soc.get('sentiment','?')}/{soc.get('momentum','?')}"
            if soc else "unavailable"
        )

        asset_reviews.append({
            "ticker":    ticker,
            "company":   ticker,
            "theme":     "unknown",
            "stage":     "unknown",
            "bull_case": "; ".join(bull) if bull else "Insufficient data for bull case",
            "bear_case": "; ".join(bear) if bear else "Insufficient data for bear case",
            "signals": {
                "price_momentum":  "unavailable",
                "relative_volume": "unavailable",
                "volume_marketcap": "unavailable",
                "options_flow":    opt_sig,
                "news":            "unavailable",
                "social_sentiment": soc_sig,
                "earnings":        "unavailable",
            },
            "view": f"Fallback read — options: {opt_sig}, social: {soc_sig}",
        })

    risk_flags: list[dict] = []
    if hhi > 3000:
        risk_flags.append({
            "severity": "critical",
            "title":    "Extreme Concentration",
            "details":  f"HHI {hhi:.0f} — dangerously concentrated. A single bad position can critically damage the portfolio.",
        })
    elif hhi > 2000:
        risk_flags.append({
            "severity": "warning",
            "title":    "High Concentration",
            "details":  f"HHI {hhi:.0f} — portfolio is heavily correlated. Spread risk across uncorrelated positions.",
        })
    for r in rows:
        if (r.get("pnl_pct") or 0) < -30 and (r.get("weight_pct") or 0) > 8:
            risk_flags.append({
                "severity": "warning",
                "title":    f"{r['ticker']} — Significant Loss at Material Weight",
                "details":  f"Down {abs(r.get('pnl_pct',0)):.0f}% from cost at {r.get('weight_pct',0):.1f}% weight.",
            })
    if crowded:
        risk_flags.append({
            "severity": "info",
            "title":    "Crowded Positions on X",
            "details":  f"{', '.join(r['ticker'] for r in crowded)} — high crowding risk means a narrative shift could accelerate selling.",
        })
    vix = macro.get("vix")
    if isinstance(vix, (int, float)) and vix > 25:
        risk_flags.append({
            "severity": "warning",
            "title":    f"Elevated VIX ({vix:.1f})",
            "details":  "Heightened volatility — consider tighter sizing and stop levels.",
        })

    macro_parts: list[str] = []
    if isinstance(vix, (int, float)):
        macro_parts.append(f"VIX {vix:.1f}")
    fg       = macro.get("fear_greed")
    fg_label = macro.get("fear_greed_label")
    if fg is not None:
        macro_parts.append(f"Fear/Greed {fg}" + (f" ({fg_label})" if fg_label else ""))
    regime = macro.get("regime")
    if regime:
        macro_parts.append(f"regime: {regime}")
    macro_read = " | ".join(macro_parts) if macro_parts else "Macro data unavailable"

    social_read = context.get("social_portfolio_read") or (
        (
            f"Top social: {rows[0]['ticker']} ({(rows[0].get('social') or {}).get('sentiment','?')})"
            if rows and rows[0].get("social") else "Social data unavailable"
        )
    )

    headline_parts = [f"{count} holdings"]
    if portfolio.get("total_return_pct") is not None:
        ret = float(portfolio["total_return_pct"])
        headline_parts.append(f"{'+'if ret>=0 else ''}{ret:.1f}% vs cost")
    headline_parts.append(f"{risk_level} risk")

    return {
        "ok":            True,
        "agent_status":  "fallback",
        "portfolio_summary": {
            "headline":       " | ".join(headline_parts),
            "risk_level":     risk_level,
            "top_strengths":  strengths[:3],
            "top_risks":      risks[:3],
            "theme_read":     "Theme analysis requires LLM synthesis",
            "macro_read":     macro_read,
            "social_read":    social_read,
        },
        "weighting_suggestions": weighting,
        "asset_reviews":         asset_reviews,
        "watchlist_swaps":       [],
        "risk_flags":            risk_flags,
        "raw_inputs_used": {
            "portfolio":       True,
            "watchlist":       bool(context.get("watchlist_candidates")),
            "options_flow":    sum(1 for r in rows if r.get("options")) > 0,
            "volume_marketcap": False,
            "relative_volume": False,
            "news":            False,
            "theme_stage":     False,
            "social_sentiment": context.get("social_status") == "ok",
            "earnings":        False,
            "macro_context":   bool(macro),
        },
        "unavailable_inputs":  ["theme_stage", "news", "relative_volume", "volume_marketcap", "earnings"],
        "agent_error_summary": None,
    }


# ── LLM Helpers ───────────────────────────────────────────────────────────────

def build_review_prompt(context_str: str, has_watchlist: bool, n_holdings: int = 1) -> str:
    watchlist_note = (
        "The context includes watchlist_candidates. Only suggest a watchlist swap when data "
        "clearly supports it (stronger theme, options flow, or social signal vs a current holding). "
        "Add swap objects to risk_flags with severity=info if relevant, or omit entirely."
        if has_watchlist
        else ""
    )

    return f"""You are CaelynAI's Portfolio Review Agent.

PORTFOLIO DATA:
{context_str}
{watchlist_note}

Return ONLY a valid JSON object — no markdown, no backticks, nothing outside the JSON:

{{
  "portfolio_summary": {{
    "headline": "1-line: holdings count + dominant theme + risk posture",
    "risk_level": "low|moderate|high|aggressive",
    "overview": "2-3 sentences on theme coherence, macro fit, and top risk"
  }},
  "holdings": [
    {{
      "ticker": "SYMBOL",
      "company": "Full company name",
      "theme": "sector/theme",
      "action": "keep_core|add_on_confirmation|trim_watch|reduce_risk|swap_candidate|monitor",
      "confidence": "low|medium|high",
      "view": "Bull: <thesis>. Bear: <risk>."
    }}
  ],
  "risk_flags": [
    {{
      "severity": "info|warning|critical",
      "title": "Short title",
      "details": "Specific detail"
    }}
  ]
}}

RULES (follow exactly):
- holdings array MUST contain all {n_holdings} positions — one object per ticker.
- view field: one sentence. Start with "Bull:" then "Bear:" — keep it tight.
- Reference actual numbers from context (weights, HHI, sentiment labels).
- risk_flags: only for real risks (HHI>2000, Stage 4, broken thesis, crowding).
- Return ONLY the JSON. No other text."""


def parse_claude_review(raw_text: str) -> Optional[dict]:
    """
    Parse Claude's JSON response. Accepts both schemas:
      - New: {portfolio_summary, holdings, watchlist_swaps, risk_flags}
      - Legacy: {portfolio_summary, weighting_suggestions, asset_reviews, ...}
    Always returns legacy shape with weighting_suggestions + asset_reviews.
    """
    try:
        clean = raw_text.strip()
        if clean.startswith("```"):
            clean = clean.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        start = clean.find("{")
        end   = clean.rfind("}") + 1
        if start < 0 or end <= start:
            return None
        parsed = json.loads(clean[start:end])

        # Accept new merged-holdings schema (ultra-compact: no reason/bull_case/bear_case)
        if "holdings" in parsed and "portfolio_summary" in parsed:
            holdings_list = parsed.get("holdings") or []
            ws = []
            ar = []
            for h in holdings_list:
                ticker  = h.get("ticker", "")
                view    = h.get("view", "")
                # Split "Bull: X. Bear: Y." into separate fields for legacy compat
                bull, bear = "", ""
                if "Bear:" in view:
                    parts = view.split("Bear:", 1)
                    bull  = parts[0].replace("Bull:", "").strip().rstrip(".")
                    bear  = parts[1].strip().rstrip(".")
                elif "Bull:" in view:
                    bull = view.replace("Bull:", "").strip()
                else:
                    bull = view
                ws.append({
                    "ticker":            ticker,
                    "current_weight":    h.get("weight", 0.0),
                    "suggested_action":  h.get("action", "monitor"),
                    "confidence":        h.get("confidence", "medium"),
                    "reason":            view,
                })
                ar.append({
                    "ticker":    ticker,
                    "company":   h.get("company", ticker),
                    "theme":     h.get("theme", ""),
                    "bull_case": bull,
                    "bear_case": bear,
                    "view":      view,
                })
            parsed["weighting_suggestions"] = ws
            parsed["asset_reviews"]         = ar

        required = {"portfolio_summary", "weighting_suggestions", "asset_reviews"}
        if not required.issubset(parsed.keys()):
            return None
        return parsed
    except Exception:
        return None


def flatten_review_to_text(review: dict) -> str:
    lines: list[str] = []
    ps = review.get("portfolio_summary") or {}
    if ps.get("headline"):
        lines.append(f"## {ps['headline']}")
    if ps.get("theme_read"):
        lines.append(f"\n{ps['theme_read']}")
    if ps.get("macro_read"):
        lines.append(f"\n**Macro:** {ps['macro_read']}")
    if ps.get("social_read"):
        lines.append(f"\n**Social:** {ps['social_read']}")
    ws = review.get("weighting_suggestions") or []
    if ws:
        lines.append("\n## Position Review")
        for w in ws:
            action = (w.get("suggested_action") or "monitor").replace("_", " ").upper()
            lines.append(f"\n**{w.get('ticker')}** — {action} ({w.get('confidence','?')} confidence)")
            if w.get("reason"):
                lines.append(f"_{w['reason']}_")
    flags = review.get("risk_flags") or []
    if flags:
        lines.append("\n## Risk Flags")
        for f in flags:
            sev = (f.get("severity") or "info").upper()
            lines.append(f"\n[{sev}] **{f.get('title','')}**: {f.get('details','')}")
    return "\n".join(lines)


def raw_inputs_used(
    context: dict,
    social_res: dict,
    opts_by_symbol: dict,
    has_macro: bool,
    has_watchlist: bool,
) -> dict:
    return {
        "portfolio":       True,
        "watchlist":       has_watchlist,
        "options_flow":    bool(opts_by_symbol),
        "volume_marketcap": False,
        "relative_volume": False,
        "news":            False,
        "theme_stage":     False,
        "social_sentiment": isinstance(social_res, dict) and social_res.get("status") == "ok",
        "earnings":        False,
        "macro_context":   has_macro,
    }
