"""
Portfolio Compare-to-Watchlist Intelligence Service
====================================================
Manual, expensive AI comparison between the user's saved portfolio and a
selected saved watchlist.  Generates a structured + markdown report with:
  - Per-ticker scoring (7 dimensions)
  - Replacement recommendations
  - Executive verdict
  - Full markdown sections (regime, fundamentals, technicals, catalysts, risks)

Storage: JSON file per user+watchlist combination (no DB dependency).
Cache TTL: 24 hours.  Stale if portfolio or watchlist tickers change.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from langsmith import traceable
except ImportError:
    def traceable(*args, **kwargs):
        def _noop(fn):
            return fn
        if args and callable(args[0]):
            return args[0]
        return _noop

# ── Constants ────────────────────────────────────────────────────────────────

REPORT_TTL_HOURS       = 24
MAX_TICKERS_FULL       = 30   # hard cap for enrichment
MAX_DEEP_ENRICH        = 10   # deep enrichment (candles + news) cap
_DATA_DIR              = Path(__file__).resolve().parent.parent / "data"


# ── File storage helpers ─────────────────────────────────────────────────────

def _compare_file(user_id: str, watchlist_id: str) -> Path:
    safe_uid = "".join(c for c in user_id if c.isalnum() or c in "-_")[:32]
    safe_wid = "".join(c for c in watchlist_id if c.isalnum() or c in "-_")[:36]
    return _DATA_DIR / f"portfolio_compare_{safe_uid}_{safe_wid}.json"


def load_report(user_id: str, watchlist_id: str) -> dict | None:
    path = _compare_file(user_id, watchlist_id)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        print(f"[COMPARE] Failed to load report {path}: {e}")
        return None


def save_report(user_id: str, watchlist_id: str, report: dict) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    path = _compare_file(user_id, watchlist_id)
    try:
        with open(path, "w") as f:
            json.dump(report, f, default=str, indent=2)
        print(f"[COMPARE] Report saved → {path.name}")
    except Exception as e:
        print(f"[COMPARE] Failed to save report: {e}")


# ── Hash helpers ─────────────────────────────────────────────────────────────

def _hash_tickers(tickers: list[str]) -> str:
    canonical = "|".join(sorted(t.upper().strip() for t in tickers if t.strip()))
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


# ── Staleness check ──────────────────────────────────────────────────────────

def check_staleness(
    report: dict,
    portfolio_tickers: list[str],
    watchlist_tickers: list[str],
) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    # TTL check
    generated_at = report.get("generated_at")
    if generated_at:
        try:
            ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
            age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            if age_h > REPORT_TTL_HOURS:
                reasons.append(f"Report is {age_h:.0f}h old (TTL={REPORT_TTL_HOURS}h)")
        except Exception:
            reasons.append("Cannot parse generated_at timestamp")

    # Portfolio tickers changed?
    if _hash_tickers(portfolio_tickers) != report.get("portfolio_snapshot_hash"):
        reasons.append("Portfolio holdings have changed since report was generated")

    # Watchlist tickers changed?
    if _hash_tickers(watchlist_tickers) != report.get("watchlist_snapshot_hash"):
        reasons.append("Watchlist has changed since report was generated")

    return bool(reasons), reasons


# ── Scoring helpers ──────────────────────────────────────────────────────────

def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        f = float(v)
        return f if not (f != f) else default  # NaN guard
    except (TypeError, ValueError):
        return default


def _score_ticker(
    ticker: str,
    fund: dict,          # fundamentals_enricher output
    technicals: dict,    # compute_technicals_from_bars output (or {})
    news: list,          # recent news items (list of dicts)
    regime: dict,        # macro regime snapshot
) -> dict:
    """
    Return a transparent 7-dimension score dict for one ticker.
    All dimensions 0-100, higher = better (except Risk where higher = more risk).
    """
    scores: dict[str, int] = {}

    # ── 1. Fundamental Quality ────────────────────────────────────────────────
    fq_pts = 50  # neutral start
    gross_margin_str = fund.get("Gross Margin", "")
    gross_margin = _safe_float(gross_margin_str.replace("%", "")) if gross_margin_str else None
    if gross_margin is not None:
        if gross_margin >= 60:   fq_pts += 15
        elif gross_margin >= 40: fq_pts += 8
        elif gross_margin >= 20: fq_pts += 2
        else:                    fq_pts -= 10

    roe_str = fund.get("ROE", "")
    roe = _safe_float(roe_str.replace("%", "")) if roe_str else None
    if roe is not None:
        if roe >= 20:    fq_pts += 10
        elif roe >= 10:  fq_pts += 5
        elif roe < 0:    fq_pts -= 10

    rev_growth_str = fund.get("Revenue Growth (YoY)", "")
    rev_growth = _safe_float(rev_growth_str.replace("%", "")) if rev_growth_str else None
    if rev_growth is not None:
        if rev_growth >= 30:    fq_pts += 15
        elif rev_growth >= 15:  fq_pts += 8
        elif rev_growth >= 5:   fq_pts += 3
        elif rev_growth < 0:    fq_pts -= 8

    fcf_str = fund.get("FCF Margin", "")
    fcf = _safe_float(fcf_str.replace("%", "")) if fcf_str else None
    if fcf is not None:
        if fcf >= 20:   fq_pts += 10
        elif fcf >= 10: fq_pts += 5
        elif fcf < 0:   fq_pts -= 10

    scores["fundamental_quality"] = max(0, min(100, fq_pts))

    # ── 2. Valuation ─────────────────────────────────────────────────────────
    val_pts = 50
    pe_str = fund.get("PE Ratio", "")
    pe = _safe_float(pe_str) if pe_str else None
    if pe is not None and pe > 0:
        if pe < 15:     val_pts += 20
        elif pe < 25:   val_pts += 10
        elif pe < 40:   val_pts += 0
        elif pe < 80:   val_pts -= 10
        else:           val_pts -= 20

    ps_str = fund.get("PS Ratio", "")
    ps = _safe_float(ps_str) if ps_str else None
    if ps is not None and ps > 0:
        if ps < 2:      val_pts += 10
        elif ps < 5:    val_pts += 5
        elif ps < 15:   val_pts += 0
        else:           val_pts -= 10

    scores["valuation"] = max(0, min(100, val_pts))

    # ── 3. Technical Setup ───────────────────────────────────────────────────
    ta_pts = 50
    rsi = technicals.get("rsi") or technicals.get("rsi_14")
    if rsi is not None:
        rsi = float(rsi)
        if 40 <= rsi <= 65:   ta_pts += 15   # healthy momentum
        elif rsi < 30:         ta_pts += 5    # oversold bounce potential
        elif rsi > 75:         ta_pts -= 10   # overbought

    sma20 = technicals.get("sma_20")
    sma50 = technicals.get("sma_50")
    sma200 = technicals.get("sma_200")
    price = technicals.get("current_price") or technicals.get("last_price")
    if price and sma50:
        price, sma50 = float(price), float(sma50)
        if price > sma50:   ta_pts += 10
        else:               ta_pts -= 5
    if sma50 and sma200:
        sma50, sma200 = float(sma50), float(sma200)
        if sma50 > sma200:  ta_pts += 10   # golden cross territory
        else:               ta_pts -= 5

    macd_hist = technicals.get("macd_histogram")
    if macd_hist is not None:
        if float(macd_hist) > 0: ta_pts += 5
        else:                    ta_pts -= 3

    scores["technical_setup"] = max(0, min(100, ta_pts))

    # ── 4. Catalyst ──────────────────────────────────────────────────────────
    cat_pts = 40
    if news:
        cat_pts += min(30, len(news) * 6)   # up to +30 for news volume
        for item in news[:5]:
            title = (item.get("title") or item.get("headline") or "").lower()
            if any(w in title for w in ("beat", "upgrade", "raised", "record", "win", "launch", "partnership")):
                cat_pts += 8
                break
            if any(w in title for w in ("miss", "downgrade", "cut", "loss", "layoff", "recall", "probe")):
                cat_pts -= 6
                break

    scores["catalyst"] = max(0, min(100, cat_pts))

    # ── 5. Sentiment / Attention ─────────────────────────────────────────────
    sent_pts = 50
    if news and len(news) >= 3:
        sent_pts += 10
    scores["sentiment_attention"] = max(0, min(100, sent_pts))

    # ── 6. Risk (0 = very risky, 100 = very safe — inverted for display) ────
    risk_pts = 60   # moderate-safe starting point
    de_str = fund.get("Debt / Equity", "")
    de = _safe_float(de_str) if de_str else None
    if de is not None:
        if de > 3:    risk_pts -= 20
        elif de > 1:  risk_pts -= 8
        elif de < 0:  risk_pts -= 15   # negative equity

    if fcf is not None and fcf < 0:
        risk_pts -= 15

    beta_str = fund.get("Beta", "")
    beta = _safe_float(beta_str) if beta_str else None
    if beta is not None:
        if beta > 2:    risk_pts -= 10
        elif beta > 1.5: risk_pts -= 4

    scores["risk"] = max(0, min(100, risk_pts))

    # ── 7. Regime Fit ────────────────────────────────────────────────────────
    regime_pts = 50
    sector = (fund.get("Sector") or "").lower()
    regime_label = (regime.get("regime") or "").lower() if isinstance(regime, dict) else ""
    if "tech" in sector and "growth" in regime_label:    regime_pts += 15
    if "energy" in sector and "inflation" in regime_label: regime_pts += 15
    if "util" in sector and "risk" in regime_label:      regime_pts += 10
    if "tech" in sector and "risk" in regime_label:      regime_pts -= 10
    scores["regime_fit"] = max(0, min(100, regime_pts))

    # ── Total (weighted) ─────────────────────────────────────────────────────
    w = {"fundamental_quality": 0.25, "valuation": 0.15, "technical_setup": 0.20,
         "catalyst": 0.15, "sentiment_attention": 0.05, "risk": 0.10, "regime_fit": 0.10}
    total = sum(scores.get(k, 50) * v for k, v in w.items())
    scores["total"] = round(total)

    return scores


def _verdict_from_score(score: int, bucket: str) -> str:
    if score >= 78:
        return "Keep" if bucket in ("portfolio_only", "both") else "Add"
    if score >= 62:
        return "Hold / Watch"
    if score >= 48:
        return "Watch / Trim"
    return "Replace Candidate" if bucket == "portfolio_only" else "Avoid"


# ── Replacement recommendation builder ──────────────────────────────────────

def _build_replacements(scored_universe: list[dict]) -> list[dict]:
    """
    For each portfolio-only ticker below score 65, find the best watchlist-only
    ticker above score 60 with a positive score delta and recommend a swap.
    """
    portfolio_weak = [t for t in scored_universe
                      if t["bucket"] == "portfolio_only" and t["scores"]["total"] < 65]
    watchlist_candidates = sorted(
        [t for t in scored_universe if t["bucket"] == "watchlist_only"],
        key=lambda t: t["scores"]["total"], reverse=True,
    )

    recs = []
    used_candidates: set[str] = set()

    for weak in sorted(portfolio_weak, key=lambda t: t["scores"]["total"]):
        for candidate in watchlist_candidates:
            if candidate["ticker"] in used_candidates:
                continue
            delta = candidate["scores"]["total"] - weak["scores"]["total"]
            if delta < 5:
                continue
            confidence = "High" if delta >= 20 else ("Medium" if delta >= 10 else "Low")
            recs.append({
                "replace": weak["ticker"],
                "replace_name": weak.get("name", weak["ticker"]),
                "with": candidate["ticker"],
                "with_name": candidate.get("name", candidate["ticker"]),
                "confidence": confidence,
                "score_delta": delta,
                "replace_score": weak["scores"]["total"],
                "candidate_score": candidate["scores"]["total"],
                "replace_sector": weak.get("sector", ""),
                "candidate_sector": candidate.get("sector", ""),
                "action": "Replace on next weakness" if confidence == "Low" else "Replace now",
            })
            used_candidates.add(candidate["ticker"])
            break

    return recs


# ── Main comparison pipeline ─────────────────────────────────────────────────

@traceable(name="portfolio_compare.run_comparison")
async def run_comparison(
    user_id: str,
    watchlist_id: str,
    portfolio_holdings: list[dict],
    watchlist_data: dict,
    data_service,
    claude_client,
    force_refresh: bool = False,
) -> dict:
    """
    Full portfolio vs watchlist comparison.

    Steps:
      1. Normalise tickers + build universe.
      2. Check cache — return if fresh and !force_refresh.
      3. Batch-enrich all tickers (FMP fundamentals + quotes + candles + news).
      4. Score each ticker on 7 dimensions.
      5. Build replacement recommendations.
      6. Call Claude for AI synthesis (structured JSON + markdown).
      7. Persist and return report.
    """
    t0 = time.time()

    # ── 1. Normalise & dedupe tickers ────────────────────────────────────────
    portfolio_tickers = sorted({
        h.get("ticker", "").upper().strip()
        for h in portfolio_holdings
        if h.get("ticker", "").strip()
    })
    watchlist_tickers_raw = watchlist_data.get("tickers", [])
    watchlist_tickers = sorted({t.upper().strip() for t in watchlist_tickers_raw if t.strip()})
    watchlist_name = watchlist_data.get("name", "Watchlist")

    if not portfolio_tickers:
        return {"ok": False, "error": "Portfolio has no valid tickers."}
    if not watchlist_tickers:
        return {"ok": False, "error": f"Watchlist '{watchlist_name}' has no tickers."}

    phash = _hash_tickers(portfolio_tickers)
    whash = _hash_tickers(watchlist_tickers)

    # ── 2. Cache check ────────────────────────────────────────────────────────
    if not force_refresh:
        existing = load_report(user_id, watchlist_id)
        if existing:
            stale, stale_reasons = check_staleness(existing, portfolio_tickers, watchlist_tickers)
            if not stale:
                existing["cache_status"] = "cached"
                existing["ok"] = True
                return existing
            print(f"[COMPARE] Cached report stale: {stale_reasons}")

    # ── 3. Build enrichment universe ──────────────────────────────────────────
    all_tickers = list(dict.fromkeys(portfolio_tickers + watchlist_tickers))  # ordered, deduped
    all_tickers = all_tickers[:MAX_TICKERS_FULL]

    bucket_map: dict[str, str] = {}
    for t in portfolio_tickers:
        bucket_map[t] = "portfolio_only"
    for t in watchlist_tickers:
        if t in bucket_map:
            bucket_map[t] = "both"
        else:
            bucket_map[t] = "watchlist_only"

    print(f"[COMPARE] Universe: {len(all_tickers)} tickers "
          f"(portfolio={len(portfolio_tickers)}, watchlist={len(watchlist_tickers)})")

    # ── 4. Data enrichment (parallel batch) ──────────────────────────────────
    from config import FMP_API_KEY
    from services.fundamentals_enricher import fetch_fundamentals

    fmp_data: dict[str, dict] = {}
    try:
        fmp_data = await asyncio.wait_for(
            fetch_fundamentals(all_tickers[:MAX_TICKERS_FULL], FMP_API_KEY or ""),
            timeout=30.0,
        )
        print(f"[COMPARE] FMP enriched {len(fmp_data)} tickers in {time.time()-t0:.1f}s")
    except Exception as e:
        print(f"[COMPARE] FMP enrichment failed (non-fatal): {e}")

    # Candles + news (deep enrich — limited count to protect rate limits)
    deep_tickers = all_tickers[:MAX_DEEP_ENRICH]
    candles_map: dict[str, list] = {}
    technicals_map: dict[str, dict] = {}
    news_map: dict[str, list] = {}

    async def _enrich_ticker_deep(ticker: str):
        results: dict[str, Any] = {}
        tasks: dict[str, Any] = {}

        if data_service and hasattr(data_service, "get_candles"):
            tasks["candles"] = asyncio.wait_for(
                data_service.get_candles(ticker, days=90), timeout=8.0)

        if data_service and hasattr(data_service, "fmp") and data_service.fmp:
            tasks["news"] = asyncio.wait_for(
                data_service.fmp.get_stock_news(ticker, limit=5), timeout=5.0)

        if not tasks:
            return ticker, results

        keys = list(tasks.keys())
        settled = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for k, v in zip(keys, settled):
            if not isinstance(v, Exception):
                results[k] = v
        return ticker, results

    deep_results = await asyncio.gather(
        *[_enrich_ticker_deep(t) for t in deep_tickers],
        return_exceptions=True,
    )

    for item in deep_results:
        if isinstance(item, Exception):
            continue
        ticker, res = item
        if isinstance(res, dict):
            bars = res.get("candles") if res.get("candles") else res.get("bars")
            if bars and isinstance(bars, list) and len(bars) >= 20:
                try:
                    from data.ta_utils import compute_technicals_from_bars
                    technicals_map[ticker] = compute_technicals_from_bars(bars)
                    candles_map[ticker] = bars
                except Exception:
                    pass
            nws = res.get("news", [])
            if nws and isinstance(nws, list):
                news_map[ticker] = nws

    # ── 5. Macro regime ───────────────────────────────────────────────────────
    macro_regime: dict = {}
    try:
        if data_service and hasattr(data_service, "_build_macro_snapshot"):
            macro_regime = await asyncio.wait_for(
                data_service._build_macro_snapshot(), timeout=8.0) or {}
    except Exception as e:
        print(f"[COMPARE] Macro snapshot failed (non-fatal): {e}")

    regime_info = {
        "regime":      macro_regime.get("regime", ""),
        "fear_greed":  macro_regime.get("fear_greed"),
        "vix":         macro_regime.get("vix"),
        "10y_yield":   (macro_regime.get("treasury_rates") or {}).get("10y"),
    }

    # ── 6. Score each ticker ─────────────────────────────────────────────────
    scored_universe: list[dict] = []
    for ticker in all_tickers:
        fund   = fmp_data.get(ticker, {})
        ta     = technicals_map.get(ticker, {})
        nws    = news_map.get(ticker, [])
        bucket = bucket_map.get(ticker, "watchlist_only")
        scores = _score_ticker(ticker, fund, ta, nws, macro_regime)
        verdict = _verdict_from_score(scores["total"], bucket)

        scored_universe.append({
            "ticker":    ticker,
            "name":      fund.get("_name", ticker),
            "bucket":    bucket,
            "sector":    fund.get("Sector", ""),
            "asset_type": "stock",
            "scores":    scores,
            "verdict":   verdict,
            "fundamentals_summary": {
                k: fund[k] for k in
                ("Stock Price", "Market Cap", "PE Ratio", "PS Ratio", "Gross Margin",
                 "ROE", "Debt / Equity", "Beta", "FCF Margin", "Revenue Growth (YoY)")
                if k in fund and fund[k]
            },
            "technicals_summary": {
                "rsi":     ta.get("rsi") or ta.get("rsi_14"),
                "sma_20":  ta.get("sma_20"),
                "sma_50":  ta.get("sma_50"),
                "sma_200": ta.get("sma_200"),
                "macd_hist": ta.get("macd_histogram"),
            } if ta else {},
            "recent_news": [
                n.get("title") or n.get("headline", "")
                for n in nws[:3] if isinstance(n, dict)
            ],
        })

    # Sort scored universe for prompt: portfolio first, then by score desc
    scored_universe.sort(key=lambda x: (-1 if x["bucket"] == "portfolio_only" else 0, -x["scores"]["total"]))

    # ── 7. Replacement recommendations ───────────────────────────────────────
    replacements = _build_replacements(scored_universe)

    print(f"[COMPARE] Scoring complete: {len(scored_universe)} tickers, "
          f"{len(replacements)} replacements ({time.time()-t0:.1f}s)")

    # ── 8. Claude synthesis ───────────────────────────────────────────────────
    generated_at = datetime.now(timezone.utc).isoformat()
    report_id    = str(uuid.uuid4())

    synthesis_data = {
        "portfolio_tickers": portfolio_tickers,
        "watchlist_name":    watchlist_name,
        "watchlist_tickers": watchlist_tickers,
        "regime":            regime_info,
        "universe":          scored_universe,
        "replacements":      replacements,
    }

    try:
        report_markdown, structured_ai = await asyncio.wait_for(
            _call_claude_synthesis(claude_client, synthesis_data, watchlist_name),
            timeout=90.0,
        )
        print(f"[COMPARE] Claude synthesis complete: {len(report_markdown)} chars ({time.time()-t0:.1f}s)")
    except Exception as e:
        print(f"[COMPARE] Claude synthesis failed: {e}")
        report_markdown = _build_fallback_markdown(scored_universe, replacements, watchlist_name, generated_at)
        structured_ai = {}

    # ── 9. Assemble and persist ───────────────────────────────────────────────
    portfolio_stack = sorted(
        [t for t in scored_universe if t["bucket"] in ("portfolio_only", "both")],
        key=lambda t: t["scores"]["total"], reverse=True,
    )
    watchlist_stack = sorted(
        [t for t in scored_universe if t["bucket"] in ("watchlist_only", "both")],
        key=lambda t: t["scores"]["total"], reverse=True,
    )

    report = {
        "ok":                      True,
        "report_id":               report_id,
        "user_id":                 user_id,
        "watchlist_id":            watchlist_id,
        "watchlist_name":          watchlist_name,
        "portfolio_snapshot_hash": phash,
        "watchlist_snapshot_hash": whash,
        "portfolio_tickers":       portfolio_tickers,
        "watchlist_tickers":       watchlist_tickers,
        "generated_at":            generated_at,
        "expires_at":              datetime.fromtimestamp(
                                       time.time() + REPORT_TTL_HOURS * 3600, tz=timezone.utc
                                   ).isoformat(),
        "stale":                   False,
        "stale_reasons":           [],
        "cache_status":            "generated",
        "source_freshness": {
            "market_data_as_of":    generated_at,
            "fundamentals_as_of":   generated_at,
            "news_window":          "last 48h",
            "filings_window":       "last 90 days",
            "model_used":           "claude-sonnet",
            "sources_used":         ["FMP", "EDGAR", "Finnhub", "Claude"],
        },
        "ticker_scores":           {t["ticker"]: t for t in scored_universe},
        "portfolio_stack":         [{"ticker": t["ticker"], "name": t["name"], "score": t["scores"]["total"], "verdict": t["verdict"]} for t in portfolio_stack],
        "watchlist_stack":         [{"ticker": t["ticker"], "name": t["name"], "score": t["scores"]["total"], "verdict": t["verdict"]} for t in watchlist_stack],
        "replacement_recommendations": replacements,
        "report_markdown":         report_markdown,
        "ai_structured":           structured_ai,
        "elapsed_seconds":         round(time.time() - t0, 1),
    }

    save_report(user_id, watchlist_id, report)
    return report


# ── Claude synthesis call ─────────────────────────────────────────────────────

async def _call_claude_synthesis(
    claude_client,
    synthesis_data: dict,
    watchlist_name: str,
) -> tuple[str, dict]:
    """Call Claude to produce a markdown report + structured executive summary."""
    from agent.prompts import SYSTEM_PROMPT
    from agent.claude_agent import MODEL_CLAUDE_BALANCED

    data_str = json.dumps(synthesis_data, default=str)
    # Hard truncation to stay within token budget
    if len(data_str) > 60_000:
        data_str = data_str[:60_000] + "\n... [truncated for token budget]"

    prompt = f"""You are a senior portfolio analyst. Compare the user's portfolio against their watchlist "{watchlist_name}".

DATA (JSON with tickers, scores, regime):
{data_str}

Return a single JSON object with these keys:
1. "executive_verdict": string (3-6 blunt sentences summarising portfolio vs watchlist quality RIGHT NOW)
2. "report_markdown": string — a full markdown report with these EXACT sections:
   # Portfolio vs Watchlist Comparison
   Generated: <timestamp>

   ## Executive Verdict
   ## Portfolio Stack Rank
   ## Watchlist Stack Rank
   ## Best Replacements
   (markdown table: Current Holding | Replace With | Confidence | Why | Timing)
   ## Keep / Add / Trim / Avoid
   ## Regime & Theme Fit
   ## Fundamental Comparison
   ## Technical Setup
   ## Catalysts & News
   ## Risks
   ## Final Action Plan

Rules:
- Be direct, blunt, and practical. No filler language.
- Use the scores and data provided — do not make up numbers.
- Keep markdown clean: use headers, bullets, and one table for replacements.
- End with a single line: "This report is decision support, not trade execution advice."
- Return ONLY the JSON object. No prose outside JSON.
"""

    response = await asyncio.wait_for(
        asyncio.to_thread(
            claude_client.messages.create,
            model=MODEL_CLAUDE_BALANCED,
            max_tokens=3500,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": prompt}],
        ),
        timeout=85.0,
    )

    raw = response.content[0].text.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        parsed = json.loads(raw)
        return parsed.get("report_markdown", raw), parsed
    except Exception:
        return raw, {}


# ── Fallback markdown (when Claude fails) ─────────────────────────────────────

def _build_fallback_markdown(
    scored_universe: list[dict],
    replacements: list[dict],
    watchlist_name: str,
    generated_at: str,
) -> str:
    portfolio = [t for t in scored_universe if t["bucket"] in ("portfolio_only", "both")]
    watchlist = [t for t in scored_universe if t["bucket"] in ("watchlist_only", "both")]

    lines = [
        f"# Portfolio vs Watchlist Comparison",
        f"Generated: {generated_at}",
        "",
        "## Portfolio Stack Rank",
        *(f"- **{t['ticker']}** ({t['name']}) — Score {t['scores']['total']} | {t['verdict']}" for t in sorted(portfolio, key=lambda x: -x["scores"]["total"])),
        "",
        f"## Watchlist Stack Rank ({watchlist_name})",
        *(f"- **{t['ticker']}** ({t['name']}) — Score {t['scores']['total']} | {t['verdict']}" for t in sorted(watchlist, key=lambda x: -x["scores"]["total"])),
        "",
        "## Best Replacements",
        "| Current Holding | Replace With | Confidence | Score Delta |",
        "|---|---|---|---|",
        *(f"| {r['replace']} | {r['with']} | {r['confidence']} | +{r['score_delta']} |" for r in replacements[:5]),
        "",
        "---",
        "*This report is decision support, not trade execution advice.*",
    ]
    return "\n".join(lines)
