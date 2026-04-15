"""
Post-processors for structured AI responses.

All functions here are pure data transformations — no async, no model calls, no I/O.
They receive and mutate/replace result dicts after Claude/Grok returns a response.

Public API
----------
inject_crypto_chart_urls(structured)         → dict
enforce_best_trades(result, market_data, category, use_chatbox_mode) → None  (mutates result)
enforce_thematic_watchlist(result, market_data) → None  (mutates result)
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# Crypto chart URL injection
# Mirrors the logic in _gather_data's crypto arm (claude_agent.py lines 4980-4995)
# Fixes latent AttributeError: TradingAgent had no _inject_crypto_chart_urls definition.
# ---------------------------------------------------------------------------

def inject_crypto_chart_urls(structured: dict) -> dict:
    """
    Ensure every coin in a crypto structured response has a correct
    tradingview_symbol and chart URL.

    Called when parsed_display == "crypto" (claude_agent.py line 1103-1104).
    Returns the (mutated) structured dict.
    """
    try:
        from data.coingecko_provider import get_crypto_tv_symbol
    except Exception:
        return structured

    if not isinstance(structured, dict):
        return structured

    coin_list = structured.get("coins") or structured.get("top_coins") or []
    if isinstance(coin_list, list):
        for item in coin_list:
            if not isinstance(item, dict):
                continue
            sym = (item.get("symbol") or "").upper()
            if sym:
                tv_sym = get_crypto_tv_symbol(sym)
                item["tradingview_symbol"] = tv_sym
                item["chart"] = f"https://www.tradingview.com/chart/?symbol={tv_sym}"

    return structured


# ---------------------------------------------------------------------------
# Best-trades enforcement
# Source: claude_agent.py lines 1106-1235 (two consecutive if blocks)
# Mutates result in-place — mirrors the exact original logic.
# ---------------------------------------------------------------------------

def enforce_best_trades(
    result: dict,
    market_data: dict,
    category: str,
    use_chatbox_mode: bool,
    parsed_display: str,
) -> None:
    """
    Enforce a valid structured trades response for the best_trades category.

    Block 1 (lines 1106-1161): If Claude returned the wrong display_type,
    rebuild the structured dict from market_data top_trades.

    Block 2 (lines 1161-1235): If Claude returned "trades" but with sparse
    trade lists, backfill from market_data.
    """
    if not (category == "best_trades" and market_data and isinstance(market_data, dict) and not use_chatbox_mode):
        return

    # ── Block 1: wrong display_type → rebuild ────────────────────────────
    if parsed_display != "trades":
        print(f"[BEST_TRADES] Claude returned display_type={parsed_display}, enforcing structured trades output")
        claude_text = result.get("analysis", "") or result.get("structured", {}).get("message", "") or ""
        top_trades = market_data.get("top_trades", [])
        bearish_setups = market_data.get("bearish_setups", [])
        macro = market_data.get("market_pulse", {})
        scan_stats = market_data.get("scan_stats", {})
        for t in top_trades:
            if not t.get("thesis"):
                sigs = t.get("indicator_signals", t.get("signals_stacking", []))
                t["thesis"] = t.get("pattern", "Technical setup") + " — " + ", ".join(sigs[:3])
            if not t.get("why_could_fail"):
                t["why_could_fail"] = "Breakdown below stop level would invalidate setup"
            if not t.get("risk"):
                t["risk"] = t.get("why_could_fail", "")
        for t in bearish_setups:
            if not t.get("thesis"):
                t["thesis"] = "Bearish breakdown with multiple confirming signals"
            if not t.get("why_could_fail"):
                t["why_could_fail"] = "Reversal above resistance would invalidate short thesis"
            if not t.get("risk"):
                t["risk"] = t.get("why_could_fail", "")
        # Build empty-state context if no trades found
        empty_context = ""
        if not top_trades and not bearish_setups:
            dh = market_data.get("data_health", {})
            reasons = []
            if dh.get("budget_exhausted"):
                reasons.append("Candle API budget was exhausted")
            if dh.get("empty_reason"):
                reasons.append(dh["empty_reason"])
            ss = scan_stats
            if isinstance(ss, dict):
                if ss.get("candles_blocked", 0) > ss.get("candles_ok", 0):
                    reasons.append(f"Rate-limited: only {ss.get('candles_ok', 0)}/{ss.get('candle_targets', 0)} candles fetched")
                if ss.get("ta_qualified", 0) == 0 and ss.get("candles_ok", 0) > 0:
                    reasons.append("No tickers had 2+ confirming bullish signals")
            empty_context = " | ".join(reasons) if reasons else "No qualifying setups in current market conditions"

        structured = {
            "display_type": "trades",
            "market_pulse": {
                "verdict": macro.get("regime", "Neutral") if isinstance(macro, dict) else "Neutral",
                "regime": macro.get("regime", "") if isinstance(macro, dict) else "",
                "summary": claude_text[:300] if claude_text else "Market scan complete",
            },
            "top_trades": top_trades,
            "bearish_setups": bearish_setups,
            "scan_stats": scan_stats,
            "notes": ["TA-first scan with deterministic trade plans", "Trade plan numbers are pre-computed from OHLCV data"],
        }
        if empty_context:
            structured["empty_reason"] = empty_context
        result["structured"] = structured

    # ── Block 2: backfill sparse trade list ───────────────────────────────
    structured = result.get("structured")
    if isinstance(structured, dict):
        claude_trades = structured.get("top_trades", [])
        backend_trades = market_data.get("top_trades", [])

        if len(claude_trades) < len(backend_trades):
            claude_tickers = {t.get("ticker") for t in claude_trades if isinstance(t, dict)}
            for bt in backend_trades:
                if isinstance(bt, dict) and bt.get("ticker") not in claude_tickers:
                    if not bt.get("thesis"):
                        sigs = bt.get("indicator_signals", bt.get("signals_stacking", []))
                        bt["thesis"] = bt.get("pattern", "Technical setup") + " — " + ", ".join(sigs[:3])
                    if not bt.get("risk"):
                        bt["risk"] = bt.get("why_could_fail", "Breakdown below stop level would invalidate setup")
                    claude_trades.append(bt)
            structured["top_trades"] = claude_trades
            print(f"[BEST_TRADES] Backfilled: Claude had {len(claude_tickers)} trades, backend had {len(backend_trades)}, merged to {len(claude_trades)}")

        if not structured.get("top_trades") and backend_trades:
            for bt in backend_trades:
                if isinstance(bt, dict):
                    if not bt.get("thesis"):
                        sigs = bt.get("indicator_signals", bt.get("signals_stacking", []))
                        bt["thesis"] = bt.get("pattern", "Technical setup") + " — " + ", ".join(sigs[:3])
                    if not bt.get("risk"):
                        bt["risk"] = bt.get("why_could_fail", "Breakdown below stop level would invalidate setup")
            structured["top_trades"] = backend_trades
            print(f"[BEST_TRADES] Forced {len(backend_trades)} backend trades (Claude returned 0)")

        if not structured.get("bearish_setups") and market_data.get("bearish_setups"):
            backend_bearish = market_data["bearish_setups"]
            for bt in backend_bearish:
                if isinstance(bt, dict):
                    if not bt.get("thesis"):
                        bt["thesis"] = "Bearish breakdown with multiple confirming signals"
                    if not bt.get("risk"):
                        bt["risk"] = bt.get("why_could_fail", "Reversal above resistance would invalidate short thesis")
            structured["bearish_setups"] = backend_bearish

        if not structured.get("market_pulse") and market_data.get("market_pulse"):
            macro = market_data["market_pulse"]
            structured["market_pulse"] = {
                "verdict": macro.get("regime", "Neutral") if isinstance(macro, dict) else "Neutral",
                "regime": macro.get("regime", "") if isinstance(macro, dict) else "",
                "summary": macro.get("summary", "Market scan complete") if isinstance(macro, dict) else "Market scan complete",
            }

        if not structured.get("scan_stats") and market_data.get("scan_stats"):
            structured["scan_stats"] = market_data["scan_stats"]

        for t in structured.get("top_trades", []):
            if isinstance(t, dict):
                if not t.get("risk"):
                    t["risk"] = t.get("why_could_fail", "Breakdown below stop level would invalidate setup")
                if not t.get("indicator_signals") and t.get("signals_stacking"):
                    t["indicator_signals"] = [s.replace("_", " ").title() for s in t["signals_stacking"]]
        for t in structured.get("bearish_setups", []):
            if isinstance(t, dict) and not t.get("risk"):
                t["risk"] = t.get("why_could_fail", "Reversal above resistance would invalidate short thesis")

        data_health = market_data.get("data_health")
        if data_health:
            structured.setdefault("meta", {})["data_health"] = data_health


# ---------------------------------------------------------------------------
# Thematic watchlist backfill
# Source: TradingAgent._enforce_thematic_watchlist (claude_agent.py lines 1467-1534)
# Mutates result["structured"]["watchlist_today"] in-place.
# ---------------------------------------------------------------------------

def enforce_thematic_watchlist(result: dict, market_data: dict) -> None:
    """
    Backfill watchlist_today in thematic structured response from market_data
    if Claude left it empty. Mirrors TradingAgent._enforce_thematic_watchlist exactly.
    """
    import re as _re

    structured = result.get("structured")
    if not isinstance(structured, dict):
        return

    wt = structured.get("watchlist_today")
    if isinstance(wt, dict) and any(wt.get(k) for k in ("large_cap", "mid_cap", "low_cap", "buy_right_now")):
        return

    ranked_tickers = market_data.get("ranked_tickers", [])
    enriched_data = market_data.get("enriched_data", {})
    if not ranked_tickers:
        return

    def _parse_mcap_b(cap_str) -> float:
        if not cap_str or not isinstance(cap_str, str):
            return 0.0
        s = cap_str.upper().strip()
        m = _re.search(r"([\d,.]+)\s*([BM]?)", s)
        if not m:
            return 0.0
        num = float(m.group(1).replace(",", ""))
        suffix = m.group(2)
        return num if suffix == "B" else (num / 1000.0 if suffix == "M" else num)

    large_cap, mid_cap, low_cap = [], [], []
    ticker_rank = {item.get("ticker"): idx + 1 for idx, item in enumerate(ranked_tickers)}

    for item in ranked_tickers:
        ticker = item.get("ticker", "")
        score = item.get("score", 0)
        edata = enriched_data.get(ticker, {})
        overview = edata.get("overview", {}) if isinstance(edata, dict) else {}
        cap_b = _parse_mcap_b(overview.get("market_cap", ""))
        tier = "large" if cap_b >= 10.0 else ("mid" if cap_b >= 2.0 else "low")
        entry = {
            "ticker": ticker,
            "company": overview.get("company_name", ticker),
            "market_cap_tier": tier,
            "conviction": "High" if score > 70 else ("Medium" if score > 40 else "Low"),
            "conviction_score": round(float(score)),
            "why_now": f"Backend-ranked #{ticker_rank.get(ticker, '?')} in sector (composite score {round(float(score))})",
            "catalyst": "",
        }
        if tier == "large":
            large_cap.append(entry)
        elif tier == "mid":
            mid_cap.append(entry)
        else:
            low_cap.append(entry)

    large_cap = large_cap[:3]
    mid_cap = mid_cap[:3]
    low_cap = low_cap[:3]

    all_ranked = large_cap + mid_cap + low_cap
    buy_right_now = dict(all_ranked[0]) if all_ranked else {}

    structured["watchlist_today"] = {
        "large_cap": [dict(e, rank=i + 1) for i, e in enumerate(large_cap)],
        "mid_cap": [dict(e, rank=i + 1) for i, e in enumerate(mid_cap)],
        "low_cap": [dict(e, rank=i + 1) for i, e in enumerate(low_cap)],
        "buy_right_now": buy_right_now,
    }
    print(f"[THEMATIC] Backfilled watchlist_today: {len(large_cap)} large, {len(mid_cap)} mid, {len(low_cap)} low")
