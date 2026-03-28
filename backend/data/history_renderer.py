"""
Convert structured agent responses into clean, human-readable text
for the History page.  Each display_type gets its own mini-renderer.

Priority: stock picks, tickers, and trade plans come FIRST.
Market summaries and context are secondary.
"""


def render_structured_to_text(result: dict) -> str:
    """
    Given a full agent result dict (with 'structured' and optionally 'analysis'),
    return a clean readable string for the history panel.
    """
    if not isinstance(result, dict):
        return str(result)[:2000]

    structured = result.get("structured", {})
    if not isinstance(structured, dict):
        return result.get("analysis", "") or str(result)[:2000]

    display_type = structured.get("display_type", "")

    renderers = {
        "briefing": _render_briefing,
        "trades": _render_picks,
        "investments": _render_picks,
        "fundamentals": _render_picks,
        "technicals": _render_picks,
        "analysis": _render_analysis,
        "macro": _render_macro,
        "crypto": _render_crypto,
        "sector_rotation": _render_sector_rotation,
        "trending": _render_trending,
        "cross_market": _render_cross_market,
        "portfolio": _render_portfolio,
        "screener": _render_screener,
        "commodities": _render_commodities,
        "chat": _render_chat,
        "csv_watchlist": _render_csv_watchlist,
        "headlines": _render_headlines,
        "social": _render_social,
        "earnings": _render_earnings,
        "prediction_markets": _render_prediction_markets,
    }

    renderer = renderers.get(display_type)
    if renderer:
        try:
            return renderer(structured)
        except Exception:
            pass

    # Fallback: use analysis text or summary
    text = result.get("analysis", "")
    if not text:
        text = structured.get("summary", "") or structured.get("message", "")
    if not text:
        text = _generic_fallback(structured)
    return text[:8000]


# ── Briefing ─────────────────────────────────────────────────

def _render_briefing(s: dict) -> str:
    lines = []

    # Market pulse verdict (one line)
    mp = s.get("market_pulse", {})
    if isinstance(mp, dict):
        verdict = mp.get("verdict", "")
        regime = mp.get("regime", "")
        if verdict:
            header = f"Market Pulse: {verdict}"
            if regime:
                header += f" ({regime})"
            lines.append(header)
        summary = mp.get("summary", "")
        if summary:
            lines.append(summary)

    # Key Numbers — always useful
    kn = s.get("key_numbers", {})
    if isinstance(kn, dict):
        nums = []
        for sym in ["spy", "qqq", "iwm", "vix", "fear_greed", "dxy", "ten_year", "oil", "gold", "btc"]:
            v = kn.get(sym, {})
            if isinstance(v, dict):
                price = v.get("price") or v.get("yield") or v.get("value", "")
                if not price:
                    continue
                change = v.get("change", "")
                part = f"{sym.upper()} {price}"
                if change:
                    part += f" ({change})"
                nums.append(part)
        if nums:
            lines.append("")
            lines.append("Key Numbers: " + " | ".join(nums))

    # TOP MOVES — the stuff the user actually wants
    tm = s.get("top_moves", [])
    if tm:
        lines.append("")
        lines.append("═══ TOP MOVES ═══")
        for move in tm[:8]:
            if isinstance(move, dict):
                _render_pick_line(lines, move, prefix=f"#{move.get('rank', '')}")
                lines.append("")

    # Signal highlights
    sh = s.get("signal_highlights", {})
    if isinstance(sh, dict) and sh:
        lines.append("═══ SIGNALS ═══")
        for key, val in sh.items():
            if isinstance(val, dict):
                ticker = val.get("ticker", "")
                signal = val.get("signal", "")
                label = key.replace("_", " ").title()
                lines.append(f"  {label}: {ticker} — {signal}")
        lines.append("")

    # Bearish setups
    bs = s.get("bearish_setups", [])
    if bs:
        lines.append("═══ BEARISH SETUPS ═══")
        for setup in bs[:5]:
            if isinstance(setup, dict):
                _render_pick_line(lines, setup)
                lines.append("")

    # What's moving
    wm = s.get("whats_moving", [])
    if wm:
        lines.append("What's Moving:")
        for item in wm[:5]:
            if isinstance(item, dict):
                lines.append(f"  • {item.get('headline', '')}")
        lines.append("")

    return "\n".join(lines)


# ── Trades / Investments / Fundamentals / Technicals ─────────

def _render_picks(s: dict) -> str:
    lines = []

    picks = s.get("picks", [])
    if not picks:
        picks = s.get("top_trades", [])

    if picks:
        lines.append("═══ PICKS ═══")
        for i, pick in enumerate(picks[:10], 1):
            if isinstance(pick, dict):
                _render_pick_line(lines, pick, prefix=f"#{i}")
                lines.append("")

    ctx = s.get("market_context", "")
    if ctx:
        lines.append(f"Context: {ctx}")

    bias = s.get("portfolio_bias", "")
    if isinstance(bias, str) and bias:
        lines.append(f"Portfolio Bias: {bias}")
    elif isinstance(bias, dict):
        lines.append(f"Portfolio Bias: {bias.get('overall', '')}")

    return "\n".join(lines)


def _render_pick_line(lines: list, pick: dict, prefix: str = ""):
    ticker = pick.get("ticker", pick.get("symbol", ""))
    company = pick.get("company", "")
    price = pick.get("price", pick.get("entry", ""))
    action = pick.get("action", pick.get("direction", ""))
    conviction = pick.get("conviction", "")
    score = pick.get("conviction_score", "")
    tier = pick.get("position_tier", "")

    header_parts = []
    if prefix:
        header_parts.append(prefix)
    if ticker:
        header_parts.append(ticker)
    if company:
        header_parts.append(f"({company})")
    if price:
        price_str = str(price)
        header_parts.append(f"@ ${price_str}" if not price_str.startswith("$") else f"@ {price_str}")
    if action:
        header_parts.append(f"[{action}]")
    if conviction:
        header_parts.append(f"— {conviction}")
    if score:
        header_parts.append(f"({score}/100)")
    if tier:
        header_parts.append(f"— {tier}")
    lines.append(" ".join(header_parts))

    thesis = pick.get("thesis", pick.get("investment_thesis", ""))
    if thesis:
        lines.append(f"  Thesis: {thesis}")

    catalyst = pick.get("catalyst", "")
    if catalyst:
        lines.append(f"  Catalyst: {catalyst}")

    risk = pick.get("why_could_fail", pick.get("risk", ""))
    if risk:
        lines.append(f"  Risk: {risk}")

    tp = pick.get("trade_plan", {})
    if isinstance(tp, dict) and tp.get("entry"):
        entry = tp.get("entry", "")
        stop = tp.get("stop", "")
        t1 = tp.get("target_1", tp.get("target", ""))
        rr = tp.get("risk_reward", "")
        parts = []
        if entry:
            parts.append(f"Entry {entry}")
        if stop:
            parts.append(f"Stop {stop}")
        if t1:
            parts.append(f"Target {t1}")
        if rr:
            parts.append(f"R:R {rr}")
        lines.append(f"  Trade: {' | '.join(parts)}")


# ── Analysis (single ticker) ────────────────────────────────

def _render_analysis(s: dict) -> str:
    lines = []
    ticker = s.get("ticker", "")
    company = s.get("company", "")
    price = s.get("price", "")
    verdict = s.get("verdict", "")
    score = s.get("conviction_score", "")
    action = s.get("action", "")

    header = f"{ticker}"
    if company:
        header += f" ({company})"
    if price:
        header += f" @ ${price}"
    if action:
        header += f" [{action}]"
    lines.append(header)

    if verdict:
        lines.append(f"Verdict: {verdict}")
    if score:
        lines.append(f"Conviction: {score}/100")

    thesis = s.get("thesis", s.get("investment_thesis", ""))
    if thesis:
        lines.append(f"Thesis: {thesis}")

    catalyst = s.get("catalyst", "")
    if catalyst:
        lines.append(f"Catalyst: {catalyst}")

    risk = s.get("why_could_fail", "")
    if risk:
        lines.append(f"Risk: {risk}")

    tp = s.get("trade_plan", {})
    if isinstance(tp, dict) and tp.get("entry"):
        lines.append(f"Trade: Entry {tp.get('entry')} | Stop {tp.get('stop')} | Target {tp.get('target_1')} | R:R {tp.get('risk_reward')}")

    return "\n".join(lines)


# ── Macro ────────────────────────────────────────────────────

def _render_macro(s: dict) -> str:
    lines = []
    regime = s.get("market_regime", "")
    summary = s.get("summary", "")
    if regime:
        lines.append(f"Market Regime: {regime}")
    if summary:
        lines.append(summary)

    ki = s.get("key_indicators", {})
    if isinstance(ki, dict):
        lines.append("")
        lines.append("Key Indicators:")
        for k, v in ki.items():
            lines.append(f"  {k.replace('_', ' ').title()}: {v}")

    impl = s.get("implications", {})
    if isinstance(impl, dict):
        lines.append("")
        lines.append("Implications:")
        for k, v in impl.items():
            lines.append(f"  {k.replace('_', ' ').title()}: {v}")

    pos = s.get("positioning", "")
    if pos:
        lines.append(f"\nPositioning: {pos}")

    return "\n".join(lines)


# ── Crypto ───────────────────────────────────────────────────

def _render_crypto(s: dict) -> str:
    lines = []

    # Top momentum picks first
    tm = s.get("top_momentum", [])
    if tm:
        lines.append("═══ TOP MOMENTUM ═══")
        for coin in tm[:8]:
            if isinstance(coin, dict):
                _render_pick_line(lines, {
                    "ticker": coin.get("symbol", coin.get("coin", "")),
                    "price": coin.get("price"),
                    "action": coin.get("action", coin.get("direction", "")),
                    "conviction": coin.get("conviction"),
                    "conviction_score": coin.get("conviction_score"),
                    "thesis": coin.get("thesis"),
                    "why_could_fail": coin.get("why_could_fail"),
                    "trade_plan": coin.get("trade_plan"),
                })
                lines.append("")

    overview = s.get("market_overview", "")
    if overview:
        lines.append(overview)

    btc_eth = s.get("btc_eth_summary", {})
    if isinstance(btc_eth, dict):
        lines.append("")
        for coin in ["btc", "eth"]:
            data = btc_eth.get(coin, {})
            if isinstance(data, dict):
                p = data.get("price", "")
                c24 = data.get("change_24h", "")
                sig = data.get("signal", "")
                lines.append(f"  {coin.upper()}: ${p} ({c24}) — {sig}")

    return "\n".join(lines)


# ── Sector Rotation ──────────────────────────────────────────

def _render_sector_rotation(s: dict) -> str:
    lines = []
    summary = s.get("summary", "")
    if summary:
        lines.append(summary)

    sectors = s.get("sectors", [])
    if sectors:
        lines.append("")
        for sec in sectors:
            if isinstance(sec, dict):
                etf = sec.get("etf", "")
                name = sec.get("sector", "")
                change = sec.get("change_today", "")
                trend = sec.get("trend", "")
                signal = sec.get("signal", "")
                vs_spy = sec.get("vs_spy", "")
                line = f"  {etf} ({name}): {change}%"
                if vs_spy:
                    line += f" vs SPY: {vs_spy}%"
                if trend:
                    line += f" | {trend}"
                if signal:
                    line += f" | {signal}"
                lines.append(line)

    rot = s.get("rotation_analysis", "")
    if rot:
        lines.append(f"\n{rot}")

    return "\n".join(lines)


# ── Trending ─────────────────────────────────────────────────

def _render_trending(s: dict) -> str:
    lines = []

    tickers = s.get("trending_tickers", [])
    if tickers:
        lines.append("═══ TRENDING ═══")
        for t in tickers[:10]:
            if isinstance(t, dict):
                ticker = t.get("ticker", "")
                src = t.get("source_count", "")
                why = t.get("why_trending", "")
                verdict = t.get("verdict", "")
                score = t.get("conviction_score", "")
                line = f"  {ticker}"
                if src:
                    line += f" ({src} sources)"
                if score:
                    line += f" [{score}/100]"
                if verdict:
                    line += f" — {verdict}"
                lines.append(line)
                if why:
                    lines.append(f"    {why}")

    summary = s.get("summary", "")
    if summary:
        lines.append("")
        lines.append(summary)

    return "\n".join(lines)


# ── Cross Market ─────────────────────────────────────────────

def _render_cross_market(s: dict) -> str:
    lines = []

    equities = s.get("equities", {})
    if isinstance(equities, dict):
        for bucket in ["large_caps", "mid_caps", "small_micro_caps"]:
            items = equities.get(bucket, [])
            if items:
                lines.append(f"═══ {bucket.replace('_', ' ').upper()} ═══")
                for item in items[:5]:
                    if isinstance(item, dict):
                        _render_pick_line(lines, item)
                        lines.append("")

    for section in ["crypto", "commodities"]:
        items = s.get(section, [])
        if items and isinstance(items, list):
            lines.append(f"═══ {section.upper()} ═══")
            for item in items[:5]:
                if isinstance(item, dict):
                    _render_pick_line(lines, item)
                    lines.append("")

    mr = s.get("macro_regime", {})
    if isinstance(mr, dict):
        lines.append(f"Regime: {mr.get('verdict', '')} | VIX: {mr.get('vix', '')} | Fear/Greed: {mr.get('fear_greed', '')}")
        summary = mr.get("summary", "")
        if summary:
            lines.append(summary)

    return "\n".join(lines)


# ── Portfolio ────────────────────────────────────────────────

def _render_portfolio(s: dict) -> str:
    lines = []

    positions = s.get("positions", [])
    if positions:
        lines.append("═══ POSITIONS ═══")
        for pos in positions:
            if isinstance(pos, dict):
                ticker = pos.get("ticker", "")
                rating = pos.get("rating", "")
                score = pos.get("combined_score", "")
                thesis = pos.get("thesis", "")
                action = pos.get("action", "")
                line = f"  {ticker}: {rating}"
                if score:
                    line += f" ({score}/100)"
                if action:
                    line += f" [{action}]"
                lines.append(line)
                if thesis:
                    lines.append(f"    {thesis}")

    summary = s.get("summary", "")
    if summary:
        lines.append("")
        lines.append(summary)

    insights = s.get("portfolio_insights", {})
    if isinstance(insights, dict):
        flags = insights.get("risk_flags", [])
        if flags:
            lines.append("\nRisk Flags:")
            for f in flags:
                lines.append(f"  • {f}")

    return "\n".join(lines)


# ── Screener ─────────────────────────────────────────────────

def _render_screener(s: dict) -> str:
    lines = []

    results = s.get("results", [])
    if results:
        lines.append(f"═══ RESULTS ({s.get('total_matches', len(results))}) ═══")
        for r in results[:15]:
            if isinstance(r, dict):
                ticker = r.get("ticker", "")
                price = r.get("price", "")
                change = r.get("change_pct", "")
                note = r.get("note", "")
                line = f"  {ticker} @ ${price}"
                if change:
                    line += f" ({change}%)"
                if note:
                    line += f" — {note}"
                lines.append(line)

    top = s.get("top_picks", [])
    if top:
        lines.append("\nTop Picks:")
        for t in top:
            if isinstance(t, dict):
                lines.append(f"  {t.get('ticker', '')}: {t.get('why', '')}")

    interp = s.get("query_interpretation", "")
    if interp:
        lines.append(f"\nQuery: {interp}")

    obs = s.get("observations", "")
    if obs:
        lines.append(f"\n{obs}")

    return "\n".join(lines)


# ── Commodities ──────────────────────────────────────────────

def _render_commodities(s: dict) -> str:
    lines = []

    plays = s.get("top_conviction_plays", [])
    if plays:
        lines.append("═══ TOP CONVICTION ═══")
        for p in plays:
            if isinstance(p, dict):
                lines.append(f"  {p.get('asset', '')} {p.get('direction', '')}: {p.get('thesis', '')}")
        lines.append("")

    comms = s.get("commodities", [])
    if comms:
        for c in comms:
            if isinstance(c, dict):
                name = c.get("name", c.get("symbol", ""))
                price = c.get("price", "")
                change = c.get("change_today", "")
                trend = c.get("trend_short", "")
                line = f"  {name}: ${price}"
                if change:
                    line += f" ({change}%)"
                if trend:
                    line += f" {trend}"
                lines.append(line)

    summary = s.get("summary", "")
    if summary:
        lines.append("")
        lines.append(summary)

    return "\n".join(lines)


# ── Headlines / News ─────────────────────────────────────────

def _render_headlines(s: dict) -> str:
    lines = []

    headlines = s.get("headlines", s.get("stories", []))
    if headlines:
        for h in headlines[:10]:
            if isinstance(h, dict):
                title = h.get("title", h.get("headline", ""))
                source = h.get("source", "")
                tickers = h.get("tickers", [])
                impact = h.get("market_impact", h.get("impact", ""))
                line = f"  • {title}"
                if source:
                    line += f" [{source}]"
                lines.append(line)
                details = []
                if tickers:
                    details.append(f"Tickers: {', '.join(tickers) if isinstance(tickers, list) else tickers}")
                if impact:
                    details.append(f"Impact: {impact}")
                if details:
                    lines.append(f"    {' | '.join(details)}")
            elif isinstance(h, str):
                lines.append(f"  • {h}")

    summary = s.get("summary", s.get("market_context", ""))
    if summary:
        lines.append("")
        lines.append(summary)

    return "\n".join(lines)


# ── Social Momentum ──────────────────────────────────────────

def _render_social(s: dict) -> str:
    lines = []

    tickers = s.get("trending_tickers", s.get("mentions", []))
    if tickers:
        lines.append("═══ SOCIAL MOMENTUM ═══")
        for t in tickers[:10]:
            if isinstance(t, dict):
                ticker = t.get("ticker", t.get("symbol", ""))
                mentions = t.get("mention_count", t.get("mentions", ""))
                sentiment = t.get("sentiment", "")
                score = t.get("conviction_score", "")
                line = f"  {ticker}"
                if mentions:
                    line += f" ({mentions} mentions)"
                if score:
                    line += f" [{score}/100]"
                if sentiment:
                    line += f" — {sentiment}"
                lines.append(line)

    summary = s.get("summary", "")
    if summary:
        lines.append("")
        lines.append(summary)

    return "\n".join(lines)


# ── Earnings ─────────────────────────────────────────────────

def _render_earnings(s: dict) -> str:
    lines = []

    events = s.get("upcoming", s.get("earnings", s.get("catalysts", [])))
    if events:
        lines.append("═══ UPCOMING CATALYSTS ═══")
        for e in events[:10]:
            if isinstance(e, dict):
                ticker = e.get("ticker", e.get("symbol", ""))
                date = e.get("date", e.get("report_date", ""))
                est = e.get("est_move", e.get("expected_move", ""))
                line = f"  {ticker}"
                if date:
                    line += f" — {date}"
                if est:
                    line += f" (est move: {est})"
                lines.append(line)

    summary = s.get("summary", "")
    if summary:
        lines.append("")
        lines.append(summary)

    return "\n".join(lines)


# ── Prediction Markets ──────────────────────────────────────

def _render_prediction_markets(s: dict) -> str:
    lines = []

    markets = s.get("markets", s.get("predictions", []))
    if markets:
        for m in markets[:10]:
            if isinstance(m, dict):
                title = m.get("title", m.get("question", ""))
                prob = m.get("probability", m.get("yes_price", ""))
                change = m.get("change_24h", "")
                line = f"  • {title}"
                if prob:
                    line += f" [{prob}%]"
                if change:
                    line += f" ({change})"
                lines.append(line)

    summary = s.get("summary", "")
    if summary:
        lines.append("")
        lines.append(summary)

    return "\n".join(lines)


# ── CSV Watchlist ────────────────────────────────────────────

def _render_csv_watchlist(s: dict) -> str:
    lines = []
    for bucket in ["strong_buy", "buy", "hold", "sell"]:
        items = s.get(bucket, [])
        if items:
            label = bucket.replace("_", " ").upper()
            lines.append(f"{label}:")
            for item in items:
                if isinstance(item, dict):
                    ticker = item.get("ticker", "")
                    price = item.get("price", "")
                    thesis = item.get("thesis", item.get("note", ""))
                    line = f"  {ticker}"
                    if price:
                        line += f" @ ${price}"
                    if thesis:
                        line += f" — {thesis}"
                    lines.append(line)
            lines.append("")
    return "\n".join(lines)


# ── Chat ─────────────────────────────────────────────────────

def _render_chat(s: dict) -> str:
    return s.get("message", "")


# ── Generic fallback ─────────────────────────────────────────

def _generic_fallback(s: dict) -> str:
    """Try to extract anything readable from an unknown structured response."""
    parts = []
    for key in ["summary", "message", "verdict", "market_context", "overview"]:
        val = s.get(key)
        if isinstance(val, str) and val:
            parts.append(val)
    if parts:
        return "\n".join(parts)
    # Last resort: list top-level keys so user sees something
    return f"Response type: {s.get('display_type', 'unknown')} (contains: {', '.join(k for k in s if k != 'display_type')})"
