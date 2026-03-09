"""
Convert structured agent responses into clean, human-readable text
for the History page.  Each display_type gets its own mini-renderer.
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

    mp = s.get("market_pulse", {})
    if isinstance(mp, dict):
        verdict = mp.get("verdict", "")
        summary = mp.get("summary", "")
        regime = mp.get("regime", "")
        if verdict:
            lines.append(f"Market Pulse: {verdict}")
        if regime:
            lines.append(f"Regime: {regime}")
        if summary:
            lines.append(summary)

    kn = s.get("key_numbers", {})
    if isinstance(kn, dict):
        lines.append("")
        lines.append("Key Numbers:")
        for sym in ["spy", "qqq", "iwm", "vix", "fear_greed", "dxy", "ten_year", "oil", "gold"]:
            v = kn.get(sym, {})
            if isinstance(v, dict):
                price = v.get("price") or v.get("yield") or v.get("value", "")
                if not price:
                    continue
                change = v.get("change", "")
                label = v.get("label", "")
                parts = [sym.upper(), str(price)]
                if change:
                    parts.append(f"({change})")
                if label:
                    parts.append(f"[{label}]")
                lines.append(f"  {' '.join(parts)}")

    wm = s.get("whats_moving", [])
    if wm:
        lines.append("")
        lines.append("What's Moving:")
        for item in wm[:5]:
            if isinstance(item, dict):
                lines.append(f"  - {item.get('headline', '')}")

    sh = s.get("signal_highlights", {})
    if isinstance(sh, dict) and sh:
        lines.append("")
        lines.append("Signal Highlights:")
        for key, val in sh.items():
            if isinstance(val, dict):
                ticker = val.get("ticker", "")
                signal = val.get("signal", "")
                label = key.replace("_", " ").title()
                lines.append(f"  {label}: {ticker} - {signal}")

    tm = s.get("top_moves", [])
    if tm:
        lines.append("")
        lines.append("Top Moves:")
        for move in tm[:5]:
            if isinstance(move, dict):
                _render_pick_line(lines, move, prefix=f"#{move.get('rank', '')}")

    return "\n".join(lines)


# ── Trades / Investments / Fundamentals / Technicals ─────────

def _render_picks(s: dict) -> str:
    lines = []
    ctx = s.get("market_context", "")
    if ctx:
        lines.append(ctx)
        lines.append("")

    picks = s.get("picks", [])
    if not picks:
        picks = s.get("top_trades", [])

    for i, pick in enumerate(picks[:10], 1):
        if isinstance(pick, dict):
            _render_pick_line(lines, pick, prefix=f"#{i}")
            lines.append("")

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
    action = pick.get("action", "")
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
        header_parts.append(f"@ ${price}" if not str(price).startswith("$") else f"@ {price}")
    if action:
        header_parts.append(f"[{action}]")
    if conviction:
        header_parts.append(f"- {conviction}")
    if score:
        header_parts.append(f"({score}/100)")
    if tier:
        header_parts.append(f"- {tier}")
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
        lines.append(f"  Trade: Entry {entry} | Stop {stop} | Target {t1} | R:R {rr}")


# ── Analysis (single ticker) ────────────────────────────────

def _render_analysis(s: dict) -> str:
    lines = []
    ticker = s.get("ticker", "")
    company = s.get("company", "")
    price = s.get("price", "")
    verdict = s.get("verdict", "")
    score = s.get("conviction_score", "")

    header = f"{ticker}"
    if company:
        header += f" ({company})"
    if price:
        header += f" @ ${price}"
    lines.append(header)

    if verdict:
        lines.append(f"Verdict: {verdict}")
    if score:
        lines.append(f"Conviction: {score}/100")

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
                lines.append(f"  {coin.upper()}: ${p} ({c24}) - {sig}")

    tm = s.get("top_momentum", [])
    if tm:
        lines.append("")
        lines.append("Top Momentum:")
        for coin in tm[:8]:
            if isinstance(coin, dict):
                _render_pick_line(lines, {
                    "ticker": coin.get("symbol", coin.get("coin", "")),
                    "price": coin.get("price"),
                    "conviction": coin.get("conviction"),
                    "conviction_score": coin.get("conviction_score"),
                    "thesis": coin.get("thesis"),
                    "why_could_fail": coin.get("why_could_fail"),
                    "trade_plan": coin.get("trade_plan"),
                })
                lines.append("")

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
    summary = s.get("summary", "")
    if summary:
        lines.append(summary)

    tickers = s.get("trending_tickers", [])
    if tickers:
        lines.append("")
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
                    line += f" - {verdict}"
                lines.append(line)
                if why:
                    lines.append(f"    {why}")

    return "\n".join(lines)


# ── Cross Market ─────────────────────────────────────────────

def _render_cross_market(s: dict) -> str:
    lines = []
    mr = s.get("macro_regime", {})
    if isinstance(mr, dict):
        lines.append(f"Regime: {mr.get('verdict', '')} | VIX: {mr.get('vix', '')} | Fear/Greed: {mr.get('fear_greed', '')}")
        summary = mr.get("summary", "")
        if summary:
            lines.append(summary)

    equities = s.get("equities", {})
    if isinstance(equities, dict):
        for bucket in ["large_caps", "mid_caps", "small_micro_caps"]:
            items = equities.get(bucket, [])
            if items:
                lines.append(f"\n{bucket.replace('_', ' ').title()}:")
                for item in items[:5]:
                    if isinstance(item, dict):
                        _render_pick_line(lines, item)

    for section in ["crypto", "commodities"]:
        items = s.get(section, [])
        if items and isinstance(items, list):
            lines.append(f"\n{section.title()}:")
            for item in items[:5]:
                if isinstance(item, dict):
                    _render_pick_line(lines, item)

    return "\n".join(lines)


# ── Portfolio ────────────────────────────────────────────────

def _render_portfolio(s: dict) -> str:
    lines = []
    summary = s.get("summary", "")
    if summary:
        lines.append(summary)

    positions = s.get("positions", [])
    if positions:
        lines.append("")
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
                    line += f" - {action}"
                lines.append(line)
                if thesis:
                    lines.append(f"    {thesis}")

    insights = s.get("portfolio_insights", {})
    if isinstance(insights, dict):
        flags = insights.get("risk_flags", [])
        if flags:
            lines.append("\nRisk Flags:")
            for f in flags:
                lines.append(f"  - {f}")

    return "\n".join(lines)


# ── Screener ─────────────────────────────────────────────────

def _render_screener(s: dict) -> str:
    lines = []
    interp = s.get("query_interpretation", "")
    if interp:
        lines.append(f"Query: {interp}")

    results = s.get("results", [])
    if results:
        lines.append(f"\nResults ({s.get('total_matches', len(results))}):")
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
                    line += f" - {note}"
                lines.append(line)

    top = s.get("top_picks", [])
    if top:
        lines.append("\nTop Picks:")
        for t in top:
            if isinstance(t, dict):
                lines.append(f"  {t.get('ticker', '')}: {t.get('why', '')}")

    obs = s.get("observations", "")
    if obs:
        lines.append(f"\n{obs}")

    return "\n".join(lines)


# ── Commodities ──────────────────────────────────────────────

def _render_commodities(s: dict) -> str:
    lines = []
    summary = s.get("summary", "")
    if summary:
        lines.append(summary)

    comms = s.get("commodities", [])
    if comms:
        lines.append("")
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

    plays = s.get("top_conviction_plays", [])
    if plays:
        lines.append("\nTop Conviction:")
        for p in plays:
            if isinstance(p, dict):
                lines.append(f"  {p.get('asset', '')} {p.get('direction', '')}: {p.get('thesis', '')}")

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
                        line += f" - {thesis}"
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
