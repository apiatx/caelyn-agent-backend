"""
Quantitative scoring engine for ranking stock candidates.
Runs BEFORE Claude — no AI calls, pure math.

Each query type has a different scoring formula that weights
indicators differently. A "best trades" query weights volume
and momentum heavily. A "best investments" query weights
fundamentals and valuation.

This lets us cast a wide net (50-100 candidates), score them
all cheaply, and send only the top 10-15 to Claude for deep
qualitative analysis.
"""


def score_for_trades(ticker_data: dict) -> float:
    """
    Score a ticker for short-term trading setup quality.

    This scores SETUPS, not just stocks that already moved.
    A stock up 15% with no volume and overbought RSI scores LOW.
    A stock up 3% breaking above 50 SMA on 3x volume with MACD crossover scores HIGH.

    Weights:
    - Volume confirmation (25 pts): Is volume confirming the move?
    - Technical alignment (30 pts): Are multiple TA indicators aligned?
    - Momentum quality (20 pts): Is the move sustainable, not exhausted?
    - Sentiment tailwind (15 pts): Is social/analyst sentiment supportive?
    - Setup freshness (10 pts): Is this early-stage or already extended?
    """
    score = 0.0
    snapshot = ticker_data.get("snapshot", {})
    technicals = ticker_data.get("technicals", {})
    sentiment = ticker_data.get("sentiment", {}) or ticker_data.get("stocktwits", {})
    details = ticker_data.get("details", {})

    price = snapshot.get("price")
    change_pct = snapshot.get("change_pct")

    # ── Volume Confirmation (25 pts) ──
    volume = snapshot.get("volume")
    avg_vol = None
    if isinstance(details, dict):
        avg_vol = details.get("avg_volume")

    volume_ratio = 0
    if volume and avg_vol:
        try:
            volume_ratio = float(volume) / float(avg_vol)
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    if volume_ratio >= 5.0:
        score += 25
    elif volume_ratio >= 3.0:
        score += 22
    elif volume_ratio >= 2.0:
        score += 17
    elif volume_ratio >= 1.5:
        score += 10
    elif volume_ratio >= 1.0:
        score += 4

    # ── Technical Alignment (30 pts) ──
    ta_points = 0
    rsi = technicals.get("rsi")
    sma_20 = technicals.get("sma_20")
    sma_50 = technicals.get("sma_50")
    macd = technicals.get("macd")
    macd_signal = technicals.get("macd_signal")
    macd_hist = technicals.get("macd_histogram")

    if price and sma_20:
        try:
            if float(price) > float(sma_20):
                ta_points += 6
        except (TypeError, ValueError):
            pass

    if price and sma_50:
        try:
            if float(price) > float(sma_50):
                ta_points += 6
        except (TypeError, ValueError):
            pass

    if macd is not None and macd_signal is not None:
        try:
            if float(macd) > float(macd_signal):
                ta_points += 7
        except (TypeError, ValueError):
            pass

    if macd_hist is not None:
        try:
            if float(macd_hist) > 0:
                ta_points += 5
        except (TypeError, ValueError):
            pass

    if rsi is not None:
        try:
            rsi_val = float(rsi)
            if 50 <= rsi_val <= 65:
                ta_points += 6
            elif 40 <= rsi_val < 50:
                ta_points += 4
            elif 65 < rsi_val <= 70:
                ta_points += 3
            elif rsi_val > 70:
                ta_points += 0
            elif 30 <= rsi_val < 40:
                ta_points += 2
        except (TypeError, ValueError):
            pass

    score += min(ta_points, 30)

    # ── Momentum Quality (20 pts) ──
    if change_pct is not None:
        try:
            change = float(change_pct)
            if 2 <= change <= 8 and volume_ratio >= 2.0:
                score += 20
            elif 1 <= change <= 15 and volume_ratio >= 3.0:
                score += 18
            elif 8 < change <= 15 and volume_ratio >= 2.0:
                score += 14
            elif 0 < change <= 2 and volume_ratio >= 2.0:
                score += 12
            elif change > 15 and volume_ratio >= 2.0:
                score += 8
            elif change > 15 and volume_ratio < 2.0:
                score += 2
            elif change <= 0:
                score += 0
        except (TypeError, ValueError):
            pass

    # ── Sentiment Tailwind (15 pts) ──
    if isinstance(sentiment, dict):
        bull_pct = sentiment.get("bull_pct") or sentiment.get("bullish_pct")
        if bull_pct is not None:
            try:
                bull = float(bull_pct)
                if bull >= 75:
                    score += 15
                elif bull >= 65:
                    score += 12
                elif bull >= 55:
                    score += 8
                elif bull >= 45:
                    score += 4
            except (TypeError, ValueError):
                pass

    # ── Setup Freshness (10 pts) ──
    if price and sma_20:
        try:
            distance_from_sma20 = (float(price) - float(sma_20)) / float(sma_20) * 100
            if 0 <= distance_from_sma20 <= 3:
                score += 10
            elif 3 < distance_from_sma20 <= 6:
                score += 7
            elif 6 < distance_from_sma20 <= 10:
                score += 4
            elif distance_from_sma20 > 10:
                score += 1
            elif distance_from_sma20 < 0:
                score += 2
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    return round(min(score, 100), 1)


def score_for_investments(ticker_data: dict) -> float:
    """
    Score a ticker for long-term investment potential.
    Weights: fundamentals (35%), valuation (25%), quality (20%), momentum (10%), insider (10%)
    Returns 0-100 score.
    """
    score = 0.0
    overview = ticker_data.get("overview", {})
    snapshot = ticker_data.get("snapshot", {})
    technicals = ticker_data.get("technicals", {})
    insider = ticker_data.get("insider_sentiment", {})
    earnings = ticker_data.get("earnings_history", [])
    details = ticker_data.get("details", {})

    if not isinstance(overview, dict):
        overview = {}

    # --- Fundamentals: Revenue Growth + Margins (35 pts max) ---
    rev_growth = overview.get("revenue_growth")
    if rev_growth is not None:
        try:
            rg = float(rev_growth)
            if rg > 0.40:
                score += 15
            elif rg > 0.25:
                score += 12
            elif rg > 0.15:
                score += 9
            elif rg > 0.05:
                score += 5
        except (TypeError, ValueError):
            pass

    ebitda_margin = overview.get("ebitda_margin")
    if ebitda_margin is not None:
        try:
            em = float(ebitda_margin)
            if em > 0.30:
                score += 10
            elif em > 0.20:
                score += 8
            elif em > 0.10:
                score += 5
            elif em > 0:
                score += 3
        except (TypeError, ValueError):
            pass

    profit_margin = overview.get("profit_margin")
    if profit_margin is not None:
        try:
            pm = float(profit_margin)
            if pm > 0.20:
                score += 10
            elif pm > 0.10:
                score += 7
            elif pm > 0:
                score += 3
        except (TypeError, ValueError):
            pass

    # --- Valuation (25 pts max) ---
    ps_ratio = overview.get("ps_ratio")
    if ps_ratio is not None:
        try:
            ps = float(ps_ratio)
            if ps < 2:
                score += 12
            elif ps < 5:
                score += 9
            elif ps < 10:
                score += 5
            elif ps < 20:
                score += 2
        except (TypeError, ValueError):
            pass

    pe_ratio = overview.get("pe_ratio")
    if pe_ratio is not None:
        try:
            pe = float(pe_ratio)
            if 0 < pe < 15:
                score += 13
            elif 15 <= pe < 25:
                score += 10
            elif 25 <= pe < 40:
                score += 6
            elif 40 <= pe < 60:
                score += 2
        except (TypeError, ValueError):
            pass

    # --- Quality: Earnings Consistency (20 pts max) ---
    if isinstance(earnings, list) and len(earnings) > 0:
        recent = earnings[:4]
        beats = sum(
            1 for e in recent
            if isinstance(e, dict) and e.get("surprise_pct") and e["surprise_pct"] > 0
        )
        score += beats * 5

    # --- Momentum (10 pts max) ---
    price = snapshot.get("price")
    sma_50 = technicals.get("sma_50")
    if price and sma_50:
        try:
            if float(price) > float(sma_50):
                score += 10
            else:
                score += 2
        except (TypeError, ValueError):
            pass

    # --- Insider Activity (10 pts max) ---
    if isinstance(insider, dict):
        mspr = insider.get("mspr")
        if mspr is not None:
            try:
                mspr = float(mspr)
                if mspr > 5:
                    score += 10
                elif mspr > 0:
                    score += 6
                elif mspr < -5:
                    score += 0
                else:
                    score += 3
            except (TypeError, ValueError):
                pass

    return round(min(score, 100), 1)


def score_for_squeeze(ticker_data: dict) -> float:
    """
    Score a ticker for short squeeze potential.
    Weights: short interest (30%), volume (25%), price action (20%), social (15%), technicals (10%)
    """
    score = 0.0
    overview = ticker_data.get("overview", {})
    snapshot = ticker_data.get("snapshot", {})
    technicals = ticker_data.get("technicals", {})
    sentiment = ticker_data.get("sentiment", {}) or ticker_data.get("stocktwits", {})
    details = ticker_data.get("details", {})

    if not isinstance(overview, dict):
        overview = {}

    # --- Short Interest (30 pts max) ---
    short_float = overview.get("short_float")
    if short_float is not None:
        try:
            sf = float(str(short_float).replace("%", ""))
            if sf > 30:
                score += 30
            elif sf > 20:
                score += 24
            elif sf > 15:
                score += 18
            elif sf > 10:
                score += 10
        except (TypeError, ValueError):
            pass

    # --- Volume Surge (25 pts max) ---
    volume = snapshot.get("volume")
    avg_vol = details.get("avg_volume") if details else None
    if volume and avg_vol:
        try:
            ratio = float(volume) / float(avg_vol)
            if ratio >= 5.0:
                score += 25
            elif ratio >= 3.0:
                score += 20
            elif ratio >= 2.0:
                score += 15
            elif ratio >= 1.5:
                score += 8
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    # --- Price Action (20 pts max) ---
    change_pct = snapshot.get("change_pct")
    if change_pct is not None:
        try:
            change = float(change_pct)
            if change > 10:
                score += 20
            elif change > 5:
                score += 15
            elif change > 2:
                score += 10
            elif change > 0:
                score += 5
        except (TypeError, ValueError):
            pass

    # --- Social Buzz (15 pts max) ---
    if isinstance(sentiment, dict):
        bull_pct = sentiment.get("bull_pct") or sentiment.get("bullish_pct")
        if bull_pct is not None:
            try:
                bull = float(bull_pct)
                if bull >= 80:
                    score += 15
                elif bull >= 65:
                    score += 10
                elif bull >= 50:
                    score += 5
            except (TypeError, ValueError):
                pass

    # --- Technicals (10 pts max) ---
    price = snapshot.get("price")
    sma_20 = technicals.get("sma_20")
    rsi = technicals.get("rsi")

    if price and sma_20:
        try:
            if float(price) > float(sma_20):
                score += 5
        except (TypeError, ValueError):
            pass

    if rsi is not None:
        try:
            rsi = float(rsi)
            if 50 <= rsi <= 75:
                score += 5
        except (TypeError, ValueError):
            pass

    return round(min(score, 100), 1)


def score_for_fundamentals(ticker_data: dict) -> float:
    """
    Score a ticker for improving fundamentals.
    Weights: revenue acceleration (30%), margin expansion (30%), earnings beats (20%), valuation (20%)
    """
    score = 0.0
    overview = ticker_data.get("overview", {})
    earnings = ticker_data.get("earnings_history", [])

    if not isinstance(overview, dict):
        overview = {}

    # --- Revenue Growth (30 pts max) ---
    rev_growth = overview.get("revenue_growth")
    if rev_growth is not None:
        try:
            rg = float(rev_growth)
            if rg > 0.50:
                score += 30
            elif rg > 0.30:
                score += 25
            elif rg > 0.20:
                score += 20
            elif rg > 0.10:
                score += 12
            elif rg > 0:
                score += 5
        except (TypeError, ValueError):
            pass

    # --- Margin Expansion (30 pts max) ---
    ebitda_margin = overview.get("ebitda_margin")
    profit_margin = overview.get("profit_margin")

    if ebitda_margin is not None:
        try:
            em = float(ebitda_margin)
            if em > 0.30:
                score += 15
            elif em > 0.15:
                score += 12
            elif em > 0.05:
                score += 8
            elif em > 0:
                score += 5
        except (TypeError, ValueError):
            pass

    if profit_margin is not None:
        try:
            pm = float(profit_margin)
            if pm > 0.20:
                score += 15
            elif pm > 0.10:
                score += 12
            elif pm > 0:
                score += 8
        except (TypeError, ValueError):
            pass

    # --- Earnings Beats (20 pts max) ---
    if isinstance(earnings, list) and len(earnings) > 0:
        recent = earnings[:4]
        beats = sum(
            1 for e in recent
            if isinstance(e, dict) and e.get("surprise_pct") and e["surprise_pct"] > 0
        )
        score += beats * 5

    # --- Valuation (20 pts max) ---
    ps_ratio = overview.get("ps_ratio")
    if ps_ratio is not None:
        try:
            ps = float(ps_ratio)
            if ps < 3:
                score += 20
            elif ps < 6:
                score += 14
            elif ps < 10:
                score += 8
            elif ps < 15:
                score += 3
        except (TypeError, ValueError):
            pass

    return round(min(score, 100), 1)


def score_for_bearish(ticker_data: dict) -> float:
    """
    Score a ticker for bearish/breakdown potential.
    Higher score = more bearish setup.
    """
    score = 0.0
    snapshot = ticker_data.get("snapshot", {})
    technicals = ticker_data.get("technicals", {})
    details = ticker_data.get("details", {})

    change_pct = snapshot.get("change_pct")
    if change_pct is not None:
        try:
            change = float(change_pct)
            if change < 0:
                score += min(abs(change) * 2, 25)
        except (TypeError, ValueError):
            pass

    price = snapshot.get("price")
    sma_20 = technicals.get("sma_20")
    sma_50 = technicals.get("sma_50")

    if price and sma_20:
        try:
            if float(price) < float(sma_20):
                score += 15
        except (TypeError, ValueError):
            pass

    if price and sma_50:
        try:
            if float(price) < float(sma_50):
                score += 15
        except (TypeError, ValueError):
            pass

    rsi = technicals.get("rsi")
    if rsi is not None:
        try:
            rsi = float(rsi)
            if rsi > 80:
                score += 20
            elif rsi > 70:
                score += 12
        except (TypeError, ValueError):
            pass

    macd = technicals.get("macd")
    macd_signal = technicals.get("macd_signal")
    if macd is not None and macd_signal is not None:
        try:
            if float(macd) < float(macd_signal):
                score += 15
        except (TypeError, ValueError):
            pass

    volume = snapshot.get("volume")
    avg_vol = details.get("avg_volume") if details else None
    if volume and avg_vol and change_pct:
        try:
            if float(change_pct) < 0 and float(volume) / float(avg_vol) > 2.0:
                score += 10
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    return round(min(score, 100), 1)


SCORING_FUNCTIONS = {
    "market_scan": score_for_trades,
    "trades": score_for_trades,
    "investments": score_for_investments,
    "squeeze": score_for_squeeze,
    "fundamentals_scan": score_for_fundamentals,
    "bearish": score_for_bearish,
    "small_cap_spec": score_for_trades,
    "asymmetric": score_for_investments,
    "volume_spikes": score_for_trades,
    "social_momentum": score_for_trades,
}


def rank_candidates(candidates: dict, category: str, top_n: int = 12) -> list:
    """
    Takes a dict of {ticker: raw_data}, scores each ticker
    for the given category, and returns the top N ranked by score.

    Returns list of (ticker, score, raw_data) tuples, sorted descending.
    """
    scoring_fn = SCORING_FUNCTIONS.get(category, score_for_trades)

    scored = []
    for ticker, data in candidates.items():
        if not isinstance(data, dict):
            continue
        try:
            s = scoring_fn(data)
            scored.append((ticker, s, data))
        except Exception as e:
            print(f"Scoring error for {ticker}: {e}")
            continue

    scored.sort(key=lambda x: x[1], reverse=True)

    return scored[:top_n]
