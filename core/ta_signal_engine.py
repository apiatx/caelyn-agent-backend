"""
TA Signal Engine — deterministic signal detection and trade plan generation.

Takes OHLCV bars and returns:
  - indicators: RSI, MACD, SMA20/50/200, EMA20/50
  - signals: list of discrete detected signals with name/direction/strength/evidence
  - ta_score: 0-100 from signal stack
  - setup_type: breakout / trend_continuation / mean_reversion / breakdown_short
  - trade_plan: deterministic entry/stop/targets/R:R from ATR
"""

from data.ta_utils import (
    compute_rsi,
    compute_sma,
    compute_ema,
    compute_ema_series,
    compute_macd,
    compute_technicals_from_bars,
)


def compute_atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float | None:
    if len(highs) < period + 1 or len(lows) < period + 1 or len(closes) < period + 1:
        return None
    true_ranges = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        true_ranges.append(tr)
    if len(true_ranges) < period:
        return None
    return sum(true_ranges[-period:]) / period


def _detect_signals(
    closes: list[float],
    highs: list[float],
    lows: list[float],
    volumes: list[int],
    indicators: dict,
) -> list[dict]:
    signals = []
    price = closes[-1]
    rsi = indicators.get("rsi")
    sma_20 = indicators.get("sma_20")
    sma_50 = indicators.get("sma_50")
    sma_200 = indicators.get("sma_200")
    ema_20 = indicators.get("ema_20")
    ema_50 = indicators.get("ema_50")
    macd = indicators.get("macd")
    macd_signal = indicators.get("macd_signal")
    macd_hist = indicators.get("macd_histogram")
    avg_vol = indicators.get("avg_volume") or 1
    current_vol = volumes[-1] if volumes else 0
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0

    if sma_50 and price > sma_50:
        dist_pct = round((price - sma_50) / sma_50 * 100, 1)
        sma_50_vals = [compute_sma(closes[:i+1], 50) for i in range(max(0, len(closes)-5), len(closes))]
        sma_50_vals = [v for v in sma_50_vals if v is not None]
        slope_rising = len(sma_50_vals) >= 2 and sma_50_vals[-1] > sma_50_vals[0]
        strength = min(80, 40 + int(dist_pct * 3)) if slope_rising else min(60, 30 + int(dist_pct * 2))
        signals.append({
            "name": "price_above_sma50",
            "direction": "bullish",
            "strength": strength,
            "evidence": f"Price ${price:.2f} is {dist_pct}% above SMA50 ${sma_50:.2f}" + (", SMA50 slope rising" if slope_rising else ""),
        })

    if sma_50 and sma_200 and sma_50 > sma_200:
        gap_pct = round((sma_50 - sma_200) / sma_200 * 100, 1)
        sma_50_prev = compute_sma(closes[:-5], 50) if len(closes) > 55 else None
        sma_200_prev = compute_sma(closes[:-5], 200) if len(closes) > 205 else None
        recent_cross = sma_50_prev is not None and sma_200_prev is not None and sma_50_prev <= sma_200_prev
        strength = 75 if recent_cross else min(65, 40 + int(gap_pct * 3))
        signals.append({
            "name": "sma50_above_sma200",
            "direction": "bullish",
            "strength": strength,
            "evidence": f"SMA50 ${sma_50:.2f} > SMA200 ${sma_200:.2f} (gap {gap_pct}%)" + (" — recent golden cross" if recent_cross else ""),
        })

    if macd is not None and macd_signal is not None and macd > macd_signal and macd_hist is not None and macd_hist > 0:
        macd_series = []
        for i in range(max(0, len(closes) - 5), len(closes)):
            md = compute_macd(closes[:i+1])
            if md.get("macd") is not None and md.get("macd_signal") is not None:
                macd_series.append(md["macd"] - md["macd_signal"])
        recent_cross = len(macd_series) >= 2 and macd_series[-1] > 0 and macd_series[0] <= 0
        hist_improving = len(macd_series) >= 2 and macd_series[-1] > macd_series[0]
        strength = 70 if recent_cross else (60 if hist_improving else 45)
        signals.append({
            "name": "macd_bull_cross",
            "direction": "bullish",
            "strength": strength,
            "evidence": f"MACD {macd:.4f} > Signal {macd_signal:.4f}, histogram {macd_hist:.4f}" + (" — recent cross" if recent_cross else ""),
        })

    if rsi is not None and 50 <= rsi <= 70:
        prev_rsi = compute_rsi(closes[:-1]) if len(closes) > 15 else None
        rising = prev_rsi is not None and rsi > prev_rsi
        strength = min(70, 40 + int((rsi - 50) * 1.5)) if rising else min(55, 30 + int((rsi - 50)))
        signals.append({
            "name": "rsi_bull_zone",
            "direction": "bullish",
            "strength": strength,
            "evidence": f"RSI={rsi:.1f}" + (f" rising from {prev_rsi:.1f}" if rising and prev_rsi else "") + " (50-70 bull zone)",
        })

    if len(highs) >= 20:
        pivot_high = max(highs[-20:])
        if price > pivot_high and price > pivot_high * 1.001:
            pct_above = round((price - pivot_high) / pivot_high * 100, 1)
            strength = min(85, 55 + int(pct_above * 5)) if vol_ratio >= 1.5 else min(65, 45 + int(pct_above * 3))
            signals.append({
                "name": "breakout_pivot",
                "direction": "bullish",
                "strength": strength,
                "evidence": f"Close ${price:.2f} broke 20-day high ${pivot_high:.2f} (+{pct_above}%)" + (f", vol {vol_ratio:.1f}x avg" if vol_ratio >= 1.5 else ""),
            })

    if vol_ratio >= 2.0:
        strength = min(80, 50 + int((vol_ratio - 2) * 10))
        signals.append({
            "name": "volume_spike_2x",
            "direction": "bullish",
            "strength": strength,
            "evidence": f"Volume {current_vol:,.0f} is {vol_ratio:.1f}x 20d avg ({avg_vol:,.0f})",
        })
    elif vol_ratio >= 1.5:
        signals.append({
            "name": "volume_expansion",
            "direction": "bullish",
            "strength": min(50, 30 + int((vol_ratio - 1.5) * 40)),
            "evidence": f"Volume {current_vol:,.0f} is {vol_ratio:.1f}x 20d avg",
        })

    if ema_20 and ema_50 and ema_20 > ema_50:
        ema_20_prev = compute_ema(closes[:-1], 20)
        ema_50_prev = compute_ema(closes[:-1], 50)
        recent_cross = ema_20_prev is not None and ema_50_prev is not None and ema_20_prev <= ema_50_prev
        if recent_cross:
            signals.append({
                "name": "ema20_cross_ema50",
                "direction": "bullish",
                "strength": 65,
                "evidence": f"EMA20 ${ema_20:.2f} just crossed above EMA50 ${ema_50:.2f}",
            })

    if sma_200 and price > sma_200 and sma_50 and sma_50 > sma_200:
        if len(highs) >= 52:
            high_52w = max(highs[-min(252, len(highs)):])
            if price >= high_52w * 0.95:
                signals.append({
                    "name": "stage2_uptrend",
                    "direction": "bullish",
                    "strength": 70,
                    "evidence": f"Stage 2: price above SMA50/200, near 52w high ${high_52w:.2f}",
                })

    if len(closes) >= 20:
        recent_range_h = max(highs[-20:])
        recent_range_l = min(lows[-20:])
        range_width = (recent_range_h - recent_range_l) / recent_range_l if recent_range_l > 0 else 0
        if 0.03 <= range_width <= 0.15 and price > recent_range_h * 0.99:
            signals.append({
                "name": "range_breakout",
                "direction": "bullish",
                "strength": 60,
                "evidence": f"Breaking out of {range_width*100:.1f}% range (${recent_range_l:.2f}-${recent_range_h:.2f})",
            })

    if len(closes) >= 40:
        recent_range = max(highs[-20:]) - min(lows[-20:])
        prior_range = max(highs[-40:-20]) - min(lows[-40:-20])
        if prior_range > 0 and recent_range < prior_range * 0.5:
            signals.append({
                "name": "volatility_contraction",
                "direction": "bullish",
                "strength": 45,
                "evidence": f"20d range contracted to {recent_range/prior_range*100:.0f}% of prior range (squeeze setup)",
            })

    if sma_50 and price < sma_50:
        dist_pct = round((sma_50 - price) / sma_50 * 100, 1)
        signals.append({
            "name": "price_below_sma50",
            "direction": "bearish",
            "strength": min(70, 30 + int(dist_pct * 3)),
            "evidence": f"Price ${price:.2f} is {dist_pct}% below SMA50 ${sma_50:.2f}",
        })

    if sma_50 and sma_200 and sma_50 < sma_200:
        gap_pct = round((sma_200 - sma_50) / sma_200 * 100, 1)
        signals.append({
            "name": "sma50_below_sma200",
            "direction": "bearish",
            "strength": min(70, 35 + int(gap_pct * 3)),
            "evidence": f"SMA50 ${sma_50:.2f} < SMA200 ${sma_200:.2f} (death cross territory, gap {gap_pct}%)",
        })

    if macd is not None and macd_signal is not None and macd < macd_signal and macd_hist is not None and macd_hist < 0:
        macd_series = []
        for i in range(max(0, len(closes) - 5), len(closes)):
            md = compute_macd(closes[:i+1])
            if md.get("macd") is not None and md.get("macd_signal") is not None:
                macd_series.append(md["macd"] - md["macd_signal"])
        recent_cross = len(macd_series) >= 2 and macd_series[-1] < 0 and macd_series[0] >= 0
        signals.append({
            "name": "macd_bear_cross",
            "direction": "bearish",
            "strength": 65 if recent_cross else 45,
            "evidence": f"MACD {macd:.4f} < Signal {macd_signal:.4f}, histogram {macd_hist:.4f}" + (" — recent bearish cross" if recent_cross else ""),
        })

    if len(lows) >= 20:
        pivot_low = min(lows[-20:])
        if price < pivot_low:
            pct_below = round((pivot_low - price) / pivot_low * 100, 1)
            signals.append({
                "name": "breakdown_support",
                "direction": "bearish",
                "strength": min(80, 50 + int(pct_below * 5)),
                "evidence": f"Close ${price:.2f} broke 20-day low ${pivot_low:.2f} (-{pct_below}%)",
            })

    return signals


def _compute_ta_score(signals: list[dict]) -> int:
    score = 50
    bullish_sum = sum(s["strength"] for s in signals if s["direction"] == "bullish")
    bearish_sum = sum(s["strength"] for s in signals if s["direction"] == "bearish")

    bull_count = sum(1 for s in signals if s["direction"] == "bullish")
    bear_count = sum(1 for s in signals if s["direction"] == "bearish")

    score += int(bullish_sum * 0.12)
    score -= int(bearish_sum * 0.12)

    if bull_count >= 3:
        score += 10
    if bull_count >= 5:
        score += 5

    if bear_count >= 3:
        score -= 10

    return max(0, min(100, score))


def _classify_setup(signals: list[dict], price: float, indicators: dict) -> str:
    names = {s["name"] for s in signals if s["direction"] == "bullish"}
    bearish_names = {s["name"] for s in signals if s["direction"] == "bearish"}

    if "breakdown_support" in bearish_names or "price_below_sma50" in bearish_names:
        if "macd_bear_cross" in bearish_names or "sma50_below_sma200" in bearish_names:
            return "breakdown_short"

    if "breakout_pivot" in names or "range_breakout" in names:
        return "breakout"
    if "stage2_uptrend" in names or "sma50_above_sma200" in names:
        return "trend_continuation"
    if "rsi_bull_zone" in names and "volume_expansion" in names:
        return "momentum"
    return "technical_setup"


def _build_trade_plan(
    price: float,
    highs: list[float],
    lows: list[float],
    closes: list[float],
    atr: float,
    setup_type: str,
    is_short: bool = False,
) -> dict:
    if is_short:
        recent_high = max(highs[-20:]) if len(highs) >= 20 else price * 1.05
        entry = price
        stop = round(entry + 1.5 * atr, 2)
        risk = stop - entry
        target_1 = round(entry - risk * 1.0, 2)
        target_2 = round(entry - risk * 2.0, 2)
        rr = round(abs(entry - target_1) / risk, 1) if risk > 0 else 0
    else:
        if setup_type == "breakout":
            pivot = max(highs[-20:]) if len(highs) >= 20 else price
            entry = round(pivot + 0.25 * atr, 2) if price >= pivot else price
        else:
            entry = price

        stop = round(entry - 1.5 * atr, 2)
        risk = entry - stop
        if risk <= 0:
            risk = entry * 0.03
            stop = round(entry - risk, 2)
        target_1 = round(entry + risk * 1.0, 2)
        target_2 = round(entry + risk * 2.0, 2)
        rr = round((target_1 - entry) / risk, 1) if risk > 0 else 0

    if setup_type == "breakout" or setup_type == "momentum":
        timeframe = "1–3 days"
    elif setup_type == "breakdown_short":
        timeframe = "1–5 days"
    else:
        timeframe = "2–6 weeks"

    return {
        "entry": f"${entry:.2f}",
        "stop": f"${stop:.2f}",
        "targets": [f"${target_1:.2f}", f"${target_2:.2f}"],
        "risk_reward": f"{rr}:1",
        "timeframe": timeframe,
        "atr": round(atr, 4),
    }


def analyze_bars(
    bars: list[dict],
    ticker: str = "",
    finviz_data: dict | None = None,
    source_list: list[str] | None = None,
) -> dict | None:
    if not bars or len(bars) < 20:
        return None

    closes = [b["c"] for b in bars if b.get("c") is not None]
    highs = [b["h"] for b in bars if b.get("h") is not None]
    lows = [b["l"] for b in bars if b.get("l") is not None]
    volumes = [b.get("v", 0) for b in bars]

    if len(closes) < 20:
        return None

    ta = compute_technicals_from_bars(bars)
    if not ta:
        return None

    price = closes[-1]
    current_vol = volumes[-1] if volumes else 0
    avg_vol = ta.get("avg_volume") or 1
    vol_ratio = current_vol / avg_vol if avg_vol > 0 else 0

    _ema_20 = compute_ema(closes, 20)
    ema_20 = round(_ema_20, 2) if _ema_20 is not None else None
    _ema_50 = compute_ema(closes, 50) if len(closes) >= 50 else None
    ema_50 = round(_ema_50, 2) if _ema_50 is not None else None

    indicators = {
        **ta,
        "ema_20": ema_20,
        "ema_50": ema_50,
        "avg_volume": ta.get("avg_volume"),
        "current_volume": current_vol,
        "volume_ratio": round(vol_ratio, 2),
    }

    signals = _detect_signals(closes, highs, lows, volumes, indicators)

    if not signals:
        return None

    ta_score = _compute_ta_score(signals)
    setup_type = _classify_setup(signals, price, indicators)

    atr = compute_atr(highs, lows, closes) or (price * 0.03)

    is_short = setup_type == "breakdown_short"
    trade_plan = _build_trade_plan(price, highs, lows, closes, atr, setup_type, is_short=is_short)

    bull_signals = [s for s in signals if s["direction"] == "bullish"]
    bear_signals = [s for s in signals if s["direction"] == "bearish"]
    signal_names = [s["name"] for s in (bear_signals if is_short else bull_signals)]

    source_list = source_list or []
    finviz_data = finviz_data or {}

    volume_confirmed = vol_ratio >= 1.5 and avg_vol > 1 and current_vol > 0
    ta_confirmed = len(bull_signals) >= 3 if not is_short else len(bear_signals) >= 3
    catalyst_confirmed = "new_high" in source_list or "unusual_vol" in source_list

    pattern = _pick_pattern_label(signals, setup_type)

    confidence = min(95, ta_score + (10 if volume_confirmed else 0) + (5 if catalyst_confirmed else 0))

    if is_short:
        action = "Sell"
    elif confidence >= 80:
        action = "Strong Buy"
    elif confidence >= 60:
        action = "Buy"
    else:
        action = "Hold"

    indicator_signals = _format_indicator_signals(signals, indicators, vol_ratio)

    tv_sym = ticker
    exchange = finviz_data.get("exchange", "") if finviz_data else ""
    tv_url = f"https://www.tradingview.com/chart/?symbol={exchange + ':' if exchange else ''}{tv_sym}"

    catalyst_check = None
    if "new_high" in source_list:
        catalyst_check = "52-week high breakout (Finviz new highs screen)"
    elif "unusual_vol" in source_list:
        catalyst_check = "Unusual volume detected (Finviz volume screen)"
    elif "breakout" in source_list:
        catalyst_check = "Technical breakout candidate (Finviz breakout screen)"

    return {
        "ticker": ticker,
        "name": finviz_data.get("company", ""),
        "exchange": exchange,
        "direction": "short" if is_short else "long",
        "action": action,
        "confidence_score": confidence,
        "technical_score": ta_score,
        "setup_type": setup_type,
        "pattern": pattern,
        "signals_stacking": signal_names,
        "indicator_signals": indicator_signals,
        "ta_signals": signals,
        "entry": trade_plan["entry"],
        "stop": trade_plan["stop"],
        "targets": trade_plan["targets"],
        "risk_reward": trade_plan["risk_reward"],
        "timeframe": trade_plan["timeframe"],
        "atr": trade_plan["atr"],
        "confirmations": {
            "ta": ta_confirmed,
            "volume": volume_confirmed,
            "catalyst": catalyst_confirmed,
            "fa": True,
        },
        "tradingview_url": tv_url,
        "tv_url": tv_url,
        "price": f"${price:.2f}",
        "change": finviz_data.get("change", ""),
        "market_cap": finviz_data.get("market_cap", ""),
        "rsi": indicators.get("rsi"),
        "volume_ratio": round(vol_ratio, 1),
        "catalyst_check": catalyst_check,
        "risk": "",
        "data_gaps": [],
        "is_bearish": is_short,
    }


def _format_indicator_signals(signals: list[dict], indicators: dict, vol_ratio: float) -> list[str]:
    formatted = []
    name_map = {
        "price_above_sma50": lambda: f"Price > SMA50",
        "sma50_above_sma200": lambda: f"SMA50 > SMA200 (Golden Cross)",
        "macd_bull_cross": lambda: f"MACD bull cross",
        "rsi_bull_zone": lambda: f"RSI {indicators.get('rsi', 0):.0f} (bull zone)" if indicators.get("rsi") else "RSI bull zone",
        "breakout_pivot": lambda: "20D pivot breakout",
        "volume_spike_2x": lambda: f"RelVol +{int((vol_ratio - 1) * 100)}%",
        "volume_expansion": lambda: f"RelVol +{int((vol_ratio - 1) * 100)}%",
        "ema20_cross_ema50": lambda: "EMA20 > EMA50 cross",
        "stage2_uptrend": lambda: "Stage 2 uptrend",
        "range_breakout": lambda: "Range breakout",
        "volatility_contraction": lambda: "Volatility squeeze",
        "price_below_sma50": lambda: "Price < SMA50",
        "sma50_below_sma200": lambda: "SMA50 < SMA200 (Death Cross)",
        "macd_bear_cross": lambda: "MACD bear cross",
        "breakdown_support": lambda: "Support breakdown",
    }
    for s in signals:
        name = s["name"]
        if name in name_map:
            formatted.append(name_map[name]())
        else:
            formatted.append(s["name"].replace("_", " ").title())
    return formatted


def _pick_pattern_label(signals: list[dict], setup_type: str) -> str:
    names = {s["name"] for s in signals}
    if "stage2_uptrend" in names:
        return "Stage 2 breakout"
    if "breakout_pivot" in names:
        return "Pivot breakout"
    if "range_breakout" in names:
        return "Range breakout"
    if "ema20_cross_ema50" in names:
        return "EMA cross"
    if "macd_bull_cross" in names:
        return "MACD bullish crossover"
    if "breakdown_support" in names:
        return "Support breakdown"
    if "macd_bear_cross" in names:
        return "MACD bearish crossover"
    if setup_type == "momentum":
        return "Momentum expansion"
    if setup_type == "trend_continuation":
        return "Trend continuation"
    return "Technical setup"
