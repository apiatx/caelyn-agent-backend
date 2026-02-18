"""
Market Regime Detector.

Determines the current macro regime from real-time market signals:
  - SPY vs 200DMA (equity trend)
  - VIX level (volatility / fear)
  - 10Y yield trend (rates)
  - DXY trend (dollar strength)
  - BTC trend (risk appetite proxy)

Returns one of: risk_on, risk_off, inflationary, neutral
with a confidence score 0-1.

Cached for 5 minutes to avoid redundant computation.
"""

import time as _time

_regime_cache = {"result": None, "expires": 0}
REGIME_CACHE_TTL = 300


def detect_market_regime(market_data_service) -> dict:
    now = _time.time()
    if _regime_cache["result"] and now < _regime_cache["expires"]:
        return _regime_cache["result"]

    signals = _gather_regime_signals(market_data_service)
    result = _classify_regime(signals)
    result["signals"] = signals

    _regime_cache["result"] = result
    _regime_cache["expires"] = now + REGIME_CACHE_TTL
    return result


def _gather_regime_signals(svc) -> dict:
    signals = {
        "spy_above_200dma": None,
        "vix_level": None,
        "yield_10y_rising": None,
        "dxy_rising": None,
        "btc_above_200dma": None,
    }

    try:
        spy_bars = svc.finnhub.get_stock_candles("SPY", days=250)
        if spy_bars and len(spy_bars) >= 200:
            closes = [b["c"] for b in spy_bars]
            sma_200 = sum(closes[-200:]) / 200
            current = closes[-1]
            signals["spy_above_200dma"] = current > sma_200
    except Exception as e:
        print(f"[REGIME] SPY signal error: {e}")

    try:
        vix_data = svc.fred.get_vix()
        if isinstance(vix_data, dict) and "current" in vix_data:
            signals["vix_level"] = float(vix_data["current"])
        elif isinstance(vix_data, dict) and "value" in vix_data:
            signals["vix_level"] = float(vix_data["value"])
    except Exception as e:
        print(f"[REGIME] VIX signal error: {e}")

    try:
        fred_macro = svc.fred.get_full_macro_dashboard()
        if isinstance(fred_macro, dict):
            yield_data = fred_macro.get("yield_curve", {})
            if isinstance(yield_data, dict):
                y10_current = yield_data.get("10y_current")
                y10_prev = yield_data.get("10y_3m_ago")
                if y10_current is not None and y10_prev is not None:
                    signals["yield_10y_rising"] = float(y10_current) > float(y10_prev)
    except Exception as e:
        print(f"[REGIME] 10Y yield signal error: {e}")

    try:
        dxy_bars = svc.finnhub.get_stock_candles("UUP", days=60)
        if dxy_bars and len(dxy_bars) >= 20:
            closes = [b["c"] for b in dxy_bars]
            sma_20 = sum(closes[-20:]) / 20
            signals["dxy_rising"] = closes[-1] > sma_20
    except Exception as e:
        print(f"[REGIME] DXY signal error: {e}")

    try:
        btc_bars = svc.finnhub.get_stock_candles("IBIT", days=250)
        if btc_bars and len(btc_bars) >= 50:
            closes = [b["c"] for b in btc_bars]
            sma_len = min(200, len(closes))
            sma = sum(closes[-sma_len:]) / sma_len
            signals["btc_above_200dma"] = closes[-1] > sma
    except Exception as e:
        print(f"[REGIME] BTC signal error: {e}")

    return signals


def _classify_regime(signals: dict) -> dict:
    spy_up = signals.get("spy_above_200dma")
    vix = signals.get("vix_level")
    yield_rising = signals.get("yield_10y_rising")
    dxy_rising = signals.get("dxy_rising")
    btc_up = signals.get("btc_above_200dma")

    risk_on_score = 0
    risk_off_score = 0
    inflationary_score = 0
    total_signals = 0

    if spy_up is not None:
        total_signals += 1
        if spy_up:
            risk_on_score += 1
        else:
            risk_off_score += 1

    if vix is not None:
        total_signals += 1
        if vix < 18:
            risk_on_score += 1
        elif vix > 25:
            risk_off_score += 1

    if yield_rising is not None and dxy_rising is not None:
        total_signals += 1
        if yield_rising and dxy_rising:
            inflationary_score += 1
        elif not yield_rising and not dxy_rising:
            risk_on_score += 0.5

    if btc_up is not None:
        total_signals += 1
        if btc_up:
            risk_on_score += 0.5
        else:
            risk_off_score += 0.3

    if total_signals == 0:
        return {"regime": "neutral", "confidence": 0.0}

    scores = {
        "risk_on": risk_on_score,
        "risk_off": risk_off_score,
        "inflationary": inflationary_score,
    }
    best = max(scores, key=scores.get)
    best_score = scores[best]

    if best_score < 0.5:
        return {"regime": "neutral", "confidence": 0.3}

    confidence = min(1.0, round(best_score / total_signals + 0.2, 2))

    return {"regime": best, "confidence": confidence}
