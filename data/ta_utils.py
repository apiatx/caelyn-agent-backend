"""
Local Technical Analysis Computation Utility.

Computes RSI, MACD, SMA, EMA from raw OHLC bar data.
No external API calls — pure math on price arrays.
Used by Finnhub candles (primary) and Polygon bars (fallback).
"""


def compute_ema(data: list[float], period: int) -> float | None:
    if not data or len(data) < period:
        return None
    multiplier = 2 / (period + 1)
    ema_val = sum(data[:period]) / period
    for price in data[period:]:
        ema_val = (price - ema_val) * multiplier + ema_val
    return ema_val


def compute_ema_series(data: list[float], period: int) -> list[float]:
    if not data or len(data) < period:
        return []
    multiplier = 2 / (period + 1)
    ema_val = sum(data[:period]) / period
    result = [ema_val]
    for price in data[period:]:
        ema_val = (price - ema_val) * multiplier + ema_val
        result.append(ema_val)
    return result


def compute_sma(data: list[float], period: int) -> float | None:
    if not data or len(data) < period:
        return None
    return sum(data[-period:]) / period


def compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if not closes or len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = deltas[-(period):]
    gains = [d if d > 0 else 0 for d in recent]
    losses = [-d if d < 0 else 0 for d in recent]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - (100 / (1 + rs)), 2)


def compute_macd(closes: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> dict:
    result = {"macd": None, "macd_signal": None, "macd_histogram": None}
    if not closes or len(closes) < slow + signal:
        return result

    ema_fast = compute_ema(closes, fast)
    ema_slow = compute_ema(closes, slow)
    if ema_fast is None or ema_slow is None:
        return result

    macd_val = round(ema_fast - ema_slow, 4)

    macd_series = []
    for i in range(slow, len(closes)):
        e_fast = compute_ema(closes[:i + 1], fast)
        e_slow = compute_ema(closes[:i + 1], slow)
        if e_fast is not None and e_slow is not None:
            macd_series.append(e_fast - e_slow)

    macd_signal_val = None
    macd_hist = None
    if len(macd_series) >= signal:
        macd_signal_val = round(compute_ema(macd_series, signal) or 0, 4)
        macd_hist = round(macd_val - macd_signal_val, 4)

    return {
        "macd": macd_val,
        "macd_signal": macd_signal_val,
        "macd_histogram": macd_hist,
    }


def compute_technicals_from_bars(bars: list[dict]) -> dict:
    """
    Compute full technical indicator set from OHLCV bar data.
    Each bar: {"o": open, "h": high, "l": low, "c": close, "v": volume, "t": timestamp}
    Returns dict with RSI, SMAs, MACD, EMA, avg_volume.
    """
    if not bars or len(bars) < 20:
        return {}

    closes = [b["c"] for b in bars if b.get("c") is not None]
    volumes = [b.get("v", 0) for b in bars]

    if len(closes) < 20:
        return {}

    rsi = compute_rsi(closes)
    sma_20 = round(compute_sma(closes, 20), 2) if compute_sma(closes, 20) else None
    sma_50 = round(compute_sma(closes, 50), 2) if len(closes) >= 50 and compute_sma(closes, 50) else None
    sma_200 = round(compute_sma(closes, 200), 2) if len(closes) >= 200 and compute_sma(closes, 200) else None

    ema_9 = round(compute_ema(closes, 9), 2) if compute_ema(closes, 9) else None
    ema_21 = round(compute_ema(closes, 21), 2) if compute_ema(closes, 21) else None

    macd_data = compute_macd(closes)

    avg_volume = round(sum(volumes[-30:]) / min(len(volumes), 30)) if volumes else None

    return {
        "rsi": rsi,
        "sma_20": sma_20,
        "sma_50": sma_50,
        "sma_200": sma_200,
        "ema_9": ema_9,
        "ema_21": ema_21,
        "macd": macd_data.get("macd"),
        "macd_signal": macd_data.get("macd_signal"),
        "macd_histogram": macd_data.get("macd_histogram"),
        "avg_volume": avg_volume,
    }
