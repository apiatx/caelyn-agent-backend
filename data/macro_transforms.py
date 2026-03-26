"""
Transform MacroProvider rich responses into the flat shapes
the Macro Terminal frontend expects.

Each function takes the raw dict from MacroProvider and returns
the exact JSON shape the Express proxy / React frontend consumes.
"""
from __future__ import annotations

from typing import Any


def _r(v: Any, n: int = 2) -> float | None:
    """Round a value safely."""
    if v is None:
        return None
    try:
        return round(float(v), n)
    except (TypeError, ValueError):
        return None


def _status(signal: str | None) -> str:
    """Map internal signal names to frontend status vocabulary."""
    mapping = {
        "bearish": "negative",
        "bullish": "positive",
        "neutral": "neutral",
        "elevated": "elevated",
        "high_fear": "high",
        "complacency": "low",
        "normal": "neutral",
        "low_vol": "low",
        "contraction": "negative",
        "expansion": "positive",
        "tight": "elevated",
        "softening": "neutral",
        "weak": "negative",
        "declining": "positive",
        "sticky": "elevated",
        "inverted": "inverted",
    }
    return mapping.get(signal or "", "neutral")


def _month_label(date_str: str | None) -> str:
    """Convert '2025-03-01' to 'Mar' (3-letter month)."""
    if not date_str:
        return ""
    try:
        parts = date_str.split("-")
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        return months[int(parts[1]) - 1]
    except (IndexError, ValueError):
        return date_str[:7] if date_str else ""


def _month_year_label(date_str: str | None) -> str:
    """Convert '2025-03-01' to 'Mar 2025'."""
    if not date_str:
        return ""
    try:
        parts = date_str.split("-")
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        return f"{months[int(parts[1]) - 1]} {parts[0]}"
    except (IndexError, ValueError):
        return date_str


# ── Dashboard ────────────────────────────────────────────────────────

def transform_dashboard(raw: dict) -> dict:
    """
    Input:  MacroProvider.get_dashboard() result
    Output: Full dashboard with all sections + frontend-expected keys
            (benchmark_etfs, vix, yield_snapshot, indicators) at top level.
    """
    # ── Frontend-required keys ────────────────────────────────────────

    # benchmark_etfs — already close to the right shape
    benchmark_etfs = []
    for etf in raw.get("benchmark_etfs", []):
        benchmark_etfs.append({
            "ticker": etf.get("ticker"),
            "price": _r(etf.get("price")),
            "change_pct": _r(etf.get("change_pct")),
            "pct_from_52w_high": _r(etf.get("pct_from_52w_high"), 1),
        })

    # vix — flat {current, change_pct}
    vix_raw = raw.get("vix", {})
    vix = {
        "current": _r(vix_raw.get("current")),
        "change_pct": _r(vix_raw.get("change_pct")),
    }

    # yield_snapshot — flat {2Y, 5Y, 10Y, 30Y}
    rates = raw.get("rates_and_yields", {})
    yield_snapshot = {
        "2Y": _r(rates.get("us_2y")),
        "5Y": _r(rates.get("us_5y")),
        "10Y": _r(rates.get("us_10y")),
        "30Y": _r(rates.get("us_30y")),
    }

    # indicators — summary cards with {name, value, status}
    indicators = []

    fed = raw.get("fed", {})
    if fed.get("funds_rate_range"):
        indicators.append({
            "name": "Fed Funds Rate",
            "value": fed["funds_rate_range"],
            "status": "elevated" if (_r(fed.get("funds_rate")) or 0) > 4 else "neutral",
        })

    infl = raw.get("inflation", {})
    if infl.get("cpi_yoy") is not None:
        indicators.append({
            "name": "CPI YoY",
            "value": f"{infl['cpi_yoy']}%",
            "status": _status(infl.get("trend")),
        })
    if infl.get("core_pce_yoy") is not None:
        indicators.append({
            "name": "Core PCE",
            "value": f"{infl['core_pce_yoy']}%",
            "status": "elevated" if (infl["core_pce_yoy"] or 0) > 2.5 else "neutral",
        })

    labor = raw.get("labor", {})
    if labor.get("unemployment_rate") is not None:
        indicators.append({
            "name": "Unemployment",
            "value": f"{labor['unemployment_rate']}%",
            "status": "positive" if (labor["unemployment_rate"] or 5) < 4.5 else "neutral",
        })

    gdp = raw.get("gdp", {})
    if gdp.get("gdp_now_estimate") is not None:
        indicators.append({
            "name": "GDP Growth",
            "value": f"{gdp['gdp_now_estimate']}%",
            "status": "positive" if (gdp["gdp_now_estimate"] or 0) > 2 else "neutral" if (gdp["gdp_now_estimate"] or 0) > 0 else "negative",
        })

    if rates.get("spread_2s10s") is not None:
        indicators.append({
            "name": "2s10s Spread",
            "value": f"{rates['spread_2s10s']}%",
            "status": "inverted" if (rates["spread_2s10s"] or 0) < 0 else "neutral",
        })

    liq = raw.get("liquidity", {})
    if liq.get("m2_yoy_growth") is not None:
        indicators.append({
            "name": "M2 Growth",
            "value": f"{liq['m2_yoy_growth']}%",
            "status": _status(liq.get("m2_trend")),
        })

    fg = raw.get("fear_greed", {})
    if fg.get("score") is not None:
        indicators.append({
            "name": "Fear & Greed",
            "value": f"{fg['score']}",
            "status": "positive" if (fg.get("score") or 50) > 60 else "negative" if (fg.get("score") or 50) < 40 else "neutral",
        })

    # ── Build full response: ALL original sections + frontend keys ────
    return {
        # Frontend-required top-level keys
        "benchmark_etfs": benchmark_etfs,
        "vix": vix,
        "yield_snapshot": yield_snapshot,
        "indicators": indicators,
        # Full dashboard sections (all original data preserved)
        "last_updated": raw.get("last_updated"),
        "data_sources": raw.get("data_sources"),
        "market_snapshot": raw.get("market_snapshot"),
        "fed": raw.get("fed"),
        "inflation": raw.get("inflation"),
        "labor": raw.get("labor"),
        "gdp": raw.get("gdp"),
        "rates_and_yields": raw.get("rates_and_yields"),
        "liquidity": raw.get("liquidity"),
        "commodities": raw.get("commodities"),
        "manufacturing": raw.get("manufacturing"),
        "scenarios": raw.get("scenarios"),
        "fear_greed": raw.get("fear_greed"),
        "dollar": raw.get("dollar"),
        "geopolitical": raw.get("geopolitical"),
    }


# ── Rates ────────────────────────────────────────────────────────────

def transform_rates(raw: dict) -> dict:
    """
    Input:  MacroProvider.get_rates() result
    Output: { yield_curve, spreads, indicators }
    """
    # yield_curve — remap tenor→maturity, yield_pct→yield, add change/previousClose
    yield_curve = []
    for pt in raw.get("yield_curve", []):
        y = _r(pt.get("yield_pct"), 2)
        yield_curve.append({
            "maturity": pt.get("tenor"),
            "yield": y,
            "change": 0.0,          # FRED doesn't give intraday change; FMP may
            "previousClose": y,      # same as yield when we lack prior close
        })

    # spreads
    spreads_raw = raw.get("spreads", {})
    spreads = {
        "2s10s": _r(spreads_raw.get("spread_2s10s"), 4),
        "10y3m": _r(spreads_raw.get("spread_10y3m"), 4),
    }

    # indicators
    indicators = []
    key = raw.get("key_rates", {})
    fed = raw.get("fed_policy", {})

    if fed.get("funds_rate") is not None:
        indicators.append({
            "name": "Fed Funds Rate",
            "value": f"{fed['funds_rate']}%",
            "status": "elevated" if (fed["funds_rate"] or 0) > 4 else "neutral",
        })

    if key.get("us_10y") is not None:
        indicators.append({
            "name": "10Y Yield",
            "value": f"{key['us_10y']}%",
            "status": "neutral",
        })

    if key.get("us_2y") is not None:
        indicators.append({
            "name": "2Y Yield",
            "value": f"{key['us_2y']}%",
            "status": "neutral",
        })

    curve_status = spreads_raw.get("curve_status", "normal")
    indicators.append({
        "name": "Yield Curve",
        "value": curve_status.title(),
        "status": "inverted" if curve_status == "inverted" else "neutral",
    })

    mortgage = raw.get("mortgage", {})
    if mortgage.get("rate_30y") is not None:
        indicators.append({
            "name": "30Y Mortgage",
            "value": f"{mortgage['rate_30y']}%",
            "status": "elevated" if (mortgage["rate_30y"] or 0) > 6 else "neutral",
        })

    credit = raw.get("credit_spreads", {})
    if credit.get("hy_oas") is not None:
        indicators.append({
            "name": "HY OAS",
            "value": f"{credit['hy_oas']}bps",
            "status": "elevated" if (credit["hy_oas"] or 0) > 400 else "neutral",
        })

    return {
        **raw,
        "yield_curve": yield_curve,
        "spreads": spreads,
        "indicators": indicators,
    }


# ── Inflation ────────────────────────────────────────────────────────

def transform_inflation(raw: dict) -> dict:
    """
    Input:  MacroProvider.get_inflation() result
    Output: { headline, history, cpi_components, indicators }
    """
    h = raw.get("headline", {})
    fed = raw.get("fed_preferred", {})

    headline = {
        "cpi_yoy": _r(h.get("cpi_yoy"), 1),
        "core_cpi_yoy": _r(h.get("core_cpi_yoy"), 1),
        "core_pce_yoy": _r(fed.get("core_pce_yoy"), 1),
        "ppi_yoy": _r(h.get("ppi_yoy"), 1),
        "cpi_mom": _r(h.get("cpi_mom"), 2),
        "target": fed.get("target", 2.0),
    }

    # history — transform from {date, value} to {month, headline, core}
    cpi_hist = raw.get("history", {}).get("cpi", [])
    pce_hist = raw.get("history", {}).get("core_pce", [])

    # Build a month→values map
    pce_by_month = {}
    for pt in pce_hist:
        ml = _month_label(pt.get("date"))
        if ml:
            pce_by_month[ml] = pt.get("value")

    history = []
    # Use last 12 CPI data points
    for pt in cpi_hist[-12:]:
        ml = _month_label(pt.get("date"))
        history.append({
            "month": ml,
            "headline": _r(pt.get("value"), 1),
            "core": _r(pce_by_month.get(ml), 1),
        })

    # cpi_components — synthesize from available data
    # We have headline CPI and core CPI; we can infer some components
    cpi_components = []
    alt = raw.get("alternative_measures", {})
    mkt = raw.get("market_expectations", {})

    # Shelter is typically the largest CPI component and driver of stickiness
    if alt.get("sticky_cpi") is not None:
        cpi_components.append({
            "name": "Sticky CPI",
            "value": _r(alt["sticky_cpi"], 1),
            "hot": (alt["sticky_cpi"] or 0) > 4.0,
        })
    if alt.get("trimmed_mean_pce") is not None:
        cpi_components.append({
            "name": "Trimmed Mean PCE",
            "value": _r(alt["trimmed_mean_pce"], 1),
            "hot": (alt["trimmed_mean_pce"] or 0) > 3.0,
        })
    if mkt.get("breakeven_5y") is not None:
        cpi_components.append({
            "name": "5Y Breakeven",
            "value": _r(mkt["breakeven_5y"], 2),
            "hot": (mkt["breakeven_5y"] or 0) > 2.5,
        })
    if mkt.get("breakeven_10y") is not None:
        cpi_components.append({
            "name": "10Y Breakeven",
            "value": _r(mkt["breakeven_10y"], 2),
            "hot": (mkt["breakeven_10y"] or 0) > 2.5,
        })

    # indicators
    indicators = []
    trend = raw.get("trend", "sticky")
    target_status = fed.get("target_status", "unknown")

    if headline["cpi_yoy"] is not None:
        indicators.append({
            "name": "CPI YoY",
            "value": f"{headline['cpi_yoy']}%",
            "status": "elevated" if (headline["cpi_yoy"] or 0) > 3.5 else _status(trend),
        })
    if headline["core_pce_yoy"] is not None:
        indicators.append({
            "name": "Core PCE",
            "value": f"{headline['core_pce_yoy']}%",
            "status": "elevated" if target_status == "well_above_target" else "neutral" if target_status == "at_target" else "elevated",
        })
    if headline["ppi_yoy"] is not None:
        indicators.append({
            "name": "PPI YoY",
            "value": f"{headline['ppi_yoy']}%",
            "status": "elevated" if (headline["ppi_yoy"] or 0) > 3 else "neutral",
        })
    indicators.append({
        "name": "Trend",
        "value": trend.title(),
        "status": _status(trend),
    })
    indicators.append({
        "name": "Fed Target",
        "value": f"{headline.get('target', 2.0)}%",
        "status": "neutral" if target_status == "at_target" else "elevated",
    })

    return {
        **raw,
        "headline": headline,
        "history": history,
        "cpi_components": cpi_components,
        "indicators": indicators,
    }


# ── Growth ───────────────────────────────────────────────────────────

def transform_growth(raw: dict) -> dict:
    """
    Input:  MacroProvider.get_growth() result
    Output: { gdp, pmi, indicators }
    """
    # gdp — already [{quarter, gdp}]
    gdp = raw.get("gdp", {}).get("quarterly_data", [])

    # pmi — build from ISM manufacturing history + consumer sentiment
    ism_hist = raw.get("history", {}).get("ism_manufacturing", [])
    sent_hist = raw.get("history", {}).get("consumer_sentiment", [])

    # Build sentiment by month
    sent_by_month = {}
    for pt in sent_hist:
        ml = _month_label(pt.get("date"))
        if ml:
            sent_by_month[ml] = pt.get("value")

    pmi = []
    for pt in ism_hist[-12:]:
        ml = _month_label(pt.get("date"))
        pmi.append({
            "month": ml,
            "mfg": _r(pt.get("value"), 1),
            "svc": _r(sent_by_month.get(ml), 1),  # Using consumer sentiment as svc proxy
        })

    # indicators
    indicators = []
    gdp_sec = raw.get("gdp", {})
    mfg = raw.get("manufacturing", {})
    consumer = raw.get("consumer", {})
    liq = raw.get("liquidity", {})
    prod = raw.get("production", {})

    if gdp_sec.get("latest") is not None:
        indicators.append({
            "name": "GDP Growth",
            "value": f"{gdp_sec['latest']}%",
            "status": "positive" if (gdp_sec["latest"] or 0) > 2 else "neutral" if (gdp_sec["latest"] or 0) > 0 else "negative",
        })
    if gdp_sec.get("recession_signal"):
        indicators.append({
            "name": "Recession Signal",
            "value": "Active",
            "status": "negative",
        })

    if mfg.get("ism_manufacturing") is not None:
        indicators.append({
            "name": "ISM Manufacturing",
            "value": f"{mfg['ism_manufacturing']}",
            "status": _status(mfg.get("signal")),
        })
    if consumer.get("retail_sales_yoy") is not None:
        indicators.append({
            "name": "Retail Sales YoY",
            "value": f"{consumer['retail_sales_yoy']}%",
            "status": "positive" if (consumer["retail_sales_yoy"] or 0) > 2 else "neutral",
        })
    if consumer.get("consumer_sentiment") is not None:
        indicators.append({
            "name": "Consumer Sentiment",
            "value": f"{consumer['consumer_sentiment']}",
            "status": "positive" if (consumer["consumer_sentiment"] or 0) > 80 else "neutral" if (consumer["consumer_sentiment"] or 0) > 60 else "negative",
        })
    if prod.get("industrial_production_yoy") is not None:
        indicators.append({
            "name": "Industrial Production",
            "value": f"{prod['industrial_production_yoy']}%",
            "status": "positive" if (prod["industrial_production_yoy"] or 0) > 1 else "neutral" if (prod["industrial_production_yoy"] or 0) > -1 else "negative",
        })
    if liq.get("m2_yoy_growth") is not None:
        indicators.append({
            "name": "M2 Growth",
            "value": f"{liq['m2_yoy_growth']}%",
            "status": _status(liq.get("m2_trend")),
        })

    return {
        **raw,
        "gdp": gdp,
        "pmi": pmi,
        "indicators": indicators,
    }


# ── Labor ────────────────────────────────────────────────────────────

def transform_labor(raw: dict) -> dict:
    """
    Input:  MacroProvider.get_labor() result
    Output: { unemployment, nfp, indicators }
    """
    # unemployment — from history
    unemp_hist = raw.get("history", {}).get("unemployment", [])
    unemployment = []
    for pt in unemp_hist[-12:]:
        unemployment.append({
            "month": _month_label(pt.get("date")),
            "rate": _r(pt.get("value"), 1),
        })

    # nfp — from history (PAYEMS is total; we need month-over-month change)
    nfp_hist = raw.get("history", {}).get("nfp", [])
    nfp = []
    for i in range(1, min(len(nfp_hist), 13)):
        idx = len(nfp_hist) - 13 + i if len(nfp_hist) >= 13 else i
        if idx < 1 or idx >= len(nfp_hist):
            continue
        curr = nfp_hist[idx].get("value")
        prev = nfp_hist[idx - 1].get("value")
        if curr is not None and prev is not None:
            change = int(round((curr - prev) * 1000))  # PAYEMS is in thousands
            nfp.append({
                "month": _month_label(nfp_hist[idx].get("date")),
                "nfp": change,
            })

    # indicators
    indicators = []
    emp = raw.get("employment", {})
    claims = raw.get("claims", {})
    wages = raw.get("wages", {})
    jobs = raw.get("job_openings", {})
    status = raw.get("labor_market_status", "neutral")

    if emp.get("unemployment_rate") is not None:
        indicators.append({
            "name": "Unemployment",
            "value": f"{emp['unemployment_rate']}%",
            "status": "positive" if (emp["unemployment_rate"] or 5) < 4.5 else "neutral",
        })
    if emp.get("u6_rate") is not None:
        indicators.append({
            "name": "U-6 Rate",
            "value": f"{emp['u6_rate']}%",
            "status": "neutral",
        })
    if emp.get("participation_rate") is not None:
        indicators.append({
            "name": "Participation Rate",
            "value": f"{emp['participation_rate']}%",
            "status": "neutral",
        })
    if emp.get("nfp_mom_change") is not None:
        indicators.append({
            "name": "NFP Last Month",
            "value": f"{emp['nfp_mom_change']:,}",
            "status": "positive" if (emp["nfp_mom_change"] or 0) > 150000 else "neutral" if (emp["nfp_mom_change"] or 0) > 0 else "negative",
        })
    if emp.get("nfp_3m_avg") is not None:
        indicators.append({
            "name": "NFP 3M Avg",
            "value": f"{emp['nfp_3m_avg']:,}",
            "status": "positive" if (emp["nfp_3m_avg"] or 0) > 150000 else "neutral",
        })
    if wages.get("avg_hourly_earnings_yoy") is not None:
        indicators.append({
            "name": "Wage Growth",
            "value": f"{wages['avg_hourly_earnings_yoy']}%",
            "status": "elevated" if (wages["avg_hourly_earnings_yoy"] or 0) > 4 else "neutral",
        })
    if claims.get("initial_claims") is not None:
        indicators.append({
            "name": "Initial Claims",
            "value": f"{claims['initial_claims']:,}",
            "status": "positive" if (claims["initial_claims"] or 300000) < 250000 else "neutral",
        })
    if jobs.get("jolts_millions") is not None:
        indicators.append({
            "name": "JOLTS Openings",
            "value": f"{jobs['jolts_millions']}M",
            "status": _status(status),
        })

    return {
        **raw,
        "unemployment": unemployment,
        "nfp": nfp,
        "indicators": indicators,
    }


# ── Risk ─────────────────────────────────────────────────────────────

def transform_risk(raw: dict) -> dict:
    """
    Input:  MacroProvider.get_risk() result
    Output: { risk_framework, vix_history, confidence, indicators }
    """
    vol = raw.get("volatility", {})
    credit = raw.get("credit_spreads", {})
    fg = raw.get("fear_greed", {})
    dollar = raw.get("dollar", {})
    yc = raw.get("yield_curve_risk", {})

    # risk_framework — synthesize from all risk dimensions
    risk_framework = []

    # RATES risk
    curve_inverted = yc.get("inverted", False)
    risk_framework.append({
        "label": "RATES",
        "level": "ELEVATED" if curve_inverted else "NORMAL",
        "color": "amber" if curve_inverted else "green",
        "detail": f"2s10s {'inverted' if curve_inverted else 'normal'} ({_r(yc.get('spread_2s10s'))}%)",
    })

    # VOLATILITY risk
    vix_val = vol.get("vix") or 0
    if vix_val > 30:
        vix_level, vix_color = "HIGH", "red"
    elif vix_val > 20:
        vix_level, vix_color = "ELEVATED", "amber"
    else:
        vix_level, vix_color = "LOW", "green"
    risk_framework.append({
        "label": "VOLATILITY",
        "level": vix_level,
        "color": vix_color,
        "detail": f"VIX at {_r(vix_val)}",
    })

    # CREDIT risk
    hy = credit.get("hy_oas") or 0
    if hy > 500:
        cr_level, cr_color = "HIGH", "red"
    elif hy > 400:
        cr_level, cr_color = "ELEVATED", "amber"
    else:
        cr_level, cr_color = "NORMAL", "green"
    risk_framework.append({
        "label": "CREDIT",
        "level": cr_level,
        "color": cr_color,
        "detail": f"HY OAS {_r(hy)}bps",
    })

    # SENTIMENT risk
    fg_score = fg.get("score") or 50
    if fg_score > 75 or fg_score < 25:
        sent_level, sent_color = "EXTREME", "red"
    elif fg_score > 60 or fg_score < 40:
        sent_level, sent_color = "ELEVATED", "amber"
    else:
        sent_level, sent_color = "NEUTRAL", "green"
    risk_framework.append({
        "label": "SENTIMENT",
        "level": sent_level,
        "color": sent_color,
        "detail": f"Fear & Greed {fg_score} ({fg.get('rating', 'N/A')})",
    })

    # DOLLAR risk
    dxy_val = dollar.get("dxy") or 0
    if dxy_val > 108:
        dxy_level, dxy_color = "STRONG", "amber"
    elif dxy_val < 95:
        dxy_level, dxy_color = "WEAK", "amber"
    else:
        dxy_level, dxy_color = "NORMAL", "green"
    risk_framework.append({
        "label": "DOLLAR",
        "level": dxy_level,
        "color": dxy_color,
        "detail": f"DXY at {_r(dxy_val)}",
    })

    # vix_history — from raw history
    vix_hist_raw = raw.get("history", {}).get("vix", [])
    vix_history = []
    for pt in vix_hist_raw[-12:]:
        vix_history.append({
            "month": _month_label(pt.get("date")),
            "vix": _r(pt.get("value"), 1),
        })

    # confidence — we don't have a dedicated confidence series in risk,
    # but fear_greed provides sentiment tracking
    confidence = []
    # Use Fear & Greed components if available, otherwise empty
    fg_components = fg.get("components") or {}
    if fg_components:
        # Build a single-point confidence entry from current data
        confidence.append({
            "month": "Current",
            "conf": _r(fg_score, 1),
            "umich": None,  # Consumer sentiment is in growth tab
        })

    # indicators
    indicators = []
    if vol.get("vix") is not None:
        indicators.append({
            "name": "VIX",
            "value": f"{vol['vix']}",
            "status": _status(vol.get("signal")),
        })
    if credit.get("hy_oas") is not None:
        indicators.append({
            "name": "HY OAS",
            "value": f"{credit['hy_oas']}bps",
            "status": "elevated" if (credit["hy_oas"] or 0) > 400 else "neutral",
        })
    if credit.get("bbb_oas") is not None:
        indicators.append({
            "name": "BBB OAS",
            "value": f"{credit['bbb_oas']}bps",
            "status": "neutral",
        })
    if fg.get("score") is not None:
        indicators.append({
            "name": "Fear & Greed",
            "value": f"{fg['score']} ({fg.get('rating', '')})",
            "status": "positive" if fg["score"] > 60 else "negative" if fg["score"] < 40 else "neutral",
        })
    if dollar.get("dxy") is not None:
        indicators.append({
            "name": "DXY",
            "value": f"{dollar['dxy']}",
            "status": "neutral",
        })
    if yc.get("spread_2s10s") is not None:
        indicators.append({
            "name": "2s10s Spread",
            "value": f"{yc['spread_2s10s']}%",
            "status": "inverted" if yc.get("inverted") else "neutral",
        })

    return {
        **raw,
        "risk_framework": risk_framework,
        "vix_history": vix_history,
        "confidence": confidence,
        "indicators": indicators,
    }
