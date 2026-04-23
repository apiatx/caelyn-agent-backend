"""
Dilution risk, catalyst proximity, and crowding risk scoring.

Data sources used:
  - edgar_cache disk cache (filings, catalysts) — instant, non-blocking
  - Finnhub company-news via httpx — 14-day window, cached FINNHUB_TTL
  - Finnhub earnings calendar via httpx — 90-day lookahead, cached EARNINGS_TTL
  - Raw FMP data heuristics (balance sheet, market cap, revenue growth)

Dilution risk note: higher score = MORE dilution risk.
  Penalty rules in playbook registry handle the deduction.
  Consistent with existing penalty structure (threshold = 70, deduction = 8pts).
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional


# ── Dilution signal keywords ──────────────────────────────────────────────────

_DILUTION_STRONG = [
    "at-the-market offering", "atm offering", "atm program",
    "shelf registration", "s-3 registration", "s3 filing",
    "secondary offering", "follow-on offering", "public offering",
    "registered direct offering", "common stock offering",
    "prospectus supplement", "dilutive issuance",
]

_DILUTION_MODERATE = [
    "warrant", "convertible note", "convertible debt",
    "shares outstanding", "common stock issuance",
    "equity financing", "capital raise", "at the market",
]

_POSITIVE_NEWS = [
    "contract award", "partnership agreement", "acquisition",
    "revenue beat", "guidance raised", "fda approval",
    "government contract", "record revenue", "new product launch",
    "strategic alliance", "joint venture",
]

# ── Catalyst proximity keywords ───────────────────────────────────────────────

_STRONG_CATALYST = [
    "contract award", "fda approval", "earnings beat",
    "record revenue", "guidance raised", "merger", "acquisition",
    "major contract", "government contract", "patent approval",
]

_MODERATE_CATALYST = [
    "partnership", "product launch", "analyst upgrade",
    "new customer", "expansion", "strategic", "collaboration",
]


# ── Dilution risk ─────────────────────────────────────────────────────────────

def score_dilution_risk_from_data(
    ticker: str,
    mkt_cap: Optional[float],
    debt_to_equity: Optional[float],
    revenue_growth_yoy: Optional[float],
    news: List[Dict],
) -> "FactorDetail":
    """
    Estimate dilution risk from news keywords + balance sheet heuristics.

    Score: 0-100 where higher = more dilution risk.
    Penalty rules fire when score > 70 (configured in registry).
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    risk_score: float = 20.0    # baseline: assume low risk
    source_tags: List[str] = []
    status = "heuristic"

    # 1. Edgar disk cache — instant, no API call
    try:
        from data.edgar_cache import get_filings
        filings = get_filings(ticker.upper()) or []
        if isinstance(filings, list):
            for f in filings[:10]:
                ftype = (f.get("form") or f.get("type") or "").upper()
                title = (f.get("title") or f.get("description") or "").lower()
                if ftype in ("S-3", "S-3/A", "424B5", "424B3", "S-1", "S-1/A"):
                    risk_score = max(risk_score, 78.0)
                    reasons.append(f"Recent SEC filing: {ftype}")
                    source_tags.append("edgar_filing")
                    status = "real"
                elif "atm" in title or "at-the-market" in title:
                    risk_score = max(risk_score, 82.0)
                    reasons.append("ATM program detected in filings")
                    source_tags.append("edgar_atm")
                    status = "real"
    except Exception as e:
        print(f"[DILUTION] Edgar cache error for {ticker}: {e}")

    # 2. News keyword scan
    news_texts = [
        ((item.get("headline") or item.get("title") or "") + " " +
         (item.get("summary") or item.get("content") or "")).lower()
        for item in (news or [])
    ]
    combined_news = " ".join(news_texts)

    strong_hit = any(kw in combined_news for kw in _DILUTION_STRONG)
    moderate_hit = any(kw in combined_news for kw in _DILUTION_MODERATE)

    if strong_hit:
        risk_score = max(risk_score, 80.0)
        hit_kws = [kw for kw in _DILUTION_STRONG if kw in combined_news]
        reasons.append(f"Dilution language in recent news: {', '.join(hit_kws[:2])}")
        source_tags.append("news_keywords")
        status = "real" if status == "heuristic" else status
    elif moderate_hit:
        risk_score = max(risk_score, 55.0)
        reasons.append("Financing-related language in recent news")
        source_tags.append("news_keywords")

    # 3. Balance sheet + size heuristic
    if mkt_cap is not None and mkt_cap < 1e9:    # sub-$1B
        if debt_to_equity is not None and debt_to_equity > 1.5:
            risk_score = max(risk_score, 60.0)
            reasons.append(f"Small cap (${mkt_cap/1e6:.0f}M) with elevated D/E {debt_to_equity:.1f}x")
            source_tags.append("balance_sheet_heuristic")
        if revenue_growth_yoy is not None and revenue_growth_yoy < 0:
            risk_score = max(risk_score, 55.0)
            reasons.append("Declining revenue + small cap = financing risk")
            source_tags.append("balance_sheet_heuristic")
        elif mkt_cap < 3e8:    # sub-$300M micro-cap
            risk_score = max(risk_score, 45.0)
            if not reasons:
                reasons.append(f"Micro-cap (${mkt_cap/1e6:.0f}M) — elevated financing risk baseline")

    # Large cap with no signals → very low risk
    if not reasons and mkt_cap is not None and mkt_cap > 5e9:
        risk_score = 15.0
        reasons.append(f"Large cap (${mkt_cap/1e9:.1f}B) with no dilution signals")
        status = "heuristic"

    if not reasons:
        reasons.append("No dilution signals detected (baseline)")
        status = "fallback"

    return FactorDetail(
        score=round(risk_score, 1),
        status=status,
        reasons=reasons,
        source_tags=source_tags,
    )


# ── Catalyst proximity ────────────────────────────────────────────────────────

def score_catalyst_proximity_from_data(
    ticker: str,
    earnings_calendar: List[Dict],
    news: List[Dict],
) -> "FactorDetail":
    """
    Score proximity to upcoming catalysts.

    Signal hierarchy:
      1. Earnings within 0-7 days         = very strong (90+)
      2. Earnings within 8-21 days         = strong (75-85)
      3. Earnings within 22-45 days        = moderate (62-72)
      4. Earnings within 46-90 days        = mild boost (50-58)
      5. Strong catalyst news in last 7 days = +15 boost
      6. Moderate catalyst news             = +8 boost
      7. No signals                        = 38 (below neutral — stale setup)
    """
    from services.playbook.playbook_types import FactorDetail

    today = date.today()
    reasons: List[str] = []
    source_tags: List[str] = []
    base_score: float = 38.0    # below neutral: no known catalyst = somewhat penalized
    status = "heuristic"

    # 1. Earnings calendar
    if earnings_calendar:
        for entry in earnings_calendar:
            sym = (entry.get("symbol") or entry.get("ticker") or "").upper()
            date_str = entry.get("date") or entry.get("earningsDate") or ""
            if sym and sym != ticker.upper():
                continue
            try:
                earnings_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
                days_away = (earnings_date - today).days
                if 0 <= days_away <= 7:
                    base_score = 92.0
                    reasons.append(f"Earnings in {days_away}d ({date_str[:10]}) — very near catalyst")
                    source_tags.append("earnings_calendar")
                    status = "real"
                    break
                elif 8 <= days_away <= 21:
                    base_score = max(base_score, 80.0)
                    reasons.append(f"Earnings in {days_away}d ({date_str[:10]})")
                    source_tags.append("earnings_calendar")
                    status = "real"
                    break
                elif 22 <= days_away <= 45:
                    base_score = max(base_score, 65.0)
                    reasons.append(f"Earnings in {days_away}d ({date_str[:10]})")
                    source_tags.append("earnings_calendar")
                    status = "real"
                    break
                elif 46 <= days_away <= 90:
                    base_score = max(base_score, 52.0)
                    reasons.append(f"Earnings in {days_away}d ({date_str[:10]}) — extended window")
                    source_tags.append("earnings_calendar")
                    status = "real"
                    break
            except (ValueError, AttributeError):
                continue

    # 2. News catalyst scan (last 14 days)
    news_texts = [
        ((item.get("headline") or item.get("title") or "") + " " +
         (item.get("summary") or item.get("content") or "")).lower()
        for item in (news or [])
    ]
    combined_news = " ".join(news_texts)

    strong_hit  = any(kw in combined_news for kw in _STRONG_CATALYST)
    moderate_hit = any(kw in combined_news for kw in _MODERATE_CATALYST)

    if strong_hit:
        hit_kws = [kw for kw in _STRONG_CATALYST if kw in combined_news]
        base_score = min(100.0, base_score + 15.0)
        reasons.append(f"Strong catalyst in recent news: {', '.join(hit_kws[:2])}")
        source_tags.append("news_catalyst")
        if status == "heuristic":
            status = "real"
    elif moderate_hit:
        base_score = min(100.0, base_score + 8.0)
        reasons.append("Moderate catalyst activity in recent news")
        source_tags.append("news_catalyst")

    # 3. Recent news volume as a weak catalyst signal
    news_count = len(news or [])
    if news_count >= 5 and base_score < 50:
        base_score = min(base_score + 6.0, 50.0)
        reasons.append(f"{news_count} recent news articles — elevated coverage")

    if not reasons:
        reasons.append("No near-term catalyst signal found")
        status = "fallback"

    return FactorDetail(
        score=round(base_score, 1),
        status=status,
        reasons=reasons,
        source_tags=source_tags,
    )


# ── Crowding risk ─────────────────────────────────────────────────────────────

# Sector median PE (same as playbook_scoring.py — keep in sync)
_SECTOR_PE_MEDIANS: Dict[str, float] = {
    "Technology":               28.0,
    "Healthcare":               22.0,
    "Health Care":              22.0,
    "Consumer Cyclical":        18.0,
    "Financial Services":       12.0,
    "Financials":               12.0,
    "Energy":                   10.0,
    "Utilities":                16.0,
    "Basic Materials":          14.0,
    "Materials":                14.0,
    "Consumer Defensive":       20.0,
    "Consumer Staples":         20.0,
    "Communication Services":   20.0,
    "Industrials":              18.0,
    "Real Estate":              30.0,
}
_DEFAULT_PE_MEDIAN = 18.0


def score_crowding_risk(
    price: Optional[float],
    week52_high: Optional[float],
    week52_low: Optional[float],
    pe_ratio: Optional[float],
    sector: Optional[str],
    news_count: int,
) -> "FactorDetail":
    """
    Heuristic crowding risk: is this a potentially over-owned / over-hyped setup?

    Components (higher = more crowded):
      1. Price extension:  % position in 52-week range (0=lows, 1=highs)
      2. Valuation premium: PE ratio vs sector median ratio
      3. News saturation:  article count in last 7-14 days

    Score: 0-100 where higher = more crowded (consistent with penalty structure).
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    components: List[float] = []
    source_tags: List[str] = []
    status = "heuristic"

    # 1. Price extension (0-100, high = near 52w high = potentially extended)
    price_ext_score = 50.0
    if price is not None and week52_high is not None and week52_low is not None:
        rng = week52_high - week52_low
        if rng > 0:
            position = (price - week52_low) / rng    # 0=low, 1=high
            price_ext_score = position * 100.0
            if position >= 0.90:
                reasons.append(f"Price near 52w high ({position*100:.0f}% of range) — extended")
                source_tags.append("price_extension")
            elif position <= 0.20:
                reasons.append(f"Price near 52w low — not extended")
    components.append(price_ext_score)

    # 2. Valuation premium (0-100, high = expensive vs peers)
    val_premium_score = 50.0
    if pe_ratio is not None and pe_ratio > 0:
        median_pe = _SECTOR_PE_MEDIANS.get(sector or "", _DEFAULT_PE_MEDIAN)
        pe_ratio_vs_median = pe_ratio / median_pe
        if pe_ratio_vs_median >= 3.0:
            val_premium_score = 95.0
            reasons.append(f"Extreme PE premium ({pe_ratio:.0f}x vs sector {median_pe:.0f}x median)")
            source_tags.append("valuation_premium")
        elif pe_ratio_vs_median >= 2.0:
            val_premium_score = 82.0
            reasons.append(f"High PE premium ({pe_ratio:.0f}x vs {median_pe:.0f}x median)")
            source_tags.append("valuation_premium")
        elif pe_ratio_vs_median >= 1.5:
            val_premium_score = 65.0
            reasons.append(f"Moderate PE premium ({pe_ratio:.0f}x vs {median_pe:.0f}x)")
        elif pe_ratio_vs_median <= 0.7:
            val_premium_score = 20.0    # cheap = not crowded
    components.append(val_premium_score)

    # 3. News saturation (0-100, high = lots of coverage = narrative heat)
    news_score = 50.0
    if news_count >= 10:
        news_score = 85.0
        reasons.append(f"High news volume ({news_count} articles in 14d) — narrative saturation")
        source_tags.append("news_saturation")
    elif news_count >= 6:
        news_score = 65.0
        reasons.append(f"Elevated news coverage ({news_count} articles)")
    elif news_count <= 1:
        news_score = 25.0    # low coverage = not crowded narrative
    components.append(news_score)

    final = sum(components) / len(components)

    if not reasons:
        reasons.append("No strong crowding signal detected")
        status = "fallback"

    return FactorDetail(
        score=round(final, 1),
        status=status,
        reasons=reasons,
        source_tags=source_tags,
    )


# ── Async data fetchers (thin httpx wrappers, consistent with FMP pattern) ───

async def fetch_company_news(ticker: str, finnhub_key: str, fmp_api_key: str = "") -> List[Dict]:
    """
    Fetch last 14 days of company news.
    Primary: FMP stable news/stock (no rate limit).
    Fallback: Finnhub company-news.
    """
    from data.cache import cache, FINNHUB_TTL
    cache_key = f"playbook:news14:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    import httpx

    # FMP stable primary
    if fmp_api_key:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.get(
                    "https://financialmodelingprep.com/stable/news/stock",
                    params={"symbols": ticker.upper(), "limit": 20, "apikey": fmp_api_key},
                )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    result = data[:20]
                    cache.set(cache_key, result, FINNHUB_TTL)
                    return result
        except Exception as e:
            print(f"[DILUTION] FMP news error for {ticker}: {e}")

    # Finnhub fallback
    if not finnhub_key:
        return []
    try:
        today   = date.today()
        from_dt = (today - timedelta(days=14)).strftime("%Y-%m-%d")
        to_dt   = today.strftime("%Y-%m-%d")
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                "https://finnhub.io/api/v1/company-news",
                params={"symbol": ticker.upper(), "from": from_dt, "to": to_dt, "token": finnhub_key},
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
        result = data[:20] if isinstance(data, list) else []
        cache.set(cache_key, result, FINNHUB_TTL)
        return result
    except Exception as e:
        print(f"[DILUTION] fetch_company_news error for {ticker}: {e}")
        return []


async def fetch_earnings_calendar(ticker: str, finnhub_key: str) -> List[Dict]:
    """Fetch 90-day earnings calendar for a ticker from Finnhub. Cached at EARNINGS_TTL."""
    if not finnhub_key:
        return []
    from data.cache import cache, EARNINGS_TTL
    cache_key = f"playbook:fh_earn:{ticker.upper()}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        import httpx
        today   = date.today()
        future  = (today + timedelta(days=90)).strftime("%Y-%m-%d")
        today_s = today.strftime("%Y-%m-%d")
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                "https://finnhub.io/api/v1/calendar/earnings",
                params={"from": today_s, "to": future, "symbol": ticker.upper(), "token": finnhub_key},
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
        earnings = data.get("earningsCalendar", []) if isinstance(data, dict) else []
        result   = [e for e in earnings if (e.get("symbol") or "").upper() == ticker.upper()]
        cache.set(cache_key, result, EARNINGS_TTL)
        return result
    except Exception as e:
        print(f"[DILUTION] fetch_earnings_calendar error for {ticker}: {e}")
        return []
