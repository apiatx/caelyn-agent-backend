"""
Extended Factor Engine — Phase 2 factors.

7 new factors implemented here:
  1. supply_chain_confirmation  — curated supplier map + keywords + news inference
  2. ebitda_inflection_proximity — FMP income statement trend + margin heuristic
  3. backlog_quality            — news keyword scan for backlog/bookings signals
  4. evidence_freshness         — news recency scoring (7d / 21d / stale)
  5. execution_risk             — margin + leverage + revenue decline heuristics
  6. insider_buying             — DB query to insider_transactions + news fallback
  7. policy_tailwind            — theme + sector + keyword → policy bucket mapping

All functions are synchronous except for async IO helpers at the bottom.
Each function returns a FactorDetail. Missing data degrades gracefully to
"heuristic" or "fallback" status — never raises.
"""
from __future__ import annotations

import asyncio
import os
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ── Supply-Chain Confirmation ─────────────────────────────────────────────────

# Curated map: ticker → {score, tier, role, tags, bucket}
# tier 1 = direct supplier/customer, tier 2 = adjacent beneficiary
SUPPLY_CHAIN_MAP: Dict[str, Dict] = {
    "ASML":  {"score": 95, "tier": 1, "bucket": "lithography",
              "role": "Sole EUV scanner supplier — all leading-edge fabs depend directly"},
    "AMAT":  {"score": 88, "tier": 1, "bucket": "semicap_equipment",
              "role": "Etch/dep/ion-implant to TSMC, Samsung, Intel"},
    "LRCX":  {"score": 86, "tier": 1, "bucket": "semicap_equipment",
              "role": "Dominant etch equipment to all advanced fabs"},
    "KLAC":  {"score": 85, "tier": 1, "bucket": "process_control",
              "role": "Process control monopoly — critical at every advanced node"},
    "ENTG":  {"score": 82, "tier": 1, "bucket": "materials",
              "role": "CMP slurries and advanced photoresist to TSMC/Samsung"},
    "TSM":   {"score": 92, "tier": 1, "bucket": "foundry",
              "role": "Leading-node foundry for NVDA, AMD, Apple, Qualcomm"},
    "AMKR":  {"score": 82, "tier": 1, "bucket": "osat_packaging",
              "role": "Advanced OSAT packaging for AI and mobile chips"},
    "ONTO":  {"score": 77, "tier": 1, "bucket": "metrology",
              "role": "Advanced packaging metrology for HBM and chiplet inspection"},
    "FORM":  {"score": 74, "tier": 2, "bucket": "test_equipment",
              "role": "Test socket / burn-in equipment for packaging qualification"},
    "COHR":  {"score": 82, "tier": 1, "bucket": "photonics",
              "role": "High-speed photonic components for AI datacenter interconnects"},
    "LITE":  {"score": 86, "tier": 1, "bucket": "photonics",
              "role": "Coherent optical modules shipped to hyperscaler datacenters"},
    "AAOI":  {"score": 78, "tier": 1, "bucket": "photonics",
              "role": "400G/800G datacenter transceivers — limited supplier pool"},
    "CRDO":  {"score": 80, "tier": 1, "bucket": "photonics",
              "role": "AEC/DSP ICs for AI fabric connecting GPUs and networking"},
    "II":    {"score": 73, "tier": 2, "bucket": "photonics",
              "role": "InP substrate supplier — key material for photonic ICs"},
    "IPGP":  {"score": 74, "tier": 2, "bucket": "photonics",
              "role": "High-power fiber laser for industrial and defense applications"},
    "MU":    {"score": 83, "tier": 1, "bucket": "memory",
              "role": "HBM memory stacked on AI accelerators — 3-player global market"},
    "RMBS":  {"score": 72, "tier": 2, "bucket": "memory_interface",
              "role": "Memory interface chips between CPU and DDR — proprietary IP"},
    "WDC":   {"score": 68, "tier": 2, "bucket": "storage_memory",
              "role": "NAND flash storage — broad cloud and device supply chain"},
    "ETN":   {"score": 82, "tier": 1, "bucket": "power_grid",
              "role": "Power management and transformer for data centers and utilities"},
    "VRT":   {"score": 82, "tier": 1, "bucket": "datacenter_power",
              "role": "Data center cooling and UPS — direct hyperscaler supply chain"},
    "GEV":   {"score": 80, "tier": 1, "bucket": "power_generation",
              "role": "Gas turbines for off-grid AI data center power supply"},
    "NVT":   {"score": 73, "tier": 2, "bucket": "power_enclosure",
              "role": "Data center enclosures and power distribution — steady demand"},
    "PWR":   {"score": 73, "tier": 2, "bucket": "grid_construction",
              "role": "Electrical construction for hyperscaler and utility grid buildout"},
    "HUBB":  {"score": 71, "tier": 2, "bucket": "grid_components",
              "role": "Grid connectors and electrical components — infrastructure supply"},
    "ATKR":  {"score": 68, "tier": 2, "bucket": "grid_components",
              "role": "Electrical conduit and cable management for grid buildout"},
    "KTOS":  {"score": 77, "tier": 1, "bucket": "defense_supply",
              "role": "Drone and directed energy systems — prime DoD supply chain"},
    "AVAV":  {"score": 74, "tier": 1, "bucket": "defense_supply",
              "role": "Tactical UAV for DoD — limited substitution in category"},
    "LHX":   {"score": 71, "tier": 2, "bucket": "defense_electronics",
              "role": "Electro-optical ISR systems for DoD programs"},
    "RKLB":  {"score": 77, "tier": 1, "bucket": "space_launch",
              "role": "Dedicated small satellite launch — limited launch provider pool"},
    "ACLS":  {"score": 73, "tier": 2, "bucket": "semicap_niche",
              "role": "Ion implant for SiC chips in EV and power electronics"},
    "MKSI":  {"score": 72, "tier": 2, "bucket": "semicap_gases",
              "role": "Gas/pressure management for semiconductor manufacturing"},
    "SMCI":  {"score": 74, "tier": 2, "bucket": "server_integration",
              "role": "GPU server integration with custom thermal — hyperscaler supply"},
    "INTC":  {"score": 65, "tier": 2, "bucket": "idm_foundry",
              "role": "IDM with foundry ambitions — direct participant in semicap chain"},
}

_SC_DIRECT_KEYWORDS: Dict[str, int] = {
    "sole source":              90,
    "sole-source":              90,
    "only supplier":            88,
    "primary supplier":         82,
    "key supplier":             78,
    "critical supplier":        80,
    "direct supplier":          78,
    "hyperscaler":              72,
    "co-packaged optics":       82,
    "advanced packaging":       75,
    "high bandwidth memory":    80,
    "hbm stacking":             82,
    "test socket":              73,
    "burn-in":                  72,
    "cmp slurry":               76,
    "photoresist":              74,
    "euv":                      85,
    "leading-edge foundry":     88,
    "wafer level package":      75,
    "chiplet interconnect":     78,
    "gpu server":               72,
    "data center cooling":      70,
    "supply agreement":         68,
    "qualified vendor":         66,
    "approved vendor":          66,
}

_SC_NEWS_KEYWORDS = [
    "supply agreement", "awarded contract", "selected as supplier",
    "qualified by", "design win", "purchase order", "letter of intent",
    "multi-year supply", "customer awarded", "partnership agreement",
]


def score_supply_chain_confirmation(
    ticker: str,
    description: str,
    industry: str,
    news: List[Dict],
) -> "FactorDetail":
    """
    Confirm that the company sits in a real supply chain position,
    not just thematic proximity.

    Sources (priority order):
      1. Manual curated map (authoritative)
      2. Description keyword scan (direct/tier-1 keywords)
      3. News scan (design win, supply agreement, contract award)
      4. Industry fallback

    Score: 0-100. High = confirmed supply-chain position.
    """
    from services.playbook.playbook_types import FactorDetail

    t = ticker.upper().strip()
    desc_lower = (description or "").lower()
    reasons: List[str] = []
    source_tags: List[str] = []

    # 1. Manual map
    if t in SUPPLY_CHAIN_MAP:
        entry = SUPPLY_CHAIN_MAP[t]
        tier_label = f"Tier {entry['tier']}"
        reasons.append(f"{tier_label}: {entry['role']}")
        source_tags.append(entry["bucket"])
        return FactorDetail(
            score=float(entry["score"]),
            status="manual",
            reasons=reasons,
            source_tags=source_tags,
        )

    # 2. Description keywords
    kw_scores: List[int] = []
    matched_kws: List[str] = []
    for kw, sc in _SC_DIRECT_KEYWORDS.items():
        if kw in desc_lower:
            kw_scores.append(sc)
            matched_kws.append(kw)

    if kw_scores:
        top2 = sorted(kw_scores, reverse=True)[:2]
        score = round(sum(top2) / len(top2), 1)
        reasons.append(f"Description confirms supply chain: {', '.join(matched_kws[:3])}")
        source_tags.append("description_keyword")
        return FactorDetail(
            score=min(score, 90.0),
            status="heuristic",
            reasons=reasons,
            source_tags=source_tags,
        )

    # 3. News scan
    combined_news = " ".join(
        ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
        for item in (news or [])
    )
    news_hits = [kw for kw in _SC_NEWS_KEYWORDS if kw in combined_news]
    if news_hits:
        reasons.append(f"Supply-chain signal in recent news: {', '.join(news_hits[:2])}")
        source_tags.append("news_inference")
        return FactorDetail(
            score=62.0,
            status="heuristic",
            reasons=reasons,
            source_tags=source_tags,
        )

    # 4. Industry fallback
    ind_lower = (industry or "").lower()
    if any(kw in ind_lower for kw in (
        "semiconductor equipment", "electronic components",
        "aerospace & defense", "communication equipment",
        "electrical equipment",
    )):
        reasons.append("Industry suggests possible supply-chain role (not directly confirmed)")
        source_tags.append("industry_inference")
        return FactorDetail(score=48.0, status="heuristic", reasons=reasons, source_tags=source_tags)

    return FactorDetail(
        score=28.0, status="fallback",
        reasons=["No supply-chain confirmation signal found"],
        source_tags=[],
    )


# ── EBITDA Inflection Proximity ───────────────────────────────────────────────

async def fetch_income_statement(ticker: str, api_key: str) -> List[Dict]:
    """Fetch last 2 annual income statement periods from FMP."""
    if not api_key:
        return []
    try:
        from data.cache import cache, FMP_TTL
        ck = f"playbook:fmp:income:{ticker.upper()}"
        cached = cache.get(ck)
        if cached is not None:
            return cached
        import httpx
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.get(
                f"https://financialmodelingprep.com/stable/income-statement",
                params={"symbol": ticker.upper(), "period": "annual", "limit": "3", "apikey": api_key},
            )
        if resp.status_code != 200:
            return []
        data = resp.json()
        result = data if isinstance(data, list) else []
        cache.set(ck, result, FMP_TTL)
        return result
    except Exception as e:
        print(f"[EXTENDED_FACTORS] income_statement error {ticker}: {e}")
        return []


def score_ebitda_inflection_proximity(
    ticker: str,
    income_statements: List[Dict],
    revenue_growth_yoy: Optional[float],
    debt_to_equity: Optional[float],
    mkt_cap: Optional[float],
) -> "FactorDetail":
    """
    Score proximity to EBITDA/FCF inflection.
    High score = company appears near a credible positive inflection.

    Data hierarchy:
      1. FMP income statement (ebitda, operatingIncome, grossProfit, revenue)
         — YoY trend over 2 periods to determine inflection direction
      2. Heuristic from revenue growth + leverage (fallback)
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    source_tags: List[str] = []

    def _fv(d: dict, *keys) -> Optional[float]:
        for k in keys:
            v = d.get(k)
            if v is not None:
                try: return float(v)
                except (TypeError, ValueError): pass
        return None

    # ── Real: parse income statements ────────────────────────────────────────
    if income_statements and len(income_statements) >= 2:
        curr = income_statements[0]
        prev = income_statements[1]

        ebitda_curr = _fv(curr, "ebitda")
        ebitda_prev = _fv(prev, "ebitda")
        opinc_curr  = _fv(curr, "operatingIncome")
        opinc_prev  = _fv(prev, "operatingIncome")
        rev_curr    = _fv(curr, "revenue") or 1.0
        rev_prev    = _fv(prev, "revenue") or 1.0
        gp_curr     = _fv(curr, "grossProfit")
        gp_prev     = _fv(prev, "grossProfit")

        score = 45.0
        status = "real"

        # EBITDA: was negative, now near-positive or positive → strong inflection signal
        if ebitda_curr is not None and ebitda_prev is not None:
            ebitda_margin_curr = ebitda_curr / rev_curr if rev_curr else None
            ebitda_margin_prev = ebitda_prev / rev_prev if rev_prev else None
            if ebitda_curr > 0 and ebitda_prev < 0:
                score = max(score, 88.0)
                reasons.append(f"EBITDA turned positive: {ebitda_curr/1e6:.0f}M (prev: {ebitda_prev/1e6:.0f}M)")
                source_tags.append("ebitda_positive_flip")
            elif ebitda_curr < 0 and ebitda_prev < 0 and ebitda_curr > ebitda_prev:
                margin = abs(ebitda_curr / rev_curr) * 100 if rev_curr else None
                if margin and margin < 5:
                    score = max(score, 78.0)
                    reasons.append(f"EBITDA loss narrowing rapidly — {margin:.1f}% margin gap to positive")
                    source_tags.append("ebitda_narrowing")
                else:
                    score = max(score, 62.0)
                    reasons.append("EBITDA improving YoY but still negative")
                    source_tags.append("ebitda_improving")
            elif ebitda_margin_curr is not None and ebitda_margin_curr > 0.10:
                score = max(score, 70.0)
                reasons.append(f"Solid EBITDA margin ({ebitda_margin_curr*100:.1f}%) — already inflected")
                source_tags.append("ebitda_healthy")
            elif ebitda_curr is not None and ebitda_curr < 0:
                score = min(score, 30.0)
                reasons.append("EBITDA negative and deteriorating")
                source_tags.append("ebitda_negative")

        # Operating income trend
        if opinc_curr is not None and opinc_prev is not None:
            if opinc_curr > 0 and opinc_prev <= 0:
                score = max(score, 82.0)
                reasons.append("Operating income crossed to positive")
                source_tags.append("opinc_positive_flip")
            elif opinc_curr < 0:
                opinc_margin = abs(opinc_curr / rev_curr) * 100 if rev_curr else None
                if opinc_margin and opinc_margin < 5 and opinc_curr > opinc_prev:
                    score = max(score, 72.0)
                    reasons.append(f"Operating loss narrowing ({opinc_margin:.1f}% gap to positive)")
                    source_tags.append("opinc_narrowing")

        # Gross margin trend
        if gp_curr is not None and rev_curr:
            gm = gp_curr / rev_curr * 100
            if gm > 60:
                score = max(score, 65.0)
                reasons.append(f"High gross margin ({gm:.1f}%) — strong unit economics base")
                source_tags.append("gross_margin_strong")
            elif gm < 20:
                score = min(score, 38.0)
                reasons.append(f"Low gross margin ({gm:.1f}%) — limited path to EBITDA inflection")
                source_tags.append("gross_margin_weak")

        if not reasons:
            reasons.append("Income statement data available but no strong inflection signal")
        return FactorDetail(
            score=round(min(100.0, max(0.0, score)), 1),
            status=status,
            reasons=reasons[:3],
            source_tags=source_tags,
        )

    # ── Heuristic: revenue growth + leverage ─────────────────────────────────
    status = "heuristic"
    score = 45.0

    if revenue_growth_yoy is not None:
        if revenue_growth_yoy >= 0.30:
            score = max(score, 72.0)
            reasons.append(f"Strong revenue growth ({revenue_growth_yoy*100:.0f}%) supports inflection")
        elif revenue_growth_yoy >= 0.15:
            score = max(score, 62.0)
            reasons.append(f"Solid revenue growth ({revenue_growth_yoy*100:.0f}%) — likely improving leverage")
        elif revenue_growth_yoy < -0.10:
            score = min(score, 28.0)
            reasons.append(f"Revenue declining ({revenue_growth_yoy*100:.0f}%) — inflection risk")
        else:
            reasons.append(f"Moderate revenue growth ({revenue_growth_yoy*100:.0f}%)")

    if debt_to_equity is not None:
        if debt_to_equity < 0.5 and revenue_growth_yoy and revenue_growth_yoy > 0:
            score = max(score, 60.0)
            reasons.append("Clean balance sheet + positive revenue growth — favorable inflection conditions")
        elif debt_to_equity > 2.0:
            score = min(score, 35.0)
            reasons.append(f"High leverage (D/E {debt_to_equity:.1f}x) limits EBITDA inflection path")

    if not reasons:
        reasons.append("Insufficient financial data — using neutral estimate")
        status = "fallback"

    return FactorDetail(
        score=round(min(100.0, max(0.0, score)), 1),
        status=status,
        reasons=reasons[:3],
        source_tags=source_tags,
    )


# ── Backlog Quality ───────────────────────────────────────────────────────────

_BACKLOG_STRONG_KEYWORDS = [
    "backlog", "order backlog", "order book",
    "bookings", "awarded contract", "contract award", "contract awarded",
    "record backlog", "record orders", "record bookings",
    "purchase commitment", "purchase order", "capacity reservation",
    "pipeline record", "unfilled orders",
]

_BACKLOG_MODERATE_KEYWORDS = [
    "design win", "won contract", "received order", "letter of intent",
    "multi-year agreement", "framework agreement", "supply agreement",
    "customer win", "new order",
]

_BACKLOG_NEGATIVE_KEYWORDS = [
    "order cancellation", "cancelled orders", "order weakness",
    "booking miss", "pipeline slowdown", "demand weakness",
]

# Manual map for sector-level backlog presence heuristic
_BACKLOG_SECTOR_PRIORS: Dict[str, float] = {
    "Aerospace & Defense":   75.0,
    "Industrials":           62.0,
    "Technology":            50.0,
    "Healthcare":            45.0,
    "Energy":                55.0,
    "Utilities":             58.0,
}


def score_backlog_quality(
    ticker: str,
    description: str,
    industry: str,
    sector: Optional[str],
    news: List[Dict],
) -> "FactorDetail":
    """
    Score backlog/order-book quality. High = recent, meaningful backlog signal.

    Sources:
      1. News scan (strong keywords → real signal)
      2. Company description (backlog mentions)
      3. Sector prior (heuristic baseline)
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    source_tags: List[str] = []
    desc_lower = (description or "").lower()
    status = "heuristic"

    combined_news = " ".join(
        ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
        for item in (news or [])
    )

    # Check for negative signals first
    neg_hits = [kw for kw in _BACKLOG_NEGATIVE_KEYWORDS if kw in combined_news]
    if neg_hits:
        reasons.append(f"Backlog/demand weakness in recent news: {', '.join(neg_hits[:2])}")
        source_tags.append("news_backlog_negative")
        return FactorDetail(score=25.0, status="heuristic", reasons=reasons, source_tags=source_tags)

    # Strong news signal
    strong_hits = [kw for kw in _BACKLOG_STRONG_KEYWORDS if kw in combined_news]
    moderate_hits = [kw for kw in _BACKLOG_MODERATE_KEYWORDS if kw in combined_news]

    score = 45.0

    if strong_hits:
        score = 82.0
        reasons.append(f"Strong backlog signal in recent news: {', '.join(strong_hits[:2])}")
        source_tags.append("news_backlog_strong")
        status = "real"
    elif moderate_hits:
        score = 65.0
        reasons.append(f"Design wins / order signals in recent news: {', '.join(moderate_hits[:2])}")
        source_tags.append("news_backlog_moderate")
        status = "real"

    # Description check
    desc_hits = [kw for kw in _BACKLOG_STRONG_KEYWORDS if kw in desc_lower]
    if desc_hits and score < 65.0:
        score = max(score, 60.0)
        reasons.append(f"Backlog mentioned in company description: {', '.join(desc_hits[:2])}")
        source_tags.append("description_backlog")

    # Sector prior (only if no news signal)
    if not reasons:
        prior = _BACKLOG_SECTOR_PRIORS.get(sector or "", 45.0)
        score = prior
        reasons.append(f"No recent backlog/order signal — sector baseline ({sector or 'general'})")
        status = "fallback"

    return FactorDetail(
        score=round(score, 1),
        status=status,
        reasons=reasons[:3],
        source_tags=source_tags,
    )


# ── Evidence Freshness ────────────────────────────────────────────────────────

def score_evidence_freshness(
    ticker: str,
    news: List[Dict],
    earnings_calendar: List[Dict],
) -> "FactorDetail":
    """
    Score how fresh and recent the thesis-confirming evidence is.

    Scoring:
      - Strong confirming signal in last 7 days  → 85-92
      - Confirming signal in last 8-21 days       → 65-75
      - No news in last 21 days                   → 35-45
      - Earnings soon AND recent news             → +5 boost

    Higher score = fresher evidence = more current thesis support.
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    source_tags: List[str] = []
    status = "heuristic"

    _FRESH_CATALYST_KEYWORDS = [
        "contract award", "design win", "earnings beat", "record revenue",
        "guidance raised", "fda approval", "merger", "partnership",
        "government contract", "new customer", "analyst upgrade",
        "product launch", "strategic", "backlog",
    ]

    today = date.today()
    now_ts = datetime.utcnow().timestamp()

    # Parse news timestamps
    fresh_count_7d = 0
    fresh_count_21d = 0
    fresh_catalyst_7d = False
    fresh_catalyst_21d = False

    for item in (news or []):
        ts = item.get("datetime") or item.get("publishedAt") or 0
        try:
            ts_f = float(ts)
            days_ago = (now_ts - ts_f) / 86400
        except (TypeError, ValueError):
            days_ago = 999

        text = ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
        has_catalyst = any(kw in text for kw in _FRESH_CATALYST_KEYWORDS)

        if days_ago <= 7:
            fresh_count_7d += 1
            if has_catalyst:
                fresh_catalyst_7d = True
        if days_ago <= 21:
            fresh_count_21d += 1
            if has_catalyst:
                fresh_catalyst_21d = True

    if fresh_catalyst_7d:
        score = 88.0
        reasons.append(f"Fresh catalyst signal in last 7 days ({fresh_count_7d} articles)")
        source_tags.append("news_fresh_catalyst")
        status = "real"
    elif fresh_count_7d >= 3:
        score = 72.0
        reasons.append(f"Active news coverage in last 7 days ({fresh_count_7d} articles)")
        source_tags.append("news_fresh")
        status = "real"
    elif fresh_catalyst_21d:
        score = 65.0
        reasons.append(f"Catalyst signal in last 21 days (slightly aging)")
        source_tags.append("news_catalyst_aging")
        status = "real"
    elif fresh_count_21d >= 2:
        score = 52.0
        reasons.append(f"Some news coverage in last 21 days ({fresh_count_21d} articles)")
        source_tags.append("news_moderate")
    else:
        score = 32.0
        reasons.append("No meaningful news activity in last 21 days — stale thesis")
        source_tags.append("news_stale")
        status = "fallback"

    # Earnings soon → freshness boost (event approaching)
    for entry in (earnings_calendar or []):
        date_str = entry.get("date") or entry.get("earningsDate") or ""
        try:
            edate = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            days_away = (edate - today).days
            if 0 <= days_away <= 21:
                score = min(100.0, score + 5.0)
                reasons.append(f"Earnings in {days_away}d — upcoming freshness catalyst")
                source_tags.append("earnings_upcoming")
                break
        except (ValueError, AttributeError):
            continue

    return FactorDetail(
        score=round(score, 1),
        status=status,
        reasons=reasons[:3],
        source_tags=source_tags,
    )


# ── Execution Risk ────────────────────────────────────────────────────────────

_EXECUTION_RISK_KEYWORDS = [
    "going concern", "liquidity concern", "restructuring",
    "bankruptcy", "default risk", "covenant breach",
    "cash runway", "bridge financing", "emergency funding",
]

_EXECUTION_MODERATE_KEYWORDS = [
    "revenue miss", "guidance cut", "guidance lowered",
    "management change", "ceo departure", "cfo departure",
    "workforce reduction", "layoffs", "cost reduction",
]


def score_execution_risk(
    ticker: str,
    revenue_growth_yoy: Optional[float],
    debt_to_equity: Optional[float],
    mkt_cap: Optional[float],
    price: Optional[float],
    week52_low: Optional[float],
    week52_high: Optional[float],
    news: List[Dict],
) -> "FactorDetail":
    """
    Score execution risk. Higher score = more execution risk.
    Used as a penalty factor — penalty fires when score > 70.

    Components:
      1. Revenue decline severity
      2. Leverage stress
      3. Price stress (near 52w lows = market distress signal)
      4. Size fragility (micro-cap = more vulnerable)
      5. News keyword scan (going concern, restructuring, guidance cut)
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    source_tags: List[str] = []
    status = "heuristic"
    risk_score = 20.0

    # 1. News critical risk keywords
    combined_news = " ".join(
        ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
        for item in (news or [])
    )
    critical_hits = [kw for kw in _EXECUTION_RISK_KEYWORDS if kw in combined_news]
    moderate_hits = [kw for kw in _EXECUTION_MODERATE_KEYWORDS if kw in combined_news]

    if critical_hits:
        risk_score = max(risk_score, 88.0)
        reasons.append(f"Critical risk signal in news: {', '.join(critical_hits[:2])}")
        source_tags.append("news_critical_risk")
        status = "real"
    elif moderate_hits:
        risk_score = max(risk_score, 58.0)
        reasons.append(f"Execution concern in news: {', '.join(moderate_hits[:2])}")
        source_tags.append("news_moderate_risk")

    # 2. Revenue decline
    if revenue_growth_yoy is not None:
        if revenue_growth_yoy < -0.20:
            risk_score = max(risk_score, 72.0)
            reasons.append(f"Revenue declining sharply ({revenue_growth_yoy*100:.0f}% YoY)")
            source_tags.append("revenue_decline")
        elif revenue_growth_yoy < -0.05:
            risk_score = max(risk_score, 52.0)
            reasons.append(f"Revenue declining ({revenue_growth_yoy*100:.0f}% YoY)")

    # 3. Leverage stress
    if debt_to_equity is not None and debt_to_equity > 0:
        if debt_to_equity > 4.0:
            risk_score = max(risk_score, 65.0)
            reasons.append(f"Very high leverage (D/E {debt_to_equity:.1f}x)")
            source_tags.append("high_leverage")
        elif debt_to_equity > 2.5:
            risk_score = max(risk_score, 48.0)
            reasons.append(f"Elevated leverage (D/E {debt_to_equity:.1f}x)")

    # 4. Price near 52w low (distress signal)
    if price is not None and week52_high is not None and week52_low is not None:
        rng = week52_high - week52_low
        if rng > 0:
            pos = (price - week52_low) / rng
            if pos < 0.10:
                risk_score = max(risk_score, 60.0)
                reasons.append(f"Price near 52w low ({pos*100:.0f}% of range) — market stress signal")
                source_tags.append("price_distress")

    # 5. Micro-cap fragility
    if mkt_cap is not None and mkt_cap < 300e6:
        risk_score = max(risk_score, 40.0)
        if not any("micro" in r for r in reasons):
            reasons.append(f"Micro-cap (${mkt_cap/1e6:.0f}M) — elevated operational fragility")
            source_tags.append("microcap_fragility")

    # Low risk: large cap, clean balance sheet, growing revenue
    if (not reasons and mkt_cap and mkt_cap > 5e9
            and (revenue_growth_yoy or 0) > 0
            and (debt_to_equity or 0) < 1.0):
        risk_score = 15.0
        reasons.append("Large cap, growing revenue, moderate leverage — low execution risk")
        status = "heuristic"

    if not reasons:
        reasons.append("No execution risk signals detected (baseline)")
        status = "fallback"

    return FactorDetail(
        score=round(min(100.0, risk_score), 1),
        status=status,
        reasons=reasons[:3],
        source_tags=source_tags,
    )


# ── Insider Buying ────────────────────────────────────────────────────────────

_INSIDER_NEWS_KEYWORDS = [
    "insider buying", "insider purchase", "open market purchase",
    "director bought", "ceo bought", "cfo bought", "insider bought",
    "executive bought", "10b5-1 plan purchase",
]

_INSIDER_SELL_KEYWORDS = [
    "insider selling", "insider sold", "director sold",
    "executive sold", "10b5-1 sale",
]


def score_insider_buying(
    ticker: str,
    news: List[Dict],
    mkt_cap: Optional[float],
) -> "FactorDetail":
    """
    Score insider buying signal.

    Sources (priority):
      1. DB query to insider_transactions table (real SEC Form 4 data)
      2. News keyword fallback
      3. Neutral for large-caps with no signal (insiders rarely buy mega-caps)

    High score = meaningful recent buying = positive signal.
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    source_tags: List[str] = []
    status = "heuristic"

    # 1. DB query (best signal)
    db_signal = _query_insider_db(ticker)
    if db_signal:
        buys = db_signal.get("buys", 0)
        max_score = db_signal.get("max_conviction", 0)
        total_value = db_signal.get("total_value_usd", 0)

        if buys > 0 and max_score >= 50:
            score = min(88.0, 50.0 + max_score * 0.4)
            reasons.append(
                f"{buys} insider buy(s) in last 30d, max conviction={max_score}, "
                f"total=${total_value/1e3:.0f}K"
            )
            source_tags.append("db_insider_buy")
            status = "real"
            return FactorDetail(score=round(score, 1), status=status, reasons=reasons, source_tags=source_tags)
        elif buys == 0 and db_signal.get("has_data"):
            # Data available but no buys → slight negative
            reasons.append("DB available — no recent insider buys in last 30 days")
            source_tags.append("db_no_insider_buy")
            return FactorDetail(score=35.0, status="real", reasons=reasons, source_tags=source_tags)

    # 2. News keyword fallback
    combined_news = " ".join(
        ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
        for item in (news or [])
    )
    buy_hits  = [kw for kw in _INSIDER_NEWS_KEYWORDS if kw in combined_news]
    sell_hits = [kw for kw in _INSIDER_SELL_KEYWORDS if kw in combined_news]

    if buy_hits and not sell_hits:
        score = 72.0
        reasons.append(f"Insider buy signal in recent news: {', '.join(buy_hits[:2])}")
        source_tags.append("news_insider_buy")
        status = "real"
    elif sell_hits and not buy_hits:
        score = 28.0
        reasons.append(f"Insider selling in recent news (mild negative)")
        source_tags.append("news_insider_sell")
    elif buy_hits and sell_hits:
        score = 48.0
        reasons.append("Mixed insider activity in recent news")
        source_tags.append("news_insider_mixed")
    else:
        # Large cap → neutral (insiders rarely buy meaningfully)
        if mkt_cap and mkt_cap > 20e9:
            score = 45.0
            reasons.append("Large cap — no insider buying signal (neutral)")
        else:
            score = 42.0
            reasons.append("No insider buying signal found — neutral estimate")
        status = "fallback"

    return FactorDetail(
        score=round(score, 1),
        status=status,
        reasons=reasons[:2],
        source_tags=source_tags,
    )


def _query_insider_db(ticker: str) -> Optional[Dict]:
    """
    Query insider_transactions table for recent open-market buys.
    Returns None if DB unavailable.
    """
    import os
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    db_url = os.getenv("NEON_DATABASE_URL") or os.getenv("DATABASE_URL")
    if not db_url:
        return None

    try:
        # Sanitize URL (remove channel_binding parameter that causes issues)
        try:
            parsed = urlparse(db_url)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            qs.pop("channel_binding", None)
            db_url = urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
        except Exception:
            pass

        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=3)
        cur = conn.cursor()
        # Check if table exists first
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'insider_transactions'
            )
        """)
        if not cur.fetchone()[0]:
            cur.close()
            conn.close()
            return None

        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE transaction_code = 'P') AS buys,
                MAX(conviction_score) FILTER (WHERE transaction_code = 'P') AS max_conv,
                SUM(total_value) FILTER (WHERE transaction_code = 'P') AS total_val,
                COUNT(*) AS total_records
            FROM insider_transactions
            WHERE ticker = %s
              AND transaction_date > NOW() - INTERVAL '30 days'
        """, (ticker.upper(),))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            return {
                "buys": int(row[0] or 0),
                "max_conviction": int(row[1] or 0),
                "total_value_usd": float(row[2] or 0),
                "has_data": int(row[3] or 0) > 0,
            }
    except Exception as e:
        print(f"[EXTENDED_FACTORS] insider_db query error for {ticker}: {e}")
    return None


# ── Policy Tailwind ───────────────────────────────────────────────────────────

# Map from theme → policy bucket + description
_THEME_POLICY_MAP: Dict[str, Dict] = {
    "ai_infrastructure":      {"score": 80, "bucket": "AI_CHIPS_ACT",
                               "label": "CHIPS Act + AI Executive Order — direct capex beneficiary"},
    "semicap_supply_chain":   {"score": 82, "bucket": "CHIPS_ACT_EQUIP",
                               "label": "CHIPS Act semiconductor equipment/materials — direct subsidy path"},
    "advanced_packaging_test":{"score": 75, "bucket": "CHIPS_ACT_PACK",
                               "label": "CHIPS Act advanced packaging — NSTC funding eligible"},
    "defense_optics":         {"score": 85, "bucket": "NDAA_DEFENSE",
                               "label": "NDAA defense spending + UAV/directed energy programs"},
    "space":                  {"score": 78, "bucket": "SPACE_POLICY",
                               "label": "Space Force + NASA commercial crew/cargo — government demand"},
    "grid_transformers":      {"score": 80, "bucket": "INFRASTRUCTURE_ACT",
                               "label": "Infrastructure Investment Act grid buildout — direct beneficiary"},
    "ai_power_energy":        {"score": 75, "bucket": "AI_POWER",
                               "label": "AI data center power demand — bipartisan infrastructure support"},
    "energy_transition":      {"score": 72, "bucket": "IRA",
                               "label": "Inflation Reduction Act clean energy credits"},
    "memory":                 {"score": 65, "bucket": "CHIPS_ACT_MEM",
                               "label": "CHIPS Act domestic memory manufacturing — subsidy eligible"},
    "photonics_cpo":          {"score": 60, "bucket": "AI_INFRA_INDIRECT",
                               "label": "Indirect AI infrastructure beneficiary — export control tailwind"},
    "neocloud":               {"score": 55, "bucket": "AI_CLOUD",
                               "label": "AI cloud infrastructure — indirect policy support"},
    "ai_software":            {"score": 45, "bucket": "AI_GENERAL",
                               "label": "General AI policy tailwind — less direct than hardware"},
    "biotech_catalyst":       {"score": 50, "bucket": "BARDA_FDA",
                               "label": "FDA fast-track / BARDA funding — drug-specific, variable"},
}

_POLICY_KEYWORDS: Dict[str, int] = {
    "chips act":                  82,
    "national security":          78,
    "onshoring":                  72,
    "reshoring":                  72,
    "domestic manufacturing":     70,
    "defense contract":           82,
    "department of defense":      80,
    "dod awarded":                80,
    "nato":                       72,
    "infrastructure act":         75,
    "ira":                        68,
    "inflation reduction":        68,
    "clean energy":               62,
    "export control":             70,
    "section 232":                75,
    "tariff protection":          68,
    "subsidized":                 62,
    "government funding":         65,
    "government grant":           68,
    "federal contract":           75,
}


def score_policy_tailwind(
    ticker: str,
    description: str,
    sector: Optional[str],
    matched_themes: List[str],
    news: List[Dict],
) -> "FactorDetail":
    """
    Score policy/macro tailwind. High = clear policy beneficiary.

    Sources:
      1. Theme → policy bucket mapping (from theme_alignment result)
      2. Description and news keyword scan
      3. Sector-level heuristic
    """
    from services.playbook.playbook_types import FactorDetail

    reasons: List[str] = []
    source_tags: List[str] = []
    desc_lower = (description or "").lower()

    combined_news = " ".join(
        ((item.get("headline") or "") + " " + (item.get("summary") or "")).lower()
        for item in (news or [])
    )
    all_text = desc_lower + " " + combined_news

    # 1. Theme → policy
    best_policy_score = 0.0
    best_policy_reason = ""
    matched_buckets: List[str] = []

    for theme in (matched_themes or []):
        pm = _THEME_POLICY_MAP.get(theme)
        if pm and pm["score"] > best_policy_score:
            best_policy_score = float(pm["score"])
            best_policy_reason = pm["label"]
            matched_buckets.append(pm["bucket"])

    # 2. Keyword scan
    kw_scores: List[int] = []
    matched_policy_kws: List[str] = []
    for kw, sc in _POLICY_KEYWORDS.items():
        if kw in all_text:
            kw_scores.append(sc)
            matched_policy_kws.append(kw)

    kw_score_val = 0.0
    if kw_scores:
        top2 = sorted(kw_scores, reverse=True)[:2]
        kw_score_val = sum(top2) / len(top2)

    # Combine: theme gives primary score, keywords can boost up to +10
    final_score = best_policy_score
    if kw_score_val > final_score:
        final_score = kw_score_val
    elif kw_score_val > 0:
        final_score = min(100.0, final_score + min(10.0, kw_score_val * 0.15))

    if best_policy_reason:
        reasons.append(best_policy_reason)
        source_tags.extend(matched_buckets[:2])
    if matched_policy_kws:
        reasons.append(f"Policy keywords: {', '.join(matched_policy_kws[:3])}")
        source_tags.append("policy_keyword")

    if not reasons:
        # Sector-level fallback
        sector_priors = {
            "Industrials": 60.0, "Energy": 58.0, "Technology": 52.0,
            "Healthcare": 48.0, "Utilities": 55.0,
        }
        final_score = sector_priors.get(sector or "", 40.0)
        reasons.append(f"No specific policy tailwind detected — sector baseline ({sector or 'general'})")
        return FactorDetail(score=round(final_score, 1), status="fallback", reasons=reasons, source_tags=[])

    return FactorDetail(
        score=round(min(100.0, final_score), 1),
        status="heuristic" if matched_themes else "heuristic",
        reasons=reasons[:3],
        source_tags=source_tags[:4],
    )
