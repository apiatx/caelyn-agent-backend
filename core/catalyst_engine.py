"""
Catalyst Strength Scorer.

Computes a structured catalyst score (0-100) with component breakdown.
Each component returns {score, present} — present is true ONLY when
verifiable underlying data exists. Missing data = present:false, score:0.

Components:
  - Earnings proximity (requires earnings date)
  - Fundamental acceleration (requires revenue/EPS growth data)
  - Volume expansion (requires current + avg volume)
  - Social acceleration (requires sentiment data with baseline)
  - News density (requires news articles)
  - Insider signal (requires insider API data)
  - Regulatory catalyst (requires news with regulatory keywords)
"""

from datetime import datetime


def calculate_catalyst_score(data_bundle: dict) -> dict:
    components = {}

    components["earnings_proximity"] = _score_earnings_proximity(data_bundle)
    components["fundamental_acceleration"] = _score_fundamental_acceleration(data_bundle)
    components["volume_expansion"] = _score_volume_expansion(data_bundle)
    components["social_acceleration"] = _score_social_acceleration(data_bundle)
    components["news_density"] = _score_news_density(data_bundle)
    components["insider_signal"] = _score_insider_signal(data_bundle)
    components["regulatory"] = _score_regulatory_catalyst(data_bundle)

    total = sum(c["score"] for c in components.values())
    total = max(0, min(100, total))
    present_count = sum(1 for c in components.values() if c["present"])

    return {
        "catalyst_score": total,
        "components": components,
        "present_count": present_count,
    }


def _score_earnings_proximity(data: dict) -> dict:
    overview = data.get("overview", {})
    if not isinstance(overview, dict):
        return {"score": 0, "present": False}

    earnings_date_str = overview.get("earnings_date") or overview.get("next_earnings")
    if not earnings_date_str:
        return {"score": 0, "present": False}

    score = 0
    try:
        for fmt in ("%Y-%m-%d", "%b %d, %Y", "%m/%d/%Y"):
            try:
                ed = datetime.strptime(str(earnings_date_str).split(" ")[0], fmt)
                days_away = (ed - datetime.now()).days
                if 0 <= days_away <= 7:
                    score = 20
                elif 7 < days_away <= 14:
                    score = 15
                elif 14 < days_away <= 30:
                    score = 8
                break
            except ValueError:
                continue
    except Exception:
        pass

    earnings_hist = data.get("earnings_history", [])
    if isinstance(earnings_hist, list) and earnings_hist:
        recent = earnings_hist[:4]
        beats = sum(1 for e in recent if isinstance(e, dict) and (e.get("surprise_pct") or 0) > 0)
        if beats >= 3:
            score = min(score + 5, 20)

    return {"score": score, "present": True}


def _score_fundamental_acceleration(data: dict) -> dict:
    overview = data.get("overview", {})
    if not isinstance(overview, dict):
        return {"score": 0, "present": False}

    rev_growth = overview.get("revenue_growth") or overview.get("revenue_growth_yoy")
    eps_growth = overview.get("eps_growth") or overview.get("earnings_growth")

    if not rev_growth and not eps_growth:
        return {"score": 0, "present": False}

    score = 0
    if rev_growth:
        try:
            val = float(str(rev_growth).replace("%", ""))
            if "%" not in str(rev_growth) and val < 5:
                val *= 100
            if val > 30:
                score += 20
            elif val > 20:
                score += 15
            elif val > 10:
                score += 8
        except (ValueError, TypeError):
            pass

    if eps_growth:
        try:
            val = float(str(eps_growth).replace("%", ""))
            if "%" not in str(eps_growth) and val < 5:
                val *= 100
            if val > 25:
                score += 5
        except (ValueError, TypeError):
            pass

    return {"score": min(score, 20), "present": True}


def _score_volume_expansion(data: dict) -> dict:
    snapshot = data.get("snapshot", {})
    details = data.get("details", {})
    technicals = data.get("technicals", {})

    volume = snapshot.get("volume")
    avg_vol = details.get("avg_volume") or technicals.get("avg_volume")

    if not volume or not avg_vol:
        return {"score": 0, "present": False}
    try:
        ratio = float(volume) / float(avg_vol)
        if ratio >= 3.0:
            return {"score": 15, "present": True}
        elif ratio >= 2.0:
            return {"score": 12, "present": True}
        elif ratio >= 1.5:
            return {"score": 8, "present": True}
        elif ratio >= 1.2:
            return {"score": 4, "present": True}
        return {"score": 0, "present": True}
    except (TypeError, ValueError, ZeroDivisionError):
        return {"score": 0, "present": False}


def _score_social_acceleration(data: dict) -> dict:
    has_data = False
    score = 0

    sentiment = data.get("sentiment", {})
    if isinstance(sentiment, dict) and sentiment:
        bull = sentiment.get("bull_pct") or sentiment.get("bullish_pct")
        watchers = sentiment.get("watchers_change") or sentiment.get("volume")
        if bull is not None or watchers is not None:
            has_data = True
            if bull:
                try:
                    val = float(bull)
                    if val > 80:
                        score += 10
                    elif val > 65:
                        score += 6
                except (TypeError, ValueError):
                    pass
            if watchers:
                try:
                    if float(watchers) > 0:
                        score += 3
                except (TypeError, ValueError):
                    pass

    x_sent = data.get("x_sentiment", {})
    if isinstance(x_sent, dict) and x_sent:
        x_score = x_sent.get("sentiment_score")
        if x_score is not None:
            has_data = True
            try:
                xs = float(x_score)
                if xs > 0.6:
                    score += 8
                elif xs > 0.3:
                    score += 4
            except (TypeError, ValueError):
                pass

    if not has_data:
        return {"score": 0, "present": False}
    return {"score": min(score, 18), "present": True}


def _score_news_density(data: dict) -> dict:
    CATALYST_KEYWORDS = [
        "fda", "approval", "partnership", "contract", "acquisition", "merger",
        "deal", "agreement", "license", "patent", "breakthrough",
        "launch", "revenue beat", "earnings beat", "guidance raised",
        "upgrade", "target raised", "buy rating",
        "government", "defense contract", "grant", "funding",
        "buyback", "repurchase",
        "phase 3", "clinical", "data readout",
    ]

    news = data.get("recent_news", [])
    if not isinstance(news, list) or not news:
        return {"score": 0, "present": False}

    news_text = " ".join(
        str(n.get("title", "") if isinstance(n, dict) else n)
        for n in news[:8]
    ).lower()

    hits = sum(1 for kw in CATALYST_KEYWORDS if kw in news_text)

    if hits >= 5:
        return {"score": 15, "present": True}
    elif hits >= 3:
        return {"score": 10, "present": True}
    elif hits >= 1:
        return {"score": 5, "present": True}
    return {"score": 0, "present": True}


def _score_insider_signal(data: dict) -> dict:
    insider = data.get("insider_sentiment", {})
    if not isinstance(insider, dict) or not insider:
        return {"score": 0, "present": False}

    mspr = insider.get("mspr") or insider.get("total_mspr")
    if mspr is None:
        return {"score": 0, "present": False}
    try:
        val = float(mspr)
        if val > 20:
            return {"score": 10, "present": True}
        elif val > 5:
            return {"score": 6, "present": True}
        elif val > 0:
            return {"score": 3, "present": True}
        return {"score": 0, "present": True}
    except (TypeError, ValueError):
        return {"score": 0, "present": False}


def _score_regulatory_catalyst(data: dict) -> dict:
    news = data.get("recent_news", [])
    if not isinstance(news, list) or not news:
        return {"score": 0, "present": False}

    REG_KEYWORDS = [
        "fda", "sec", "approval", "clearance", "authorized",
        "regulation", "policy", "executive order", "tariff",
        "sanctions", "ban", "mandate", "compliance",
    ]

    news_text = " ".join(
        str(n.get("title", "") if isinstance(n, dict) else n)
        for n in news[:5]
    ).lower()

    hits = sum(1 for kw in REG_KEYWORDS if kw in news_text)
    if hits >= 3:
        return {"score": 10, "present": True}
    elif hits >= 1:
        return {"score": 5, "present": True}
    return {"score": 0, "present": True}
