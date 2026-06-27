"""
Investor Mode — Theme Taxonomy.

Defines the 9 equity-relevant macro themes and their keyword/tag/category
matching rules.  This is the single source of truth for theme classification.

Adding a new theme: append to THEMES and add its keywords/tags/categories.
Each theme can be matched by:
  - question keywords (case-insensitive substring)
  - Polymarket tag substrings (case-insensitive)
  - Polymarket category names (exact, case-insensitive)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ThemeDef:
    theme_id: str
    theme_name: str
    description: str
    emoji: str
    question_keywords: list[str]
    tag_keywords: list[str]
    categories: list[str]
    base_relevance_boost: float = 0.0   # 0-20 bonus added to raw relevance
    # Sector / regime hints (used by impact_engine)
    bullish_bias_sectors: list[str] = field(default_factory=list)
    bearish_bias_sectors: list[str] = field(default_factory=list)


THEMES: list[ThemeDef] = [
    ThemeDef(
        theme_id="macro_rates_inflation",
        theme_name="Macro / Rates / Inflation",
        description="Federal Reserve policy, interest rates, CPI, PCE, yield curve, recession signals",
        emoji="📈",
        question_keywords=[
            "fed", "federal reserve", "fomc", "interest rate", "rate hike", "rate cut",
            "basis point", "inflation", "cpi", "pce", "deflation", "disinflation",
            "yield curve", "treasury", "10-year", "2-year", "inversion",
            "recession", "soft landing", "hard landing", "gdp", "stagflation",
            "quantitative tightening", "qt", "quantitative easing", "qe",
            "jerome powell", "powell", "janet yellen", "debt ceiling",
            "government shutdown", "fiscal", "deficit", "national debt",
        ],
        tag_keywords=[
            "federal-reserve", "fomc", "interest-rate", "inflation", "gdp",
            "recession", "economy", "macro", "rates", "treasury", "fed",
        ],
        categories=["Economy", "Finance"],
        base_relevance_boost=10.0,
        bullish_bias_sectors=["Financials", "REITs (rate cut)"],
        bearish_bias_sectors=["REITs (rate hike)", "Long-Duration Growth Tech"],
    ),

    ThemeDef(
        theme_id="geopolitics_war_trade",
        theme_name="Geopolitics / War / Trade / Sanctions",
        description="Armed conflicts, peace deals, trade wars, tariffs, sanctions, international relations",
        emoji="🌍",
        question_keywords=[
            "war", "conflict", "invasion", "attack", "strike", "ceasefire", "peace",
            "tariff", "trade war", "sanction", "embargo", "export ban",
            "nato", "nuclear", "missile", "bomb", "troops", "military",
            "russia", "ukraine", "israel", "iran", "hamas", "hezbollah",
            "middle east", "strait of hormuz", "red sea", "suez",
            "north korea", "kim", "taiwan strait",
            "g7", "g20", "un security", "bilateral", "wto",
        ],
        tag_keywords=[
            "war", "conflict", "geopolitics", "nato", "russia", "ukraine",
            "israel", "iran", "middle-east", "trade", "tariff", "sanctions",
            "north-korea", "peace", "ceasefire",
        ],
        categories=["Geopolitics"],
        base_relevance_boost=8.0,
        bullish_bias_sectors=["Defense/Aerospace", "Energy", "Cybersecurity"],
        bearish_bias_sectors=["Airlines/Transport", "Consumer Discretionary", "Industrials (global)"],
    ),

    ThemeDef(
        theme_id="energy_commodities",
        theme_name="Energy / Oil / Commodities",
        description="Crude oil, natural gas, OPEC, energy prices, metals, agricultural commodities",
        emoji="⛽",
        question_keywords=[
            "oil", "crude", "brent", "wti", "opec", "opec+", "natural gas", "lng",
            "gasoline", "petroleum", "refinery", "pipeline", "aramco",
            "gold", "silver", "copper", "metal", "commodity", "commodities",
            "wheat", "corn", "soybean", "agricultural",
            "energy price", "fuel", "coal",
        ],
        tag_keywords=[
            "oil", "energy", "opec", "commodity", "gold", "natural-gas",
            "lng", "crude", "metal", "petroleum",
        ],
        categories=["Economy", "Finance"],
        base_relevance_boost=7.0,
        bullish_bias_sectors=["Energy", "Gold/Metals/Commodities"],
        bearish_bias_sectors=["Airlines/Transport", "Consumer Discretionary"],
    ),

    ThemeDef(
        theme_id="us_politics_policy",
        theme_name="US Politics / Elections / Policy",
        description="US elections, White House, Congress, legislation, regulatory policy, executive orders",
        emoji="🏛️",
        question_keywords=[
            "trump", "biden", "harris", "democrat", "republican", "gop",
            "white house", "president", "election", "congress", "senate", "house of representatives",
            "legislation", "bill", "vote", "filibuster", "veto", "executive order",
            "doge", "elon musk", "doj", "sec", "ftc", "antitrust",
            "tax", "tax cut", "tax reform", "budget", "spending bill",
            "midterm", "primary", "ballot", "electoral college",
            "immigration", "border", "deportation",
        ],
        tag_keywords=[
            "trump", "democrat", "republican", "election", "us-politics",
            "congress", "senate", "president", "policy", "legislation",
            "doge", "elon",
        ],
        categories=["Politics", "Election"],
        base_relevance_boost=6.0,
        bullish_bias_sectors=["Defense/Aerospace", "Energy"],
        bearish_bias_sectors=["Clean Energy", "Financials (regulatory risk)"],
    ),

    ThemeDef(
        theme_id="ai_semis_tech",
        theme_name="AI / Semiconductors / Export Controls / Regulation",
        description="Artificial intelligence, GPU/chip supply, export controls, tech regulation, AI safety",
        emoji="🤖",
        question_keywords=[
            "artificial intelligence", "ai ", " ai", "chatgpt", "gpt", "openai",
            "anthropic", "claude", "gemini", "llm", "large language model",
            "machine learning", "deep learning",
            "nvidia", "nvda", "semiconductor", "chip", "gpu", "wafer",
            "tsmc", "intel", "amd", "qualcomm", "asml",
            "export control", "chip ban", "export restriction", "entity list",
            "tech regulation", "algorithm", "ai safety", "ai governance",
            "data center", "hyperscaler", "cloud compute",
        ],
        tag_keywords=[
            "ai", "artificial-intelligence", "semiconductor", "nvidia",
            "chips", "tech-regulation", "export-control", "openai",
            "chatgpt", "data-center",
        ],
        categories=["Tech", "Technology"],
        base_relevance_boost=9.0,
        bullish_bias_sectors=["Semiconductors", "AI Infra/Data Centers", "Cybersecurity"],
        bearish_bias_sectors=["Consumer Hardware", "Legacy Tech"],
    ),

    ThemeDef(
        theme_id="crypto_risk_appetite",
        theme_name="Crypto / Risk Appetite / Liquidity",
        description="Bitcoin, Ethereum, altcoins, crypto ETFs, risk sentiment, liquidity conditions",
        emoji="₿",
        question_keywords=[
            "bitcoin", "btc", "ethereum", "eth", "crypto", "cryptocurrency",
            "altcoin", "blockchain", "defi", "nft", "stablecoin", "tether", "usdc",
            "sec crypto", "crypto etf", "bitcoin etf", "spot etf",
            "coinbase", "binance", "kraken", "exchange hack",
            "bull run", "bear market", "risk-on", "risk-off", "liquidity",
        ],
        tag_keywords=[
            "bitcoin", "crypto", "ethereum", "defi", "blockchain",
            "altcoin", "nft", "stablecoin", "crypto-regulation",
        ],
        categories=["Crypto"],
        base_relevance_boost=5.0,
        bullish_bias_sectors=["Crypto Proxies", "Software/Growth Tech"],
        bearish_bias_sectors=["Traditional Finance/Banks (crypto stress)"],
    ),

    ThemeDef(
        theme_id="china_taiwan_supply_chain",
        theme_name="China / Taiwan / Supply Chain",
        description="China-US relations, Taiwan strait tension, TSMC, supply chain risk, decoupling",
        emoji="🇨🇳",
        question_keywords=[
            "china", "chinese", "prc", "xi jinping", "taiwan", "taiwanese",
            "taiwan strait", "pla", "invasion of taiwan", "taiwan independence",
            "tsmc", "supply chain", "decoupling", "friend-shoring", "nearshoring",
            "hong kong", "huawei", "tiktok", "bytedance",
            "sino-us", "trade deficit", "renminbi", "yuan",
        ],
        tag_keywords=[
            "china", "taiwan", "prc", "supply-chain", "tsmc",
            "huawei", "tiktok", "hong-kong",
        ],
        categories=["Geopolitics", "Tech"],
        base_relevance_boost=9.0,
        bullish_bias_sectors=["Defense/Aerospace", "Semiconductors (US domestic)", "Cybersecurity"],
        bearish_bias_sectors=["Consumer Electronics", "Industrials (China-exposed)", "Airlines (Asia routes)"],
    ),

    ThemeDef(
        theme_id="defense_security",
        theme_name="Defense / Security / Cybersecurity",
        description="Defense spending, military contracts, cybersecurity threats, intelligence",
        emoji="🛡️",
        question_keywords=[
            "defense budget", "defense spending", "pentagon", "military contract",
            "lockheed", "raytheon", "northrop", "general dynamics",
            "cybersecurity", "cyber attack", "hack", "ransomware", "malware",
            "nsa", "cia", "intelligence", "espionage", "surveillance",
            "drone", "hypersonic", "stealth", "aircraft carrier",
            "nato spending", "defense procurement",
        ],
        tag_keywords=[
            "defense", "military", "cybersecurity", "pentagon",
            "nato-spending", "hack", "cyber", "intelligence",
        ],
        categories=["Geopolitics"],
        base_relevance_boost=7.0,
        bullish_bias_sectors=["Defense/Aerospace", "Cybersecurity"],
        bearish_bias_sectors=[],
    ),

    ThemeDef(
        theme_id="consumer_labor_growth",
        theme_name="Consumer / Labor / Recession / Growth",
        description="Jobs report, unemployment, retail sales, consumer confidence, housing, GDP growth",
        emoji="👷",
        question_keywords=[
            "jobs", "unemployment", "nonfarm payroll", "payroll", "labor market",
            "consumer confidence", "retail sales", "consumer spending",
            "housing", "mortgage", "home price", "real estate",
            "pmi", "purchasing managers", "ism manufacturing",
            "gdp growth", "economic growth", "slowdown", "contraction",
            "consumer price", "wage", "income",
            "small business", "bankruptcy", "default",
        ],
        tag_keywords=[
            "jobs", "unemployment", "housing", "consumer", "gdp",
            "labor-market", "retail", "pmi", "recession",
        ],
        categories=["Economy"],
        base_relevance_boost=6.0,
        bullish_bias_sectors=["Consumer Discretionary", "Financials", "Small Caps"],
        bearish_bias_sectors=["REITs (rising rates)", "Long-Duration Bonds"],
    ),
]

# Fast lookup by theme_id
THEME_BY_ID: dict[str, ThemeDef] = {t.theme_id: t for t in THEMES}
THEME_IDS: list[str] = [t.theme_id for t in THEMES]
