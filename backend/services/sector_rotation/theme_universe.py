"""
Canonical Theme ETF universe for the Sectors > Themes layer.

This file is intentionally kept isolated so the list can be edited without
touching any route or service logic.  Import THEME_ETF_UNIVERSE wherever needed.

Structure per entry:
  key             — stable machine ID (used in API responses + caching)
  label           — human-readable display name
  symbols         — primary ETF ticker(s) to pull quotes/history for
  representative_tickers — optional: individual stocks that represent the theme
                            (used in prompt context only, no price fetching)
  parent_sector   — broad SPDR sector(s) this theme lives under
  theme_type      — investable narrative tag
"""
from __future__ import annotations

THEME_ETF_UNIVERSE: dict[str, dict] = {
    "semiconductors": {
        "label": "Semiconductors",
        "symbols": ["SMH", "SOXX", "XSD", "PSI"],
        "parent_sector": "Technology",
        "theme_type": "AI Supply Chain",
    },
    "semicap_equipment": {
        "label": "Semiconductor Equipment",
        "symbols": ["SOXX", "SMH"],
        "representative_tickers": ["ASML", "AMAT", "LRCX", "KLAC", "TER", "ACLS"],
        "parent_sector": "Technology",
        "theme_type": "Bottleneck",
    },
    "datacenter_infrastructure": {
        "label": "Data Center Infrastructure",
        "symbols": ["SRVR", "VPN", "DTCR"],
        "representative_tickers": ["EQIX", "DLR", "VRT", "ETN", "PWR", "CEG", "SMR"],
        "parent_sector": "Technology / Utilities / Industrials",
        "theme_type": "AI Infrastructure",
    },
    "cybersecurity": {
        "label": "Cybersecurity",
        "symbols": ["CIBR", "HACK", "BUG"],
        "parent_sector": "Technology",
        "theme_type": "Software/Security",
    },
    "cloud_software": {
        "label": "Cloud Software",
        "symbols": ["SKYY", "CLOU", "IGV", "WCLD"],
        "parent_sector": "Technology",
        "theme_type": "Software",
    },
    "aerospace_defense": {
        "label": "Aerospace & Defense",
        "symbols": ["ITA", "XAR", "PPA", "DFEN"],
        "parent_sector": "Industrials",
        "theme_type": "Defense",
    },
    "robotics_automation": {
        "label": "Robotics & Automation",
        "symbols": ["BOTZ", "ROBO", "ARKQ"],
        "parent_sector": "Industrials / Technology",
        "theme_type": "Automation",
    },
    "nuclear_uranium": {
        "label": "Nuclear & Uranium",
        "symbols": ["URA", "URNM", "NLR"],
        "parent_sector": "Energy / Utilities",
        "theme_type": "Power Bottleneck",
    },
    "copper_metals": {
        "label": "Copper & Industrial Metals",
        "symbols": ["COPX", "PICK", "DBB"],
        "parent_sector": "Materials",
        "theme_type": "Commodity Bottleneck",
    },
    "lithium_batteries": {
        "label": "Lithium & Batteries",
        "symbols": ["LIT", "BATT"],
        "parent_sector": "Materials / Industrials",
        "theme_type": "EV Supply Chain",
    },
    "oil_services": {
        "label": "Oil Services",
        "symbols": ["OIH", "XES", "IEZ"],
        "parent_sector": "Energy",
        "theme_type": "Energy Services",
    },
    "lng_gas": {
        "label": "LNG & Natural Gas",
        "symbols": ["FCG", "UNG", "BOIL"],
        "representative_tickers": ["LNG", "FLNG", "NEXT", "WMB", "KMI"],
        "parent_sector": "Energy",
        "theme_type": "Energy Infrastructure",
    },
    "homebuilders": {
        "label": "Homebuilders",
        "symbols": ["XHB", "ITB"],
        "parent_sector": "Consumer Discretionary",
        "theme_type": "Rates Sensitive",
    },
    "regional_banks": {
        "label": "Regional Banks",
        "symbols": ["KRE", "IAT"],
        "parent_sector": "Financials",
        "theme_type": "Rates/Credit",
    },
    "biotech": {
        "label": "Biotech",
        "symbols": ["XBI", "IBB", "ARKG"],
        "parent_sector": "Healthcare",
        "theme_type": "Risk-On Growth",
    },
    "medical_devices": {
        "label": "Medical Devices",
        "symbols": ["IHI"],
        "parent_sector": "Healthcare",
        "theme_type": "Healthcare Equipment",
    },
    "fintech": {
        "label": "Fintech",
        "symbols": ["FINX", "ARKF"],
        "parent_sector": "Financials / Technology",
        "theme_type": "Digital Finance",
    },
    "crypto_equities": {
        "label": "Crypto Equities / Blockchain",
        "symbols": ["BLOK", "BITQ", "BKCH", "WGMI"],
        "parent_sector": "Technology",
        "theme_type": "Crypto Beta",
    },
    "quantum": {
        "label": "Quantum Computing",
        "symbols": ["QTUM"],
        "representative_tickers": ["IONQ", "RGTI", "QUBT", "QBTS"],
        "parent_sector": "Technology",
        "theme_type": "Speculative Innovation",
    },
    "space": {
        "label": "Space Economy",
        "symbols": ["ARKX", "UFO"],
        "representative_tickers": ["RKLB", "ASTS", "LUNR", "SATL", "SPIR"],
        "parent_sector": "Industrials / Technology",
        "theme_type": "Space",
    },
}

# Flat de-duplicated list of all ETF symbols referenced in the universe.
# Used for batch quote/history fetching.
ALL_THEME_SYMBOLS: list[str] = sorted(
    set(sym for v in THEME_ETF_UNIVERSE.values() for sym in v["symbols"])
)
