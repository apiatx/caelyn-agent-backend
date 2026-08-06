"""
Canonical Theme & Sector Registry — Taxonomy v2
================================================

Taxonomy v2 model:
  One actual economic sector
  + One primary thematic membership  (theme OR sub_theme)
  + Zero or more additional thematic memberships

Hierarchy ceiling:  Sector → Theme → Subtheme
No subthemes beneath subthemes.

110 total entries:
  11 SPDR broad sectors           (classification="sector")
  23 top-level parent themes      (classification="theme")
  ~65 child subthemes             (classification="sub_theme")
   3 commodity market lenses      (classification="market_lens",  assignable=False)
   8 deprecated structural nodes  (classification="deprecated",   assignable=False)

Classification rules:
  sector      – the 11 GICS/SPDR sectors; never assigned as theme membership
  theme       – top-level thematic parent; parent_theme_id=None
  sub_theme   – child of exactly one theme; parent_theme_id required
  market_lens – Gold/Silver/Copper commodity proxy series; not assignable as theme
  deprecated  – retired node; kept for backward-compat alias resolution only

Per-entry fields:
  classification    "sector"|"theme"|"sub_theme"|"market_lens"|"deprecated"
  display_name      human-readable label
  assignable        False for market_lens and deprecated (defaults True)
  parent_sector     sector theme_id this entry belongs under (sectors: None)
  parent_theme_id   parent theme ID for sub_themes (others: None)
  rollup_sector_ids explicit cross-sector rollup (overrides inherited chain)
  proxy_type        "etf"|"basket"|"custom"|"hybrid"
  proxy_symbols     ETF/index tickers for performance series (primary first)
  candidate_symbols stocks for leader/laggard discovery (static seeds only)
  description       short description for AI classification and user display
  keywords          NLP/search tags
  aliases           backward-compat ID aliases (prior display labels / IDs)
"""
from __future__ import annotations

THEME_RS_UNIVERSE: dict[str, dict] = {

    # ══════════════════════════════════════════════════════════════════════════
    # 11 SPDR BROAD SECTORS  (classification="sector")
    # ══════════════════════════════════════════════════════════════════════════

    "communication_services": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Communication Services",
        "proxy_type": "etf",
        "proxy_symbols": ["XLC", "VOX"],
        "candidate_symbols": ["META", "GOOGL", "NFLX", "DIS", "T", "VZ", "CMCSA"],
        "description": "Telecom, media, social media, entertainment, and advertising companies.",
        "keywords": ["communication", "social media", "streaming", "telecom", "advertising"],
    },

    "consumer_discretionary": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Consumer Discretionary",
        "proxy_type": "etf",
        "proxy_symbols": ["XLY", "VCR"],
        "candidate_symbols": ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX"],
        "description": "Non-essential consumer goods and services: retail, autos, restaurants, leisure.",
        "keywords": ["consumer discretionary", "retail", "autos", "restaurants", "leisure"],
    },

    "consumer_staples": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Consumer Staples",
        "proxy_type": "etf",
        "proxy_symbols": ["XLP", "VDC"],
        "candidate_symbols": ["PG", "KO", "PEP", "WMT", "COST", "PM", "MO"],
        "description": "Essential consumer products: food, beverages, household goods, tobacco.",
        "keywords": ["consumer staples", "food", "beverages", "household", "essential"],
    },

    "energy": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["XLE", "VDE"],
        "candidate_symbols": ["XOM", "CVX", "COP", "EOG", "SLB", "OXY", "PSX"],
        "description": "Oil, gas, coal, and energy equipment companies.",
        "keywords": ["energy", "oil", "gas", "fossil fuel", "exploration"],
    },

    "financials": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Financials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLF", "VFH"],
        "candidate_symbols": ["JPM", "BAC", "WFC", "GS", "MS", "BRK.B", "V", "MA"],
        "description": "Banks, insurance, asset managers, fintech, and diversified financial services.",
        "keywords": ["financials", "banking", "insurance", "payments", "capital markets"],
    },

    "healthcare": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Health Care",
        "proxy_type": "etf",
        "proxy_symbols": ["XLV", "VHT"],
        "candidate_symbols": ["JNJ", "UNH", "LLY", "ABBV", "MRK", "TMO", "ABT"],
        "description": "Pharmaceuticals, biotech, medical devices, healthcare services, and life science tools.",
        "keywords": ["healthcare", "pharma", "biotech", "medical", "health insurance"],
    },

    "industrials": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Industrials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLI", "VIS"],
        "candidate_symbols": ["GE", "HON", "UNP", "BA", "CAT", "RTX", "LMT"],
        "description": "Aerospace, defense, machinery, transportation, construction, and industrial services.",
        "keywords": ["industrials", "aerospace", "machinery", "transportation", "construction"],
    },

    "materials": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Materials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLB", "VAW"],
        "candidate_symbols": ["LIN", "APD", "FCX", "NEM", "NUE", "ALB", "ECL"],
        "description": "Chemicals, construction materials, metals, mining, and forest products.",
        "keywords": ["materials", "chemicals", "metals", "mining", "construction materials"],
    },

    "real_estate": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Real Estate",
        "proxy_type": "etf",
        "proxy_symbols": ["XLRE", "VNQ"],
        "candidate_symbols": ["PLD", "AMT", "EQIX", "SPG", "CCI", "PSA", "DLR"],
        "description": "Real estate investment trusts (REITs), commercial real estate, residential.",
        "keywords": ["real estate", "REIT", "commercial property", "residential", "industrial REIT"],
    },

    "technology": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Technology",
        "proxy_type": "etf",
        "proxy_symbols": ["XLK", "VGT"],
        "candidate_symbols": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "AMD", "QCOM"],
        "description": "Software, hardware, semiconductors, IT services, and technology equipment.",
        "keywords": ["technology", "software", "semiconductors", "hardware", "IT services"],
    },

    "utilities": {
        "classification": "sector",
        "parent_sector": None,
        "display_name": "Utilities",
        "proxy_type": "etf",
        "proxy_symbols": ["XLU", "VPU"],
        "candidate_symbols": ["NEE", "DUK", "SO", "D", "EXC", "AEP", "SRE"],
        "description": "Electric, water, gas utilities, and independent power producers.",
        "keywords": ["utilities", "electric", "water", "gas utility", "power generation"],
    },

    # ══════════════════════════════════════════════════════════════════════════
    # PARENT THEMES (classification="theme", parent_theme_id=None)
    # ══════════════════════════════════════════════════════════════════════════

    # ── 1. Agribusiness ───────────────────────────────────────────────────────
    "agribusiness": {
        "classification": "theme",
        "parent_sector": "consumer_staples",
        "display_name": "Agribusiness",
        "proxy_type": "etf",
        "proxy_symbols": ["MOO", "DBA", "VEGI"],
        "candidate_symbols": ["DE", "MOS", "ADM", "BG", "CF", "NTR", "FMC", "CTVA"],
        "rollup_sector_ids": ["consumer_staples", "materials", "industrials"],
        "description": "Agricultural inputs, farm equipment, crop protection, and food production businesses.",
        "keywords": ["agriculture", "fertilizer", "crop", "farming", "food supply", "agchem"],
    },

    # ── 2. Consumer & Housing ─────────────────────────────────────────────────
    "consumer_housing": {
        "classification": "theme",
        "parent_sector": "consumer_discretionary",
        "display_name": "Consumer & Housing",
        "proxy_type": "etf",
        "proxy_symbols": ["XRT", "ITB"],
        "candidate_symbols": ["WMT", "COST", "DHI", "LEN", "AMZN", "TJX", "PHM"],
        "rollup_sector_ids": ["consumer_discretionary", "consumer_staples", "real_estate"],
        "description": "Consumer retail and housing/homebuilder businesses.",
        "keywords": ["consumer", "retail", "housing", "homebuilder", "e-commerce"],
    },

    # ── 3. Banking (promoted from sub_theme; ID preserved) ────────────────────
    "banks": {
        "classification": "theme",
        "parent_sector": "financials",
        "display_name": "Banking",
        "proxy_type": "etf",
        "proxy_symbols": ["KBE", "KRE", "XLF"],
        "candidate_symbols": ["JPM", "BAC", "WFC", "GS", "MS", "C"],
        "rollup_sector_ids": ["financials"],
        "description": "Commercial banks, investment banks, and diversified banking businesses.",
        "keywords": ["banking", "credit", "lending", "deposits", "interest rates", "bank"],
        "aliases": ["banking"],
    },

    # ── 4. Insurance (promoted from sub_theme; ID preserved) ─────────────────
    "insurance": {
        "classification": "theme",
        "parent_sector": "financials",
        "display_name": "Insurance",
        "proxy_type": "etf",
        "proxy_symbols": ["KIE", "IAK"],
        "candidate_symbols": ["PGR", "CB", "ACGL", "ALL", "MET", "TRV", "AFL"],
        "rollup_sector_ids": ["financials"],
        "description": "Property & casualty, life insurance, reinsurance, and specialty insurance.",
        "keywords": ["insurance", "P&C", "reinsurance", "underwriting", "annuity"],
    },

    # ── 5. Fintech & Digital Payments (promoted; ID preserved) ────────────────
    "fintech": {
        "classification": "theme",
        "parent_sector": "financials",
        "display_name": "Fintech & Digital Payments",
        "proxy_type": "etf",
        "proxy_symbols": ["FINX", "IPAY", "ARKF"],
        "candidate_symbols": ["V", "MA", "SQ", "PYPL", "SOFI", "HOOD", "AFRM", "NU"],
        "rollup_sector_ids": ["financials", "technology"],
        "description": "Digital payments, embedded finance, neobanks, buy-now-pay-later, and fintech infrastructure.",
        "keywords": ["fintech", "payments", "digital banking", "neobank", "BNPL", "crypto payments"],
    },

    # ── 6. Healthcare Innovation ──────────────────────────────────────────────
    "healthcare_innovation": {
        "classification": "theme",
        "parent_sector": "healthcare",
        "display_name": "Healthcare Innovation",
        "proxy_type": "etf",
        "proxy_symbols": ["XBI", "IBB", "IHI"],
        "candidate_symbols": ["LLY", "MRNA", "REGN", "VRTX", "ISRG", "MDT", "ABT"],
        "rollup_sector_ids": ["healthcare"],
        "description": "Biotech drug development, medical devices, diagnostics, and life-science tools.",
        "keywords": ["biotech", "medical devices", "healthcare innovation", "diagnostics", "life science"],
    },

    # ── 7. Metals & Mining ────────────────────────────────────────────────────
    "metals_mining": {
        "classification": "theme",
        "parent_sector": "materials",
        "display_name": "Metals & Mining",
        "proxy_type": "etf",
        "proxy_symbols": ["XME", "PICK", "SLX"],
        "candidate_symbols": ["FCX", "CLF", "AA", "NUE", "STLD", "NEM", "GOLD", "PAAS"],
        "rollup_sector_ids": ["materials"],
        "description": "Metals extraction and primary mining: precious metals, base metals, bulk commodities.",
        "keywords": ["metals", "mining", "gold", "silver", "copper", "steel", "iron ore"],
    },

    # ── 8. Advanced Materials ─────────────────────────────────────────────────
    "advanced_materials": {
        "classification": "theme",
        "parent_sector": "materials",
        "display_name": "Advanced Materials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLB", "IYM"],
        "candidate_symbols": ["APD", "LIN", "DD", "ECL", "SHW", "AMCR", "EMN"],
        "rollup_sector_ids": ["materials", "industrials", "technology"],
        "description": "Engineered, specialty, and high-performance materials: alloys, composites, electronic materials, polymers.",
        "keywords": ["specialty chemicals", "engineered materials", "composites", "polymers", "advanced alloys"],
        "aliases": ["chemicals_materials_advanced"],
    },

    # ── 9. Semiconductors ─────────────────────────────────────────────────────
    "semiconductors": {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Semiconductors",
        "proxy_type": "etf",
        "proxy_symbols": ["SMH", "SOXX", "XSD", "PSI"],
        "candidate_symbols": ["NVDA", "AMD", "INTC", "QCOM", "TSM", "AVGO", "ASML", "AMAT"],
        "rollup_sector_ids": ["technology"],
        "description": "Semiconductor chip design, foundry, equipment, materials, and packaging.",
        "keywords": ["semiconductors", "chips", "GPU", "CPU", "fab", "wafer", "silicon"],
    },

    # ── 10. Data Center Infrastructure (promoted from sub_theme; ID preserved) ─
    "datacenter_infra": {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Data Center Infrastructure",
        "proxy_type": "etf",
        "proxy_symbols": ["SRVR", "DTCR"],
        "candidate_symbols": ["EQIX", "DLR", "VRT", "SMCI", "ANET", "NFLX", "IRM"],
        "rollup_sector_ids": ["technology", "utilities", "real_estate"],
        "description": "Physical infrastructure for data centers: networking, optical, servers, power, cooling, operators.",
        "keywords": ["data center", "DC infrastructure", "networking", "optical", "server", "power"],
    },

    # ── 11. Software ──────────────────────────────────────────────────────────
    "software": {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Software",
        "proxy_type": "etf",
        "proxy_symbols": ["IGV", "WCLD"],
        "candidate_symbols": ["CRM", "NOW", "DDOG", "SNOW", "MDB", "PANW", "ZS"],
        "rollup_sector_ids": ["technology"],
        "description": "Enterprise software, cloud SaaS, cybersecurity, and AI software platforms.",
        "keywords": ["software", "SaaS", "cloud software", "cybersecurity", "AI platform"],
    },

    # ── 12. Photonics & Optical Systems ───────────────────────────────────────
    "photonics_optical": {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Photonics & Optical Systems",
        "proxy_type": "custom",
        "proxy_symbols": ["IPGP", "COHR", "LITE", "VIAV", "IIVI"],
        "candidate_symbols": ["IPGP", "COHR", "LITE", "AAOI", "VIAV", "FN", "LPTH", "LASR"],
        "rollup_sector_ids": ["technology", "industrials"],
        "description": "Photonics, laser systems, optical components, and sensing technology.",
        "keywords": ["photonics", "lasers", "optical components", "silicon photonics", "LiDAR"],
    },

    # ── 13. Defense & Aerospace (ID "defense" preserved; display renamed) ─────
    "defense": {
        "classification": "theme",
        "parent_sector": "industrials",
        "display_name": "Defense & Aerospace",
        "proxy_type": "etf",
        "proxy_symbols": ["ITA", "XAR", "PPA"],
        "candidate_symbols": ["LMT", "RTX", "NOC", "GD", "L3H", "KTOS", "AVAV"],
        "rollup_sector_ids": ["industrials"],
        "description": "Defense platforms, weapons systems, aerospace, and defense electronics.",
        "keywords": ["defense", "aerospace", "weapons", "military", "contractor", "radar", "EW"],
        "aliases": ["defense_aerospace"],
    },

    # ── 14. Space Economy (promoted from sub_theme; ID "space" preserved) ─────
    "space": {
        "classification": "theme",
        "parent_sector": "industrials",
        "display_name": "Space Economy",
        "proxy_type": "etf",
        "proxy_symbols": ["ARKX", "UFO"],
        "candidate_symbols": ["RKLB", "ASTS", "LUNR", "SPIR", "SATL", "GNSS", "IRDM"],
        "rollup_sector_ids": ["industrials", "communication_services", "technology"],
        "description": "Launch vehicles, satellites, space operations, and commercial space services.",
        "keywords": ["space", "launch", "satellite", "space economy", "orbital", "LEO"],
        "aliases": ["space_economy"],
    },

    # ── 15. Clean Energy ──────────────────────────────────────────────────────
    "clean_energy": {
        "classification": "theme",
        "parent_sector": "utilities",
        "display_name": "Clean Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["ICLN", "PBW", "QCLN", "CNRG"],
        "candidate_symbols": ["NEE", "FSLR", "ENPH", "PLUG", "RUN", "CWEN", "BE"],
        "rollup_sector_ids": ["utilities", "energy", "industrials"],
        "description": "Renewable energy generation, storage, and hydrogen technology companies.",
        "keywords": ["clean energy", "renewable", "solar", "wind", "hydrogen", "battery storage", "ESG"],
    },

    # ── 16. Grid & Electrification ────────────────────────────────────────────
    "grid_electrification": {
        "classification": "theme",
        "parent_sector": "industrials",
        "display_name": "Grid & Electrification",
        "proxy_type": "custom",
        "proxy_symbols": ["GRID", "ETN", "EMR", "PWR"],
        "candidate_symbols": ["ETN", "EMR", "PWR", "HUBB", "AZZ", "ACNB", "GEV", "GNRC"],
        "rollup_sector_ids": ["industrials", "utilities"],
        "description": "Electrical grid hardware, power generation equipment, transmission, and backup/distributed power.",
        "keywords": ["grid", "electrification", "electrical equipment", "power generation", "transmission", "turbines"],
    },

    # ── 17. Nuclear Energy ────────────────────────────────────────────────────
    "nuclear_energy": {
        "classification": "theme",
        "parent_sector": "utilities",
        "display_name": "Nuclear Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["URA", "URNM", "NLR"],
        "candidate_symbols": ["CCJ", "LEU", "BWXT", "SMR", "OKLO", "UUUU", "GEV"],
        "rollup_sector_ids": ["utilities", "energy", "industrials", "materials"],
        "description": "Nuclear fuel, uranium mining, reactor equipment, SMRs, and nuclear utility operators.",
        "keywords": ["nuclear", "uranium", "reactor", "SMR", "nuclear power", "nuclear fuel"],
    },

    # ── 18. Oil & Gas ─────────────────────────────────────────────────────────
    "oil_gas": {
        "classification": "theme",
        "parent_sector": "energy",
        "display_name": "Oil & Gas",
        "proxy_type": "etf",
        "proxy_symbols": ["XOP", "XLE", "OIH"],
        "candidate_symbols": ["XOM", "CVX", "COP", "SLB", "EOG", "HAL", "DVN"],
        "rollup_sector_ids": ["energy"],
        "description": "Exploration, production, integrated oil, LNG, midstream, and oil services.",
        "keywords": ["oil", "gas", "E&P", "upstream", "LNG", "midstream", "oil services", "refining"],
    },

    # ── 19. Industrial Automation ─────────────────────────────────────────────
    "industrial_automation": {
        "classification": "theme",
        "parent_sector": "industrials",
        "display_name": "Industrial Automation",
        "proxy_type": "etf",
        "proxy_symbols": ["BOTZ", "ROBO", "IRBO"],
        "candidate_symbols": ["SYM", "ISRG", "TER", "HON", "ROK", "ABB", "FANUY"],
        "rollup_sector_ids": ["industrials", "technology"],
        "description": "Industrial robots, automation systems, process control, and factory sensors.",
        "keywords": ["robotics", "automation", "industrial robots", "cobots", "process control", "sensors"],
    },

    # ── 20. Construction & Infrastructure ────────────────────────────────────
    "construction_infrastructure": {
        "classification": "theme",
        "parent_sector": "industrials",
        "display_name": "Construction & Infrastructure",
        "proxy_type": "etf",
        "proxy_symbols": ["PKB", "PAVE", "XHB"],
        "candidate_symbols": ["CAT", "DE", "VMC", "MLM", "PWR", "FLR", "J", "MTZ"],
        "rollup_sector_ids": ["industrials", "materials"],
        "description": "Engineering, heavy construction, infrastructure projects, building products, and heavy equipment.",
        "keywords": ["construction", "infrastructure", "engineering", "building", "heavy equipment"],
    },

    # ── 21. Transportation & Mobility ─────────────────────────────────────────
    "transportation_mobility": {
        "classification": "theme",
        "parent_sector": "industrials",
        "display_name": "Transportation & Mobility",
        "proxy_type": "etf",
        "proxy_symbols": ["IYT", "XTN"],
        "candidate_symbols": ["UNP", "CSX", "UPS", "FDX", "UAL", "DAL", "UBER"],
        "rollup_sector_ids": ["industrials", "consumer_discretionary"],
        "description": "Travel, freight, logistics, rail, shipping, and passenger mobility.",
        "keywords": ["transportation", "travel", "logistics", "freight", "rail", "airline", "shipping"],
    },

    # ── 22. Quantum Computing (promoted from sub_theme; ID "quantum" preserved) ─
    "quantum": {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Quantum Computing",
        "proxy_type": "custom",
        "proxy_symbols": ["IONQ", "RGTI", "QUBT"],
        "candidate_symbols": ["IONQ", "RGTI", "QUBT", "QBTS", "ARQQ", "IBM"],
        "rollup_sector_ids": ["technology"],
        "description": "Quantum computing hardware, software, and quantum networking.",
        "keywords": ["quantum computing", "qubit", "quantum hardware", "quantum software", "quantum error correction"],
        "aliases": ["quantum_computing"],
    },

    # ── 23. Crypto Equities / Blockchain ──────────────────────────────────────
    "crypto_equities": {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Crypto Equities / Blockchain",
        "proxy_type": "etf",
        "proxy_symbols": ["BLOK", "BITQ", "WGMI", "BKCH"],
        "candidate_symbols": ["MSTR", "COIN", "MARA", "CLSK", "HUT", "RIOT", "IREN"],
        "rollup_sector_ids": ["technology", "financials"],
        "description": "Bitcoin miners, digital asset platforms, and blockchain infrastructure equities.",
        "keywords": ["bitcoin", "crypto", "blockchain", "digital assets", "mining", "DeFi"],
    },

    # ══════════════════════════════════════════════════════════════════════════
    # SUB-THEMES (classification="sub_theme", grouped by parent)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Under Agribusiness ────────────────────────────────────────────────────
    "agri_inputs_fertilizers": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "agribusiness",
        "display_name": "Agricultural Inputs & Fertilizers",
        "proxy_type": "etf",
        "proxy_symbols": ["SOIL", "MOO"],
        "candidate_symbols": ["MOS", "NTR", "CF", "ICL", "IPI", "CTVA", "FMC"],
        "rollup_sector_ids": ["materials", "consumer_staples"],
        "description": "Fertilizers, crop chemicals, and agricultural input companies.",
        "keywords": ["fertilizer", "crop protection", "agchem", "potash", "nitrogen", "phosphate"],
    },

    "farm_machinery": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "agribusiness",
        "display_name": "Farm Machinery & Equipment",
        "proxy_type": "custom",
        "proxy_symbols": ["DE", "AGCO", "CNH"],
        "candidate_symbols": ["DE", "AGCO", "CNH", "TRIMB", "LNN", "AZZ"],
        "rollup_sector_ids": ["industrials", "consumer_staples"],
        "description": "Agricultural machinery, precision farming equipment, and related services.",
        "keywords": ["farm equipment", "precision agriculture", "tractors", "combines", "irrigation"],
    },

    "food_producers": {
        "classification": "sub_theme",
        "parent_sector": "consumer_staples",
        "parent_theme_id": "agribusiness",
        "display_name": "Food Producers & Processors",
        "proxy_type": "etf",
        "proxy_symbols": ["MOO", "VEGI"],
        "candidate_symbols": ["ADM", "BG", "INGR", "LAND", "TSN", "HRL", "SFM"],
        "rollup_sector_ids": ["consumer_staples", "materials"],
        "description": "Grain processors, food manufacturers, and agricultural commodity traders.",
        "keywords": ["food processing", "grain", "milling", "agricultural commodity", "food manufacturer"],
    },

    # ── Under Consumer & Housing ──────────────────────────────────────────────
    "consumer_retail": {
        "classification": "sub_theme",
        "parent_sector": "consumer_discretionary",
        "parent_theme_id": "consumer_housing",
        "display_name": "Consumer Retail",
        "proxy_type": "etf",
        "proxy_symbols": ["XRT", "RTH"],
        "candidate_symbols": ["WMT", "COST", "TGT", "TJX", "AMZN", "ROST", "DLTR"],
        "rollup_sector_ids": ["consumer_discretionary", "consumer_staples"],
        "description": "General merchandise retailers, e-commerce, specialty retail, and discount chains.",
        "keywords": ["retail", "consumer spending", "e-commerce", "big box", "discount", "merchandise"],
    },

    "homebuilders": {
        "classification": "sub_theme",
        "parent_sector": "consumer_discretionary",
        "parent_theme_id": "consumer_housing",
        "display_name": "Homebuilders",
        "proxy_type": "etf",
        "proxy_symbols": ["ITB", "XHB"],
        "candidate_symbols": ["DHI", "LEN", "PHM", "TOL", "NVR", "MDC", "KBH"],
        "rollup_sector_ids": ["consumer_discretionary", "real_estate"],
        "description": "Residential homebuilders and related building materials and services.",
        "keywords": ["homebuilders", "residential construction", "housing", "new homes"],
    },

    # ── Under Banking ─────────────────────────────────────────────────────────
    "regional_banks": {
        "classification": "sub_theme",
        "parent_sector": "financials",
        "parent_theme_id": "banks",
        "display_name": "Regional Banks",
        "proxy_type": "etf",
        "proxy_symbols": ["KRE", "IAT"],
        "candidate_symbols": ["WAL", "ZION", "CMA", "KEY", "RF", "HBAN", "FHN"],
        "rollup_sector_ids": ["financials"],
        "description": "US regional and community banks with concentrated geographic deposit bases.",
        "keywords": ["regional banks", "community banking", "CRE", "deposits", "NIM"],
    },

    # ── Under Healthcare Innovation ───────────────────────────────────────────
    "biotech": {
        "classification": "sub_theme",
        "parent_sector": "healthcare",
        "parent_theme_id": "healthcare_innovation",
        "display_name": "Biotech",
        "proxy_type": "etf",
        "proxy_symbols": ["XBI", "IBB", "ARKG"],
        "candidate_symbols": ["MRNA", "REGN", "VRTX", "BIIB", "GILD", "ALNY", "BMRN"],
        "rollup_sector_ids": ["healthcare"],
        "description": "Biopharmaceutical drug development, gene therapy, and precision medicine.",
        "keywords": ["biotech", "biopharma", "clinical trials", "FDA", "drug development", "gene therapy"],
    },

    "medical_devices": {
        "classification": "sub_theme",
        "parent_sector": "healthcare",
        "parent_theme_id": "healthcare_innovation",
        "display_name": "Medical Devices",
        "proxy_type": "etf",
        "proxy_symbols": ["IHI"],
        "candidate_symbols": ["MDT", "ABT", "SYK", "EW", "ISRG", "BDX", "GEHC", "ZBH"],
        "rollup_sector_ids": ["healthcare"],
        "description": "Medical instruments, surgical systems, implants, and diagnostic equipment.",
        "keywords": ["medical devices", "surgical", "implants", "diagnostics equipment", "cardiac"],
    },

    "diagnostics_life_science": {
        "classification": "sub_theme",
        "parent_sector": "healthcare",
        "parent_theme_id": "healthcare_innovation",
        "display_name": "Diagnostics & Life Science Tools",
        "proxy_type": "etf",
        "proxy_symbols": ["XBI", "IHI"],
        "candidate_symbols": ["TMO", "DHR", "A", "ILMN", "BGNE", "BIO", "EXAS"],
        "rollup_sector_ids": ["healthcare"],
        "description": "In-vitro diagnostics, genomics sequencing, lab instruments, and life-science tools.",
        "keywords": ["diagnostics", "genomics", "life science tools", "lab instruments", "sequencing"],
    },

    # ── Under Metals & Mining ─────────────────────────────────────────────────
    "precious_metals": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",
        "display_name": "Precious Metals",
        "proxy_type": "etf",
        "proxy_symbols": ["GDX", "GDXJ", "SIL"],
        "candidate_symbols": ["NEM", "GOLD", "AEM", "AUY", "PAAS", "AG", "MAG", "FNV"],
        "rollup_sector_ids": ["materials"],
        "description": "Gold miners, silver producers, platinum-group metals, and royalty/streaming companies.",
        "keywords": ["gold miners", "silver miners", "precious metals", "royalty streaming", "PGMs"],
    },

    "base_metals_diversified": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",
        "display_name": "Base Metals & Diversified Mining",
        "proxy_type": "etf",
        "proxy_symbols": ["COPX", "DBB", "PICK"],
        "candidate_symbols": ["FCX", "SCCO", "TECK", "HBM", "BHP", "RIO", "VALE"],
        "rollup_sector_ids": ["materials"],
        "description": "Copper, nickel, zinc, aluminum, iron ore, and diversified mining portfolios.",
        "keywords": ["copper", "nickel", "zinc", "aluminum", "diversified mining", "base metals"],
        "aliases": ["copper_miners_structural"],
    },

    "rare_earth": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",
        "display_name": "Rare Earth Elements",
        "proxy_type": "etf",
        "proxy_symbols": ["REMX"],
        "candidate_symbols": ["MP", "UUUU", "TMRC", "LYSDY", "NB", "AMRK"],
        "rollup_sector_ids": ["materials"],
        "description": "Rare earth elements, magnet materials, graphite, antimony, and strategic mineral supply chains.",
        "keywords": ["rare earth", "critical minerals", "magnets", "graphite", "antimony", "defense supply chain"],
        "aliases": ["rare_earth_metals", "rare_earths_strategic_minerals"],
    },

    "lithium": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",
        "display_name": "Lithium",
        "proxy_type": "custom",
        "proxy_symbols": ["LIT", "ALB", "SQM"],
        "candidate_symbols": ["ALB", "SQM", "LAC", "PLL", "LTHM", "SGML", "LITKF"],
        "rollup_sector_ids": ["materials"],
        "description": "Lithium extraction, hard-rock and brine production, refining, and chemical conversion.",
        "keywords": ["lithium", "lithium mining", "brines", "hard rock lithium", "lithium carbonate"],
    },

    "steel_ferrous": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",
        "display_name": "Steel & Ferrous Metals",
        "proxy_type": "etf",
        "proxy_symbols": ["SLX", "XME"],
        "candidate_symbols": ["NUE", "STLD", "CLF", "X", "CMC", "RS", "MTL"],
        "rollup_sector_ids": ["materials"],
        "description": "Steelmakers, ferrous-metal processors, and metallurgical feedstock businesses.",
        "keywords": ["steel", "ferrous metals", "iron ore", "steelmaker", "mini-mill", "metallurgical"],
    },

    # ── Under Advanced Materials ──────────────────────────────────────────────
    "specialty_alloys": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "advanced_materials",
        "display_name": "Specialty Alloys & Engineered Metals",
        "proxy_type": "custom",
        "proxy_symbols": ["ATI", "CRS", "HWM"],
        "candidate_symbols": ["ATI", "CRS", "HWM", "TIE", "KALU", "HAYN"],
        "rollup_sector_ids": ["materials", "industrials"],
        "description": "High-performance alloys, titanium, superalloys, and engineered metal products.",
        "keywords": ["specialty alloys", "titanium", "superalloys", "high performance metals", "nickel alloys"],
    },

    "electronic_materials": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "advanced_materials",
        "display_name": "Electronic & Semiconductor Materials",
        "proxy_type": "custom",
        "proxy_symbols": ["ENTG", "MKSI", "CMC"],
        "candidate_symbols": ["ENTG", "MKSI", "CMC", "FTEK", "MTSN", "AZTA"],
        "rollup_sector_ids": ["materials", "technology"],
        "description": "Wafer chemicals, electronic gases, CMP slurries, process materials, and specialty substrate inputs.",
        "keywords": ["semiconductor materials", "wafer chemicals", "CMP", "electronic materials", "process gases"],
    },

    "composites_materials": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "advanced_materials",
        "display_name": "Composites & Specialty Materials",
        "proxy_type": "custom",
        "proxy_symbols": ["XLB", "IYM"],
        "candidate_symbols": ["AVAV", "HXL", "CXT", "MEOH", "EMN", "CC"],
        "rollup_sector_ids": ["materials", "industrials"],
        "description": "Carbon fiber, polymer composites, performance plastics, and specialty industrial materials.",
        "keywords": ["composites", "carbon fiber", "specialty materials", "polymers", "performance plastics"],
    },

    # ── Under Semiconductors ──────────────────────────────────────────────────
    "ai_accelerators": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "AI Accelerators & Compute Silicon",
        "proxy_type": "custom",
        "proxy_symbols": ["NVDA", "AMD", "AVGO"],
        "candidate_symbols": ["NVDA", "AMD", "AVGO", "INTC", "QCOM", "GOOG", "AMZN"],
        "rollup_sector_ids": ["technology"],
        "description": "GPUs, AI accelerators, custom compute ASICs, and compute-related semiconductor IP.",
        "keywords": ["AI chips", "GPU", "accelerator", "ASIC", "compute silicon", "AI processor"],
    },

    "dc_connectivity_silicon": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Data Center Connectivity & Interconnect Silicon",
        "proxy_type": "custom",
        "proxy_symbols": ["ALAB", "CRDO", "MRVL", "AVGO"],
        "candidate_symbols": [
            "ALAB",  # Astera Labs — PCIe/CXL retimers, memory connectivity
            "CRDO",  # Credo Technology — SerDes for high-speed links
            "MRVL",  # Marvell — custom silicon, optical DSPs
            "AVGO",  # Broadcom — networking ASICs, custom silicon
            "INPHI", # Marvell InPhi division — high-speed signaling
        ],
        "rollup_sector_ids": ["technology"],
        "description": "Silicon and semiconductor IP for SerDes, PCIe, CXL, retimers, memory connectivity, and high-speed data-center interconnects.",
        "keywords": ["SerDes", "PCIe", "CXL", "retimer", "memory connectivity", "interconnect silicon", "fabric silicon"],
    },

    "memory_storage": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Memory & Storage",
        "proxy_type": "basket",
        "proxy_symbols": ["MU", "WDC", "SMH"],
        "candidate_symbols": ["MU", "WDC", "STX", "SNDK", "SIMO", "RMBS", "NAND"],
        "description": "DRAM, NAND flash, storage devices, controllers, and memory-related semiconductor IP.",
        "keywords": ["memory", "DRAM", "NAND", "storage", "flash", "SSD", "HDD", "memory controller"],
    },

    "analog_power_mixed": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Analog, Power & Mixed-Signal",
        "proxy_type": "custom",
        "proxy_symbols": ["TXN", "ADI", "MPWR"],
        "candidate_symbols": ["TXN", "ADI", "MPWR", "ON", "STM", "MCHP", "NXPI", "SWKS"],
        "rollup_sector_ids": ["technology"],
        "description": "Analog, mixed-signal, power-management, and power-semiconductor companies.",
        "keywords": ["analog", "mixed-signal", "power management", "power semiconductor", "ADC", "DAC"],
    },

    "foundry_manufacturing": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Foundry & Manufacturing",
        "proxy_type": "custom",
        "proxy_symbols": ["TSM", "UMC", "GFS"],
        "candidate_symbols": ["TSM", "UMC", "GFS", "INTC", "SMIC"],
        "rollup_sector_ids": ["technology"],
        "description": "Wafer fabrication, foundry operations, and integrated manufacturing where that is the defining business role.",
        "keywords": ["foundry", "wafer fab", "semiconductor manufacturing", "TSMC", "pure-play foundry"],
    },

    "semicap_equip": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Semiconductor Equipment",
        "proxy_type": "custom",
        "proxy_symbols": ["ASML", "AMAT", "LRCX", "KLAC"],
        "candidate_symbols": ["ASML", "AMAT", "LRCX", "KLAC", "ACLS", "UCTT", "CAMT"],
        "rollup_sector_ids": ["technology"],
        "description": "Wafer-fab process equipment for deposition, etch, lithography, and metrology.",
        "keywords": ["semiconductor equipment", "wafer fab equipment", "lithography", "etch", "deposition", "AMAT", "ASML"],
    },

    "semicap_materials_node": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "semiconductors",
        "display_name": "Semiconductor Materials",
        "proxy_type": "custom",
        "proxy_symbols": ["ENTG", "MKSI", "CEVA"],
        "candidate_symbols": ["ENTG", "MKSI", "MTSN", "FTEK", "AZTA", "PLAB"],
        "rollup_sector_ids": ["materials", "technology"],
        "description": "Wafers, process chemicals, gases, and other semiconductor-production input materials.",
        "keywords": ["semiconductor materials", "wafer", "process chemicals", "gases", "CMP slurry"],
    },

    "test_measurement": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Test & Measurement",
        "proxy_type": "custom",
        "proxy_symbols": ["TER", "AEHR", "COHU"],
        "candidate_symbols": ["TER", "AEHR", "FORM", "COHU", "CAMT", "ONTO", "NVMI", "INTT"],
        "rollup_sector_ids": ["technology"],
        "description": "Semiconductor testing, inspection, metrology, burn-in, handlers, and relevant electronic test systems.",
        "keywords": ["semiconductor test", "ATE", "burn-in", "inspection", "metrology", "handler", "test socket"],
    },

    "packaging_substrates": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",
        "display_name": "Packaging & Substrates",
        "proxy_type": "custom",
        "proxy_symbols": ["AMKR", "KLIC", "ONTO"],
        "candidate_symbols": ["AMKR", "KLIC", "ONTO", "BESI", "ASMVF", "IBIDF", "UMICF"],
        "rollup_sector_ids": ["technology"],
        "description": "Advanced packaging, OSAT, flip-chip, substrates, and packaging equipment.",
        "keywords": ["packaging", "OSAT", "advanced packaging", "substrates", "flip-chip", "HBM packaging"],
        "aliases": ["substrates_packaging_structural"],
    },

    # ── Under Data Center Infrastructure ─────────────────────────────────────
    "networking_fabric_infra": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "datacenter_infra",
        "display_name": "Networking & Fabric Infrastructure",
        "proxy_type": "custom",
        "proxy_symbols": ["ANET", "CSCO", "JNPR"],
        "candidate_symbols": ["ANET", "CSCO", "JNPR", "EXTR", "CALX", "NTGR"],
        "rollup_sector_ids": ["technology"],
        "description": "Networking switches, routers, fabric products, and infrastructure for data-center connectivity.",
        "keywords": ["networking", "switches", "routers", "fabric", "Ethernet", "InfiniBand", "data center network"],
    },

    "optical_interconnects": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "datacenter_infra",
        "display_name": "Optical Interconnects",
        "proxy_type": "custom",
        "proxy_symbols": ["COHR", "LITE", "AAOI", "CIEN"],
        "candidate_symbols": ["COHR", "LITE", "AAOI", "CIEN", "INFN", "FN", "VIAV", "LWLG"],
        "rollup_sector_ids": ["technology"],
        "description": "Optical transceivers, coherent systems, photonic interconnects, and silicon photonics for data-center connectivity.",
        "keywords": ["optical transceivers", "coherent", "silicon photonics", "QSFP", "800G", "optical interconnect"],
    },

    "servers_compute_systems": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "datacenter_infra",
        "display_name": "Servers & Compute Systems",
        "proxy_type": "custom",
        "proxy_symbols": ["SMCI", "HPE", "DELL"],
        "candidate_symbols": ["SMCI", "HPE", "DELL", "NTAP", "PTC", "PSTG"],
        "rollup_sector_ids": ["technology"],
        "description": "Server hardware, rack systems, and integrated compute-system vendors.",
        "keywords": ["servers", "rack", "compute systems", "server hardware", "OCP", "HPC"],
    },

    "ai_cloud_dc_operators": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "datacenter_infra",
        "display_name": "AI Cloud & Data Center Operators",
        "proxy_type": "custom",
        "proxy_symbols": ["EQIX", "DLR", "IREN"],
        "candidate_symbols": ["EQIX", "DLR", "IREN", "APLD", "GDS", "VNET", "NBIS", "COR"],
        "rollup_sector_ids": ["technology", "real_estate", "utilities"],
        "description": "AI cloud platforms, GPU-cloud operators, data-center operators, colocation operators, and relevant REIT businesses.",
        "keywords": ["data center operators", "AI cloud", "GPU cloud", "colocation", "colo REIT", "hyperscaler"],
    },

    "power_cooling": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "datacenter_infra",
        "display_name": "Power & Cooling",
        "proxy_type": "custom",
        "proxy_symbols": ["VRT", "ETN", "GEV", "TT"],
        "candidate_symbols": [
            "VRT", "ETN", "GEV", "TT", "IR", "NVT", "GNRC", "HUBB",
            "BE", "GTLS", "AAON", "SPX",
        ],
        "rollup_sector_ids": ["technology", "industrials"],
        "description": "Data-center power distribution, thermal management, cooling systems, UPS, and backup power.",
        "keywords": ["power", "cooling", "thermal management", "UPS", "liquid cooling", "HVAC", "PDU"],
    },

    # ── Under Software ────────────────────────────────────────────────────────
    "cloud_software": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "software",
        "display_name": "Cloud Software",
        "proxy_type": "etf",
        "proxy_symbols": ["SKYY", "CLOU"],
        "candidate_symbols": ["SNOW", "DDOG", "MDB", "NET", "AMZN", "MSFT", "GOOGL"],
        "rollup_sector_ids": ["technology"],
        "description": "SaaS, PaaS, and cloud-native software platforms.",
        "keywords": ["cloud", "SaaS", "cloud computing", "platform software", "AWS", "Azure"],
        "aliases": ["cloud_computing"],
    },

    "cybersecurity": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "software",
        "display_name": "Cybersecurity",
        "proxy_type": "etf",
        "proxy_symbols": ["CIBR", "HACK", "BUG", "IHAK"],
        "candidate_symbols": ["PANW", "CRWD", "FTNT", "ZS", "CYBR", "S", "QLYS"],
        "rollup_sector_ids": ["technology"],
        "description": "Network security, endpoint security, identity, SIEM, and zero-trust platforms.",
        "keywords": ["cybersecurity", "zero trust", "endpoint security", "SIEM", "identity", "network security"],
    },

    "ai_software_platforms": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "software",
        "display_name": "AI Software & Data Platforms",
        "proxy_type": "custom",
        "proxy_symbols": ["PLTR", "AI", "BBAI"],
        "candidate_symbols": ["PLTR", "AI", "BBAI", "SOUN", "RBRK", "DXC", "BIGC"],
        "rollup_sector_ids": ["technology"],
        "description": "AI software platforms, data analytics, AI Ops, and enterprise AI applications.",
        "keywords": ["AI software", "data platform", "AI Ops", "enterprise AI", "AI analytics"],
    },

    # ── Under Photonics & Optical Systems ─────────────────────────────────────
    "optical_components_lasers": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "photonics_optical",
        "display_name": "Optical Components & Lasers",
        "proxy_type": "custom",
        "proxy_symbols": ["IPGP", "COHR", "LITE", "AAOI"],
        "candidate_symbols": ["IPGP", "COHR", "LITE", "AAOI", "VIAV", "FN", "LPTH", "MKSI", "LASR"],
        "rollup_sector_ids": ["technology", "industrials"],
        "description": "Fiber lasers, solid-state lasers, optical components, and photonic manufacturing.",
        "keywords": ["fiber laser", "industrial laser", "optical components", "photonic", "laser diode"],
    },

    "sensing_lidar": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "photonics_optical",
        "display_name": "Sensing & LiDAR",
        "proxy_type": "custom",
        "proxy_symbols": ["LAZR", "LIDR", "INVZ"],
        "candidate_symbols": ["LAZR", "LIDR", "INVZ", "OUST", "VLDR", "MVIS"],
        "rollup_sector_ids": ["technology", "industrials"],
        "description": "LiDAR sensors, radar, and photonic sensing systems for automotive and industrial applications.",
        "keywords": ["LiDAR", "sensing", "radar", "MEMS", "optical sensing", "autonomous sensing"],
    },

    # ── Under Defense & Aerospace ─────────────────────────────────────────────
    "defense_platforms_electronics": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "defense",
        "display_name": "Defense Platforms & Electronics",
        "proxy_type": "etf",
        "proxy_symbols": ["ITA", "PPA", "XAR"],
        "candidate_symbols": ["LMT", "RTX", "NOC", "GD", "L3H", "BWXT", "CW", "HWM"],
        "rollup_sector_ids": ["industrials", "technology"],
        "description": "Defense systems, aircraft, ships, electronic warfare, radar, and prime-contractor platforms.",
        "keywords": ["defense platforms", "missiles", "aircraft", "radar", "electronic warfare", "C4ISR"],
    },

    "drones": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "defense",
        "display_name": "Drones & Autonomous Systems",
        "proxy_type": "hybrid",
        "proxy_symbols": ["XAR", "ITA"],
        "candidate_symbols": ["AVAV", "KTOS", "RCAT", "JOBY", "ACHR", "EH", "UURX"],
        "rollup_sector_ids": ["industrials", "technology"],
        "description": "Unmanned aerial vehicles, autonomous systems, and counter-drone technology.",
        "keywords": ["drones", "UAV", "autonomous systems", "VTOL", "counter-drone", "eVTOL"],
    },

    # ── Under Space Economy ───────────────────────────────────────────────────
    "launch_space_systems": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "space",
        "display_name": "Launch & Space Systems",
        "proxy_type": "custom",
        "proxy_symbols": ["RKLB", "LUNR", "SPIR"],
        "candidate_symbols": ["RKLB", "LUNR", "SPIR", "SATL", "ASTS"],
        "rollup_sector_ids": ["industrials", "technology"],
        "description": "Launch vehicles, spacecraft, in-space propulsion, and commercial space infrastructure.",
        "keywords": ["launch", "rocket", "spacecraft", "LEO", "moon", "in-space systems"],
    },

    "satellite_comms": {
        "classification": "sub_theme",
        "parent_sector": "communication_services",
        "parent_theme_id": "space",
        "display_name": "Satellite Communications",
        "proxy_type": "custom",
        "proxy_symbols": ["IRDM", "VSAT", "GSAT"],
        "candidate_symbols": ["IRDM", "VSAT", "GSAT", "ASTS", "GNSS", "ViaSat"],
        "rollup_sector_ids": ["communication_services", "industrials"],
        "description": "Satellite connectivity, broadband, and communications services.",
        "keywords": ["satellite communications", "broadband satellite", "LEO constellation", "GEO satellite"],
    },

    "earth_observation": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "space",
        "display_name": "Earth Observation & Space Data",
        "proxy_type": "custom",
        "proxy_symbols": ["SPIR", "MAXR", "SATL"],
        "candidate_symbols": ["SPIR", "SATL", "GNSS", "KBAL"],
        "rollup_sector_ids": ["technology", "industrials"],
        "description": "Earth imaging, geospatial intelligence, weather data, and space-derived analytics.",
        "keywords": ["earth observation", "geospatial", "remote sensing", "SAR", "weather satellite"],
    },

    # ── Under Clean Energy ────────────────────────────────────────────────────
    "solar": {
        "classification": "sub_theme",
        "parent_sector": "utilities",
        "parent_theme_id": "clean_energy",
        "display_name": "Solar",
        "proxy_type": "etf",
        "proxy_symbols": ["TAN"],
        "candidate_symbols": ["FSLR", "ENPH", "SEDG", "ARRY", "RUN", "MAXN"],
        "rollup_sector_ids": ["utilities", "energy", "industrials"],
        "description": "Solar panel manufacturers, inverter companies, and solar installation businesses.",
        "keywords": ["solar", "photovoltaic", "solar panels", "inverter", "solar installation", "utility-scale solar"],
    },

    "battery_tech_storage": {
        "classification": "sub_theme",
        "parent_sector": "utilities",
        "parent_theme_id": "clean_energy",
        "display_name": "Battery Technology & Energy Storage",
        "proxy_type": "etf",
        "proxy_symbols": ["BATT", "LIT"],
        "candidate_symbols": ["ENVX", "QS", "FREYR", "NKLA", "STEM", "FLUX", "KULR"],
        "rollup_sector_ids": ["utilities", "industrials", "materials"],
        "description": "Battery cells, battery components, storage systems, grid-storage platforms, and storage integrators.",
        "keywords": ["battery", "energy storage", "grid storage", "battery cells", "solid state battery"],
    },

    "hydrogen_fuel_cells": {
        "classification": "sub_theme",
        "parent_sector": "utilities",
        "parent_theme_id": "clean_energy",
        "display_name": "Hydrogen & Fuel Cells",
        "proxy_type": "custom",
        "proxy_symbols": ["PLUG", "BE", "FCEL"],
        "candidate_symbols": ["PLUG", "BE", "FCEL", "BLDP", "ITM", "NKLA"],
        "rollup_sector_ids": ["utilities", "industrials", "energy"],
        "description": "Green hydrogen production, fuel cells, electrolyzers, and hydrogen infrastructure.",
        "keywords": ["hydrogen", "fuel cells", "green hydrogen", "electrolyzer", "PEM", "SOFC"],
    },

    "wind_renewable": {
        "classification": "sub_theme",
        "parent_sector": "utilities",
        "parent_theme_id": "clean_energy",
        "display_name": "Wind & Renewable Generation",
        "proxy_type": "etf",
        "proxy_symbols": ["FAN", "ICLN"],
        "candidate_symbols": ["NEE", "CWEN", "AES", "RNW", "VWDRY", "ORSTED"],
        "rollup_sector_ids": ["utilities", "energy", "industrials"],
        "description": "Wind turbine manufacturers, wind and renewable power generation, and renewable utilities.",
        "keywords": ["wind", "renewable generation", "offshore wind", "turbines", "wind power"],
    },

    # ── Under Grid & Electrification ──────────────────────────────────────────
    "grid_hardware_electrical": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "grid_electrification",
        "display_name": "Grid Hardware & Electrical Equipment",
        "proxy_type": "custom",
        "proxy_symbols": ["ETN", "HUBB", "AZZ"],
        "candidate_symbols": ["ETN", "HUBB", "AZZ", "POWL", "REZI", "AMETEK"],
        "rollup_sector_ids": ["industrials", "utilities"],
        "description": "Switchgear, transformers, power distribution, and electrical grid hardware.",
        "keywords": ["switchgear", "transformer", "power distribution", "grid hardware", "electrical equipment"],
    },

    "power_generation_turbines": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "grid_electrification",
        "display_name": "Power Generation & Turbines",
        "proxy_type": "custom",
        "proxy_symbols": ["GEV", "EMR", "MHI"],
        "candidate_symbols": ["GEV", "MTUAY", "SIEGY", "WLTW", "PWR"],
        "rollup_sector_ids": ["industrials", "utilities"],
        "description": "Gas turbines, steam turbines, and large-scale power generation equipment.",
        "keywords": ["turbines", "power generation", "gas turbine", "steam turbine", "combined cycle"],
    },

    "distributed_backup_power": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "grid_electrification",
        "display_name": "Distributed & Backup Power",
        "proxy_type": "custom",
        "proxy_symbols": ["GNRC", "ARBE", "AMETEK"],
        "candidate_symbols": ["GNRC", "AMETEK", "RGEN", "VICR", "ACNB"],
        "rollup_sector_ids": ["industrials", "utilities"],
        "description": "Backup generators, UPS, microgrid, and distributed power systems.",
        "keywords": ["backup power", "generators", "UPS", "microgrid", "distributed power"],
    },

    # ── Under Nuclear Energy ──────────────────────────────────────────────────
    "uranium_nuclear_fuel": {
        "classification": "sub_theme",
        "parent_sector": "materials",
        "parent_theme_id": "nuclear_energy",
        "display_name": "Uranium Mining & Nuclear Fuel",
        "proxy_type": "etf",
        "proxy_symbols": ["URA", "URNM"],
        "candidate_symbols": ["CCJ", "UUUU", "LEU", "NXE", "DYL", "PDN", "URG"],
        "rollup_sector_ids": ["materials", "energy"],
        "description": "Uranium mining, enrichment, and nuclear fuel cycle businesses.",
        "keywords": ["uranium", "uranium mining", "nuclear fuel", "enrichment", "yellow cake"],
    },

    "nuclear_equipment_services": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "nuclear_energy",
        "display_name": "Nuclear Equipment & Services",
        "proxy_type": "custom",
        "proxy_symbols": ["BWXT", "CW", "LMT"],
        "candidate_symbols": ["BWXT", "CW", "AMSC", "GEV"],
        "rollup_sector_ids": ["industrials", "utilities"],
        "description": "Nuclear reactor components, services, and maintenance businesses.",
        "keywords": ["nuclear equipment", "reactor services", "nuclear components", "decommissioning"],
    },

    "smr_advanced_reactors": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "nuclear_energy",
        "display_name": "SMRs & Advanced Reactors",
        "proxy_type": "custom",
        "proxy_symbols": ["SMR", "OKLO", "NLR"],
        "candidate_symbols": ["SMR", "OKLO", "NUKE", "BWX", "NANO"],
        "rollup_sector_ids": ["industrials", "utilities"],
        "description": "Small modular reactors, advanced nuclear designs, and next-generation reactor developers.",
        "keywords": ["SMR", "small modular reactor", "advanced nuclear", "microreactor", "thorium"],
    },

    "nuclear_utilities_operators": {
        "classification": "sub_theme",
        "parent_sector": "utilities",
        "parent_theme_id": "nuclear_energy",
        "display_name": "Nuclear Utilities & Operators",
        "proxy_type": "etf",
        "proxy_symbols": ["NLR", "XLU"],
        "candidate_symbols": ["CEG", "VST", "NRG", "AEE", "ETR"],
        "rollup_sector_ids": ["utilities"],
        "description": "Electric utilities and independent power producers with significant nuclear generation.",
        "keywords": ["nuclear utility", "nuclear operator", "nuclear power plant", "baseload nuclear"],
    },

    # ── Under Oil & Gas ───────────────────────────────────────────────────────
    "ep_upstream": {
        "classification": "sub_theme",
        "parent_sector": "energy",
        "parent_theme_id": "oil_gas",
        "display_name": "E&P / Upstream",
        "proxy_type": "etf",
        "proxy_symbols": ["XOP", "DRIP"],
        "candidate_symbols": ["COP", "EOG", "DVN", "MRO", "APA", "OVV", "CTRA"],
        "rollup_sector_ids": ["energy"],
        "description": "Exploration and production companies focused on oil and gas upstream activities.",
        "keywords": ["E&P", "upstream", "exploration", "production", "shale", "offshore drilling"],
    },

    "integrated_oil_refining": {
        "classification": "sub_theme",
        "parent_sector": "energy",
        "parent_theme_id": "oil_gas",
        "display_name": "Integrated Oil & Refining",
        "proxy_type": "etf",
        "proxy_symbols": ["XLE", "VDE"],
        "candidate_symbols": ["XOM", "CVX", "BP", "SHEL", "TTE", "PSX", "VLO"],
        "rollup_sector_ids": ["energy"],
        "description": "Integrated oil companies and refiners spanning upstream, midstream, and downstream.",
        "keywords": ["integrated oil", "refining", "downstream", "crack spread", "major oil", "refinery"],
    },

    "lng_gas": {
        "classification": "sub_theme",
        "parent_sector": "energy",
        "parent_theme_id": "oil_gas",
        "display_name": "LNG & Natural Gas",
        "proxy_type": "etf",
        "proxy_symbols": ["FCG", "UNG"],
        "candidate_symbols": ["LNG", "WMB", "KMI", "FLNG", "NEXT", "AR", "EQT"],
        "rollup_sector_ids": ["energy"],
        "description": "LNG exporters, natural gas producers, and natural gas pipeline operators.",
        "keywords": ["LNG", "natural gas", "liquefied natural gas", "gas export", "gas producer"],
    },

    "midstream_pipelines": {
        "classification": "sub_theme",
        "parent_sector": "energy",
        "parent_theme_id": "oil_gas",
        "display_name": "Midstream & Pipelines",
        "proxy_type": "etf",
        "proxy_symbols": ["AMLP", "MLPX"],
        "candidate_symbols": ["ET", "EPD", "MMP", "WMB", "OKE", "PAA", "MPLX"],
        "rollup_sector_ids": ["energy"],
        "description": "Pipeline, storage, and gathering & processing MLPs and midstream companies.",
        "keywords": ["midstream", "pipelines", "MLP", "storage", "gathering", "processing"],
    },

    "oil_services": {
        "classification": "sub_theme",
        "parent_sector": "energy",
        "parent_theme_id": "oil_gas",
        "display_name": "Oil Services",
        "proxy_type": "etf",
        "proxy_symbols": ["OIH", "XES", "IEZ"],
        "candidate_symbols": ["SLB", "HAL", "BKR", "WTTR", "NE", "HP"],
        "rollup_sector_ids": ["energy"],
        "description": "Oilfield services, drilling, completion, and well services.",
        "keywords": ["oil services", "drilling", "completion", "oilfield services", "fracking", "wellbore"],
    },

    # ── Under Industrial Automation ───────────────────────────────────────────
    "robotics_automation": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "industrial_automation",
        "display_name": "Robotics & Automation",
        "proxy_type": "etf",
        "proxy_symbols": ["BOTZ", "ROBO", "IRBO", "ARKQ"],
        "candidate_symbols": ["SYM", "ISRG", "TER", "ROK", "FANUY", "ABB"],
        "rollup_sector_ids": ["industrials", "technology"],
        "description": "Industrial robots, collaborative robots, automated assembly, and robotic-process automation.",
        "keywords": ["robotics", "automation", "cobots", "industrial robots", "RPA", "autonomous mobile robots"],
    },

    "industrial_controls_sensors": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "industrial_automation",
        "display_name": "Industrial Controls & Sensors",
        "proxy_type": "custom",
        "proxy_symbols": ["HON", "ROK", "EMR"],
        "candidate_symbols": ["HON", "ROK", "EMR", "KEYS", "MKSI", "OSIS", "AMETEK"],
        "rollup_sector_ids": ["industrials", "technology"],
        "description": "PLC/DCS process control, industrial sensors, IoT instrumentation, and factory automation software.",
        "keywords": ["process control", "PLC", "DCS", "industrial sensors", "IoT", "SCADA"],
    },

    # ── Under Construction & Infrastructure ───────────────────────────────────
    "engineering_construction": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "construction_infrastructure",
        "display_name": "Engineering & Construction",
        "proxy_type": "etf",
        "proxy_symbols": ["PAVE", "PKB"],
        "candidate_symbols": ["FLR", "J", "MTZ", "PWR", "DY", "STRL", "GVA"],
        "rollup_sector_ids": ["industrials"],
        "description": "Engineering, procurement, and construction (EPC) contractors and infrastructure builders.",
        "keywords": ["EPC", "engineering", "construction", "infrastructure projects", "civil engineering"],
    },

    "heavy_equipment": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "construction_infrastructure",
        "display_name": "Heavy Equipment",
        "proxy_type": "custom",
        "proxy_symbols": ["CAT", "DE", "CNHI"],
        "candidate_symbols": ["CAT", "CNHI", "KOM", "HII", "MTW", "TXT"],
        "rollup_sector_ids": ["industrials"],
        "description": "Heavy construction equipment, mining machinery, and large industrial machinery.",
        "keywords": ["heavy equipment", "excavators", "bulldozers", "cranes", "mining equipment"],
    },

    "building_products": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "construction_infrastructure",
        "display_name": "Building Products",
        "proxy_type": "etf",
        "proxy_symbols": ["XHB", "PKB"],
        "candidate_symbols": ["VMC", "MLM", "BECN", "AWI", "SUM", "EXP", "FBHS"],
        "rollup_sector_ids": ["industrials", "materials"],
        "description": "Aggregates, cement, insulation, windows, flooring, and building product manufacturers.",
        "keywords": ["building products", "aggregates", "cement", "insulation", "flooring", "building materials"],
    },

    # ── Under Transportation & Mobility ───────────────────────────────────────
    "travel_leisure": {
        "classification": "sub_theme",
        "parent_sector": "consumer_discretionary",
        "parent_theme_id": "transportation_mobility",
        "display_name": "Travel & Leisure",
        "proxy_type": "etf",
        "proxy_symbols": ["JETS", "AWAY"],
        "candidate_symbols": ["UAL", "DAL", "AAL", "CCL", "RCL", "MAR", "HLT"],
        "rollup_sector_ids": ["consumer_discretionary", "industrials"],
        "description": "Airlines, cruise lines, hotels, travel services, and leisure businesses.",
        "keywords": ["travel", "airlines", "cruise", "hotels", "leisure", "tourism"],
    },

    "freight_logistics": {
        "classification": "sub_theme",
        "parent_sector": "industrials",
        "parent_theme_id": "transportation_mobility",
        "display_name": "Freight & Logistics",
        "proxy_type": "etf",
        "proxy_symbols": ["IYT", "XTN"],
        "candidate_symbols": ["UPS", "FDX", "UNP", "CSX", "NSC", "CHRW", "XPO"],
        "rollup_sector_ids": ["industrials"],
        "description": "Freight carriers, rail, trucking, supply chain logistics, and parcel delivery.",
        "keywords": ["freight", "logistics", "trucking", "rail", "supply chain", "parcel", "shipping"],
    },

    # ── Under Crypto Equities / Blockchain ────────────────────────────────────
    "bitcoin_miners": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "crypto_equities",
        "display_name": "Bitcoin Miners",
        "proxy_type": "etf",
        "proxy_symbols": ["WGMI", "BLOK"],
        "candidate_symbols": ["MARA", "RIOT", "CLSK", "HUT", "IREN", "CIFR", "BTBT"],
        "rollup_sector_ids": ["technology"],
        "description": "Bitcoin mining operations and hash-rate infrastructure businesses.",
        "keywords": ["bitcoin mining", "hash rate", "ASICs", "BTC miner", "proof of work"],
    },

    "digital_asset_platforms": {
        "classification": "sub_theme",
        "parent_sector": "financials",
        "parent_theme_id": "crypto_equities",
        "display_name": "Digital Asset Platforms",
        "proxy_type": "custom",
        "proxy_symbols": ["COIN", "HOOD", "MSTR"],
        "candidate_symbols": ["COIN", "HOOD", "MSTR", "GLXY", "GBTC", "IBIT"],
        "rollup_sector_ids": ["financials", "technology"],
        "description": "Crypto exchanges, custodians, brokers, and digital-asset treasury companies.",
        "keywords": ["crypto exchange", "digital assets", "custody", "bitcoin treasury", "crypto brokerage"],
    },

    "blockchain_infrastructure": {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "parent_theme_id": "crypto_equities",
        "display_name": "Blockchain Infrastructure",
        "proxy_type": "custom",
        "proxy_symbols": ["BITQ", "BKCH"],
        "candidate_symbols": ["HIVE", "BTCS", "MGTI", "BTSG"],
        "rollup_sector_ids": ["technology"],
        "description": "Blockchain technology, DeFi infrastructure, smart-contract platforms, and Web3 equities.",
        "keywords": ["blockchain", "DeFi", "Web3", "smart contracts", "Layer 2", "infrastructure"],
    },

    # ══════════════════════════════════════════════════════════════════════════
    # COMMODITY MARKET LENSES  (classification="market_lens", assignable=False)
    # Preserved for Gold/Silver/Copper proxy/performance series only.
    # These are NOT assignable canonical theme-membership buckets.
    # ══════════════════════════════════════════════════════════════════════════

    "gold": {
        "classification": "market_lens",
        "assignable": False,
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",  # logical parent for rollup/context; not assignable
        "display_name": "Gold (Commodity Lens)",
        "commodity_lens": True,
        "proxy_type": "etf",
        "proxy_symbols": ["GLD", "IAU", "GDX", "GDXJ"],
        "candidate_symbols": ["NEM", "GOLD", "AEM"],
        "description": "Gold commodity price exposure and gold-miner performance lens. Not an assignable stock-taxonomy bucket.",
        "keywords": ["gold", "GLD", "gold miners", "bullion"],
        "aliases": ["gold_commodity"],
    },

    "silver": {
        "classification": "market_lens",
        "assignable": False,
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",  # logical parent for rollup/context; not assignable
        "display_name": "Silver (Commodity Lens)",
        "commodity_lens": True,
        "proxy_type": "etf",
        "proxy_symbols": ["SLV", "SIL", "SILJ"],
        "candidate_symbols": ["PAAS", "AG", "MAG"],
        "description": "Silver commodity price exposure and silver-miner performance lens. Not an assignable stock-taxonomy bucket.",
        "keywords": ["silver", "SLV", "silver miners"],
        "aliases": ["silver_commodity"],
    },

    "copper_miners": {
        "classification": "market_lens",
        "assignable": False,
        "parent_sector": "materials",
        "parent_theme_id": "metals_mining",  # logical parent for rollup/context; not assignable
        "display_name": "Copper",
        "commodity_lens": True,
        "proxy_type": "etf",
        "proxy_symbols": ["COPX", "CPER", "DBB"],
        "candidate_symbols": ["FCX", "SCCO", "TECK", "HBM"],
        "description": "Copper commodity price exposure and copper-miner performance lens. Structural stock membership: use Base Metals & Diversified Mining.",
        "keywords": ["copper", "COPX", "copper miners"],
        "aliases": ["copper_miners", "copper_commodity"],
    },

    # ══════════════════════════════════════════════════════════════════════════
    # DEPRECATED STRUCTURAL NODES  (classification="deprecated", assignable=False)
    # Kept for backward-compatibility alias resolution during migration.
    # These IDs must not appear in assignment dropdowns, taxonomy chips,
    # AI output, or canonical registry results intended for assignment.
    # ══════════════════════════════════════════════════════════════════════════

    "ai_networking": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "technology",
        "display_name": "[Deprecated] AI Networking",
        "proxy_type": "custom",
        "proxy_symbols": ["ANET", "AVGO", "MRVL", "CRDO", "ALAB"],
        "candidate_symbols": [],
        "description": "DEPRECATED — split into dc_connectivity_silicon (Semiconductors) and networking_fabric_infra (Data Center Infrastructure).",
        "migration_targets": ["dc_connectivity_silicon", "networking_fabric_infra"],
        "keywords": ["AI networking", "ai_networking", "DC networking silicon"],
        "aliases": ["ai_networking"],
    },

    "semicap_equipment": {
        "classification": "deprecated",
        "parent_theme_id": "semiconductors",  # logical parent retained for backward-compat rollup
        "assignable": False,
        "parent_sector": "technology",
        "display_name": "[Deprecated] Semi Equipment & Materials",
        "proxy_type": "custom",
        "proxy_symbols": ["SOXX", "SMH"],
        "candidate_symbols": [],
        "description": "DEPRECATED — split stock-by-stock into semicap_equip, semicap_materials_node, test_measurement, packaging_substrates.",
        "migration_targets": ["semicap_equip", "semicap_materials_node", "test_measurement", "packaging_substrates"],
        "keywords": ["semicap", "semiconductor equipment", "semi materials"],
        "aliases": [
            "semicap_equipment", "semicap", "semiconductor_equipment",
            "semi_equipment", "semi_materials", "semiconductor_materials",
            "semi_equipment_and_materials",
        ],
    },

    "lithium_battery": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "materials",
        "display_name": "[Deprecated] Lithium & Battery Tech",
        "proxy_type": "etf",
        "proxy_symbols": ["LIT", "BATT"],
        "candidate_symbols": [],
        "description": "DEPRECATED — lithium miners → lithium (Metals & Mining); battery/storage → battery_tech_storage (Clean Energy).",
        "migration_targets": ["lithium", "battery_tech_storage"],
        "keywords": ["lithium", "battery tech", "LIT", "BATT"],
        "aliases": ["lithium_battery"],
    },

    "uranium_nuclear": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "utilities",
        "display_name": "[Deprecated] Uranium & Nuclear Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["URA", "URNM", "NLR"],
        "candidate_symbols": [],
        "description": "DEPRECATED — replaced by nuclear_energy parent theme with four children.",
        "migration_targets": ["nuclear_energy", "uranium_nuclear_fuel", "nuclear_equipment_services", "smr_advanced_reactors", "nuclear_utilities_operators"],
        "keywords": ["uranium", "nuclear energy", "URA", "URNM", "NLR"],
        "aliases": ["uranium_nuclear"],
    },

    "chemicals_materials": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "materials",
        "display_name": "[Deprecated] Chemicals & Materials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLB", "IYM"],
        "candidate_symbols": [],
        "description": "DEPRECATED — replaced by advanced_materials parent theme.",
        "migration_targets": ["advanced_materials"],
        "keywords": ["chemicals", "materials", "specialty materials", "XLB"],
        "aliases": ["chemicals_materials"],
    },

    "photonics_lasers": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "technology",
        "display_name": "[Deprecated] Photonics / Lasers",
        "proxy_type": "custom",
        "proxy_symbols": ["IPGP", "COHR", "LITE", "AAOI"],
        "candidate_symbols": [],
        "description": "DEPRECATED — replaced by photonics_optical parent theme (optical_components_lasers child).",
        "migration_targets": ["photonics_optical", "optical_components_lasers", "optical_interconnects"],
        "keywords": ["photonics", "lasers", "optical", "IPGP", "COHR"],
        "aliases": ["photonics_lasers"],
    },

    "substrates_packaging": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "technology",
        "parent_theme_id": "semiconductors",  # logical parent retained for backward-compat rollup
        "display_name": "[Deprecated] Substrates / Packaging",
        "proxy_type": "custom",
        "proxy_symbols": ["AMKR", "KLIC"],
        "candidate_symbols": [],
        "description": "DEPRECATED — replaced by packaging_substrates (Packaging & Substrates) under Semiconductors.",
        "migration_targets": ["packaging_substrates"],
        "keywords": ["substrates", "packaging", "AMKR", "KLIC"],
        "aliases": ["substrates_packaging"],
    },

    "travel_transportation": {
        "classification": "deprecated",
        "assignable": False,
        "parent_sector": "industrials",
        "display_name": "[Deprecated] Travel & Transportation",
        "proxy_type": "etf",
        "proxy_symbols": ["IYT", "JETS", "XTN"],
        "candidate_symbols": [],
        "description": "DEPRECATED — replaced by transportation_mobility parent theme (travel_leisure + freight_logistics children).",
        "migration_targets": ["transportation_mobility", "travel_leisure", "freight_logistics"],
        "keywords": ["travel", "transportation", "airlines", "JETS", "IYT"],
        "aliases": ["travel_transportation"],
    },
}


# ══════════════════════════════════════════════════════════════════════════════
# Helper utilities
# ══════════════════════════════════════════════════════════════════════════════

def get_assignable_registry(registry: dict | None = None) -> dict:
    """Return only assignable nodes (excludes market_lens and deprecated)."""
    reg = registry or THEME_RS_UNIVERSE
    return {
        k: v for k, v in reg.items()
        if v.get("assignable", True) and v.get("classification") not in ("market_lens", "deprecated")
    }


def get_effective_rollup_sector_ids(
    theme_id: str,
    registry: dict | None = None,
) -> list[str]:
    """
    Return the effective set of rollup sector IDs for any theme/sub-theme node.

    Priority:
    1. Explicit ``rollup_sector_ids`` on the node itself.
    2. The node's own ``parent_sector`` (if it is a recognised sector ID).
    3. Inherited from ``parent_theme_id`` chain (recursively).

    Returns a deduplicated, sorted list of valid sector IDs, or [] for sector
    nodes and nodes with no derivable rollup.
    """
    reg = registry or THEME_RS_UNIVERSE

    sector_ids: set[str] = {
        k for k, v in reg.items() if v.get("classification") == "sector"
    }

    def _collect(tid: str, seen: set[str]) -> set[str]:
        if tid in seen:
            return set()
        seen = seen | {tid}
        meta = reg.get(tid) or {}
        cls = meta.get("classification", "")

        # Sector nodes are the rollup targets, not rollup sources.
        if cls == "sector":
            return set()

        explicit = meta.get("rollup_sector_ids") or []
        if explicit:
            return {s for s in explicit if s in sector_ids}

        result: set[str] = set()
        ps = meta.get("parent_sector")
        if ps and ps in sector_ids:
            result.add(ps)

        ptid = meta.get("parent_theme_id")
        if ptid:
            result |= _collect(ptid, seen)

        return result

    return sorted(_collect(theme_id, set()))


# ══════════════════════════════════════════════════════════════════════════════
# Registry integrity validator
# ══════════════════════════════════════════════════════════════════════════════

_VALID_CLASSIFICATIONS = frozenset(
    {"sector", "theme", "sub_theme", "market_lens", "deprecated"}
)
_STRUCTURAL_CLASSIFICATIONS = frozenset({"sector", "theme", "sub_theme"})
_ASSIGNABLE_CLASSIFICATIONS = frozenset({"sector", "theme", "sub_theme"})


def validate_registry(registry: dict | None = None) -> list[str]:
    """
    Validate the registry and return a list of error strings.

    Empty return → registry is valid.
    """
    reg = registry or THEME_RS_UNIVERSE
    errors: list[str] = []

    ids = set(reg.keys())
    sector_ids = {k for k, v in reg.items() if v.get("classification") == "sector"}

    # ── 1: required fields ────────────────────────────────────────────────────
    for tid, meta in reg.items():
        for field in ("classification", "display_name"):
            if field not in meta:
                errors.append(f"{tid}: missing required field '{field}'")

        cls = meta.get("classification", "")
        if cls not in _VALID_CLASSIFICATIONS:
            errors.append(f"{tid}: invalid classification {cls!r}")

    # ── 2: unique IDs (guaranteed by dict; sanity check) ─────────────────────
    # Python dict keys are unique — nothing to check.

    # ── 3: parent_theme_id must point to an assignable "theme" node ──────────
    for tid, meta in reg.items():
        cls = meta.get("classification", "")
        if cls not in _STRUCTURAL_CLASSIFICATIONS:
            continue  # market_lens and deprecated are exempt
        ptid = meta.get("parent_theme_id")
        if ptid is None:
            continue
        if ptid not in reg:
            errors.append(f"{tid}: parent_theme_id {ptid!r} not in registry")
        else:
            parent_cls = reg[ptid].get("classification")
            if parent_cls != "theme":
                errors.append(
                    f"{tid}: parent_theme_id {ptid!r} has classification "
                    f"{parent_cls!r}; must be 'theme'"
                )

    # ── 4: sub_theme must have parent_theme_id; theme must not ───────────────
    for tid, meta in reg.items():
        cls = meta.get("classification", "")
        if cls == "sub_theme" and not meta.get("parent_theme_id"):
            errors.append(f"{tid}: sub_theme is missing parent_theme_id")
        if cls == "theme" and meta.get("parent_theme_id"):
            errors.append(
                f"{tid}: theme node must not have parent_theme_id "
                f"(found {meta['parent_theme_id']!r})"
            )

    # ── 5: no cycles ──────────────────────────────────────────────────────────
    for tid in ids:
        meta = reg.get(tid, {})
        if meta.get("classification") not in _STRUCTURAL_CLASSIFICATIONS:
            continue
        seen: set[str] = set()
        current: str | None = meta.get("parent_theme_id")
        while current:
            if current in seen:
                errors.append(f"{tid}: cycle detected in parent_theme_id chain at {current!r}")
                break
            seen.add(current)
            current = reg.get(current, {}).get("parent_theme_id")

    # ── 6: rollup_sector_ids must be valid sector IDs ─────────────────────────
    for tid, meta in reg.items():
        for sid in meta.get("rollup_sector_ids") or []:
            if sid not in sector_ids:
                errors.append(f"{tid}: rollup_sector_id {sid!r} is not a sector")

    # ── 7: alias uniqueness ───────────────────────────────────────────────────
    seen_aliases: dict[str, str] = {}
    for tid, meta in reg.items():
        for alias in (meta.get("aliases") or []):
            # Alias shadowing a *different* node's canonical ID is disallowed.
            if alias in ids and alias != tid:
                errors.append(
                    f"{tid}: alias {alias!r} shadows canonical ID of another node"
                )
            if alias in seen_aliases and seen_aliases[alias] != tid:
                errors.append(
                    f"{tid}: alias {alias!r} already claimed by {seen_aliases[alias]!r}"
                )
            seen_aliases[alias] = tid

    # ── 8: required backward-compat aliases ──────────────────────────────────
    _required_aliases: dict[str, str] = {
        # alias_string → owning_theme_id
        "rare_earth_metals":      "rare_earth",     # prior display label preserved
        "copper_miners":          "copper_miners",  # market_lens node carries its own alias
        "substrates_packaging":   "substrates_packaging",  # deprecated node carries its own alias
    }
    for alias, owning_tid in _required_aliases.items():
        node_aliases = reg.get(owning_tid, {}).get("aliases") or []
        if alias not in node_aliases:
            errors.append(
                f"{owning_tid}: required backward-compat alias {alias!r} is missing"
            )

    # ── 9: non-sector structural nodes must have effective rollup ─────────────
    for tid, meta in reg.items():
        cls = meta.get("classification", "")
        if cls not in ("theme", "sub_theme"):
            continue  # sectors, market_lens, deprecated exempt
        if not meta.get("parent_sector") and not meta.get("rollup_sector_ids"):
            continue  # market-wide themes with no sector affiliation — exempt
        effective = get_effective_rollup_sector_ids(tid, reg)
        if not effective:
            errors.append(
                f"{tid}: non-sector node has parent_sector "
                f"{meta.get('parent_sector')!r} but get_effective_rollup_sector_ids() "
                f"returned [] — check rollup_sector_ids / parent_sector / parent_theme_id chain"
            )

    # ── 10: no sector ID is assignable as a theme/sub_theme ──────────────────
    for sid in sector_ids:
        # sector nodes are by definition not themes — no additional check needed
        pass

    # ── 11: Big Tech must not be a structural node ────────────────────────────
    for name in ("big_tech", "hyperscalers", "magnificent_seven"):
        if name in reg and reg[name].get("classification") in _ASSIGNABLE_CLASSIFICATIONS:
            errors.append(f"{name}: must not be an assignable structural taxonomy node")

    # ── 12: Gold, Silver, Copper must not be assignable structural nodes ──────
    for lens_id in ("gold", "silver", "copper_miners"):
        if lens_id in reg:
            cls = reg[lens_id].get("classification")
            if cls in _ASSIGNABLE_CLASSIFICATIONS:
                errors.append(f"{lens_id}: commodity lens must not be assignable (classification={cls!r})")

    # ── 13: 11 sectors exactly ────────────────────────────────────────────────
    if len(sector_ids) != 11:
        errors.append(f"Expected exactly 11 sector nodes, found {len(sector_ids)}: {sorted(sector_ids)}")

    return errors


# Public alias for backward compatibility and test imports.
# Callers using the legacy name `validate_theme_hierarchy` are supported.
validate_theme_hierarchy = validate_registry


# ── Sector normalization ───────────────────────────────────────────────────────

# Map of lowercased provider label variants → canonical sector_id.
# FMP uses different labels than canonical (e.g. "Consumer Cyclical", "Basic Materials").
# Tradier / other providers may emit their own variants.
_SECTOR_LABEL_MAP: dict[str, str] = {
    # Technology
    "technology":            "technology",
    "information technology": "technology",
    # Materials
    "materials":             "materials",
    "basic materials":       "materials",
    # Energy
    "energy":                "energy",
    # Industrials
    "industrials":           "industrials",
    "industrial":            "industrials",
    # Utilities
    "utilities":             "utilities",
    # Financials
    "financials":            "financials",
    "financial services":    "financials",
    "financial":             "financials",
    # Healthcare
    "healthcare":            "healthcare",
    "health care":           "healthcare",
    # Real Estate
    "real estate":           "real_estate",
    # Communication Services
    "communication services": "communication_services",
    "communication":          "communication_services",
    # Consumer Discretionary — FMP label is "Consumer Cyclical"
    "consumer cyclical":     "consumer_discretionary",
    "consumer discretionary": "consumer_discretionary",
    # Consumer Staples — FMP label is "Consumer Defensive"
    "consumer defensive":    "consumer_staples",
    "consumer staples":      "consumer_staples",
}


def normalize_company_sector_to_id(label: str | None) -> str | None:
    """
    Map a provider sector string (FMP, Tradier, etc.) to a canonical sector_id.

    Returns the canonical sector_id or None if the label is unknown / empty.
    Lookup is case-insensitive and strips surrounding whitespace.

    Example:
        >>> normalize_company_sector_to_id("Consumer Cyclical")
        'consumer_discretionary'
        >>> normalize_company_sector_to_id("Basic Materials")
        'materials'
        >>> normalize_company_sector_to_id("Widget XYZ")
        None
    """
    if not label:
        return None
    return _SECTOR_LABEL_MAP.get(str(label).strip().lower())
