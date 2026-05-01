"""
Canonical Theme Registry for Themes by Relative Strength.

49 themes — original 39 new RS themes + 10 merged from old Sectors > Themes tab.
The 10 additions are: semiconductors, semicap_equipment, datacenter_infra,
cloud_software, oil_services, lng_gas, medical_devices, crypto_equities,
quantum, space.

Structure per entry
-------------------
proxy_type        "etf" | "basket" | "hybrid"
proxy_symbols     ETF/index tickers used for price-history performance
                  (primary ETF first, then backup ETFs — no individual stocks)
candidate_symbols Individual stocks used ONLY for per-theme leaders/laggards
                  (dynamic ETF-holdings discovery runs first; these are
                   last-resort static fallback seeds only)
sector_tags       broad sector labels
keywords          NLP/search tags
macro_sensitivities macro drivers
"""
from __future__ import annotations

THEME_RS_UNIVERSE: dict[str, dict] = {

    # ── A ──────────────────────────────────────────────────────────────────────
    "agribusiness": {
        "display_name": "Agribusiness",
        "proxy_type": "etf",
        "proxy_symbols": ["MOO", "DBA", "VEGI"],
        "candidate_symbols": ["DE", "MOS", "ADM", "BG", "CF"],
        "sector_tags": ["Materials", "Consumer Staples"],
        "keywords": ["agriculture", "fertilizer", "crop", "farming", "food supply"],
        "macro_sensitivities": ["commodity prices", "weather", "food inflation", "USD"],
    },

    # ── B ──────────────────────────────────────────────────────────────────────
    "banks": {
        "display_name": "Banks",
        "proxy_type": "etf",
        "proxy_symbols": ["KBE", "XLF"],
        "candidate_symbols": ["JPM", "BAC", "WFC", "GS", "MS"],
        "sector_tags": ["Financials"],
        "keywords": ["banking", "credit", "lending", "deposits", "interest rates"],
        "macro_sensitivities": ["Fed rates", "yield curve", "credit spreads", "GDP"],
    },

    "biotech": {
        "display_name": "Biotech",
        "proxy_type": "etf",
        "proxy_symbols": ["XBI", "IBB", "ARKG"],
        "candidate_symbols": ["MRNA", "REGN", "VRTX", "BIIB", "GILD", "ALNY"],
        "sector_tags": ["Healthcare"],
        "keywords": ["biotech", "biopharma", "clinical trials", "FDA", "drug development"],
        "macro_sensitivities": ["FDA approvals", "risk appetite", "M&A activity"],
    },

    # ── C ──────────────────────────────────────────────────────────────────────
    "chemicals_materials": {
        "display_name": "Chemicals & Materials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLB", "IYM"],
        "candidate_symbols": ["LIN", "APD", "DD", "ECL", "SHW"],
        "sector_tags": ["Materials"],
        "keywords": ["chemicals", "specialty materials", "polymers", "industrial chemicals"],
        "macro_sensitivities": ["energy costs", "China demand", "construction activity"],
    },

    "clean_energy": {
        "display_name": "Clean Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["ICLN", "PBW", "QCLN", "CNRG"],
        "candidate_symbols": ["ENPH", "RUN", "PLUG", "FSLR", "NEE", "CWEN", "BE"],
        "sector_tags": ["Energy", "Utilities", "Industrials"],
        "keywords": ["clean energy", "renewable", "wind", "solar", "hydrogen", "ESG"],
        "macro_sensitivities": ["IRA policy", "interest rates", "power demand", "carbon prices"],
    },

    "cloud_software": {
        "display_name": "Cloud Software",
        "proxy_type": "etf",
        # SKYY = First Trust Cloud Computing ETF (primary); CLOU = Global X Cloud Computing ETF
        "proxy_symbols": ["SKYY", "CLOU"],
        "candidate_symbols": ["SNOW", "DDOG", "MDB", "NET", "AMZN", "MSFT", "GOOGL"],
        "sector_tags": ["Technology"],
        "keywords": ["cloud", "SaaS", "cloud computing", "hyperscaler", "AWS", "Azure"],
        "macro_sensitivities": ["enterprise IT budgets", "AI adoption", "interest rates (multiples)"],
        "aliases": ["cloud_computing"],
    },

    "consumer_retail": {
        "display_name": "Consumer Retail",
        "proxy_type": "etf",
        "proxy_symbols": ["XRT", "RTH"],
        "candidate_symbols": ["WMT", "COST", "TGT", "TJX", "AMZN"],
        "sector_tags": ["Consumer Discretionary", "Consumer Staples"],
        "keywords": ["retail", "consumer spending", "e-commerce", "discretionary"],
        "macro_sensitivities": ["consumer confidence", "wages", "inflation", "credit"],
    },

    "copper_miners": {
        "display_name": "Copper Miners",
        "proxy_type": "etf",
        "proxy_symbols": ["COPX", "CPER"],
        "candidate_symbols": ["FCX", "SCCO", "TECK", "HBM"],
        "sector_tags": ["Materials", "Mining"],
        "keywords": ["copper", "mining", "electrification", "EV infrastructure"],
        "macro_sensitivities": ["China manufacturing", "electrification demand", "USD", "global growth"],
    },

    "crypto_equities": {
        "display_name": "Crypto Equities / Blockchain",
        "proxy_type": "etf",
        # BLOK = Amplify Transformational Data Sharing ETF (primary crypto equity basket)
        "proxy_symbols": ["BLOK", "BITQ", "WGMI"],
        "candidate_symbols": ["MSTR", "COIN", "MARA", "CLSK", "HUT", "RIOT"],
        "sector_tags": ["Technology", "Financials"],
        "keywords": ["crypto", "blockchain", "bitcoin proxy", "mining", "digital assets"],
        "macro_sensitivities": ["bitcoin price", "regulatory risk", "risk appetite", "liquidity"],
        "aliases": ["blockchain", "crypto_equity"],
    },

    "cybersecurity": {
        "display_name": "Cybersecurity",
        "proxy_type": "etf",
        "proxy_symbols": ["CIBR", "HACK", "BUG", "IHAK"],
        "candidate_symbols": ["PANW", "CRWD", "FTNT", "ZS", "CYBR"],
        "sector_tags": ["Technology"],
        "keywords": ["cybersecurity", "network security", "identity", "SIEM", "zero-trust"],
        "macro_sensitivities": ["threat landscape", "enterprise IT budgets", "AI adoption"],
    },

    # ── D ──────────────────────────────────────────────────────────────────────
    "datacenter_infra": {
        "display_name": "Data Center Infrastructure",
        "proxy_type": "etf",
        # SRVR = Pacer Data & Infrastructure Real Estate ETF (primary)
        "proxy_symbols": ["SRVR", "VPN", "DTCR"],
        "candidate_symbols": ["EQIX", "DLR", "VRT", "ETN", "PWR", "CEG"],
        "sector_tags": ["Technology", "Utilities", "Real Estate"],
        "keywords": ["data center", "REIT", "power", "colocation", "hyperscaler infrastructure"],
        "macro_sensitivities": ["AI capex", "power demand", "interest rates", "cloud growth"],
        "aliases": ["datacenter_infrastructure", "data_center"],
    },

    "defense": {
        "display_name": "Defense",
        "proxy_type": "etf",
        # ITA = iShares U.S. Aerospace & Defense ETF (primary)
        # DFEN (3× leveraged) excluded from proxies to avoid distorting performance median
        "proxy_symbols": ["ITA", "XAR", "PPA"],
        "candidate_symbols": ["LMT", "RTX", "NOC", "KTOS", "AVAV"],
        "sector_tags": ["Industrials", "Defense"],
        "keywords": ["defense", "aerospace", "military", "government contracts", "NATO"],
        "macro_sensitivities": ["geopolitical risk", "defense budgets", "NATO spending"],
        "aliases": ["aerospace_defense"],
    },

    "drones": {
        "display_name": "Drones",
        "proxy_type": "hybrid",
        "proxy_symbols": ["XAR", "ITA"],
        "candidate_symbols": ["AVAV", "KTOS", "RCAT", "JOBY", "ACHR", "EH"],
        "sector_tags": ["Technology", "Industrials", "Defense"],
        "keywords": ["drones", "UAV", "eVTOL", "autonomous flight", "air taxi"],
        "macro_sensitivities": ["FAA regulation", "defense budgets", "AI automation"],
    },

    # ── E ──────────────────────────────────────────────────────────────────────
    "energy": {
        "display_name": "Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["XLE", "VDE", "XOP"],
        "candidate_symbols": ["XOM", "CVX", "COP"],
        "sector_tags": ["Energy"],
        "keywords": ["energy", "oil", "gas", "petroleum", "upstream", "downstream"],
        "macro_sensitivities": ["crude oil price", "OPEC", "global growth", "USD"],
    },

    "equal_weight_sp500": {
        "display_name": "Equal-Weighted S&P 500",
        "proxy_type": "etf",
        "proxy_symbols": ["RSP"],
        "candidate_symbols": [],
        "sector_tags": ["Broad Market"],
        "keywords": ["S&P 500", "equal weight", "diversified", "broad market"],
        "macro_sensitivities": ["interest rates", "earnings growth", "economic cycle"],
    },

    # ── F ──────────────────────────────────────────────────────────────────────
    "financials": {
        "display_name": "Financials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLF", "VFH"],
        "candidate_symbols": ["JPM", "BAC", "GS", "MS", "BRK-B"],
        "sector_tags": ["Financials"],
        "keywords": ["financials", "banks", "insurance", "asset managers", "brokers"],
        "macro_sensitivities": ["Fed rates", "credit conditions", "economic cycle"],
    },

    "fintech": {
        "display_name": "Fintech",
        "proxy_type": "etf",
        "proxy_symbols": ["FINX", "ARKF", "IPAY"],
        "candidate_symbols": ["SQ", "PYPL", "SOFI", "HOOD", "AFRM"],
        "sector_tags": ["Technology", "Financials"],
        "keywords": ["fintech", "payments", "digital banking", "crypto", "BNPL"],
        "macro_sensitivities": ["consumer spending", "interest rates", "regulatory risk"],
    },

    # ── G ──────────────────────────────────────────────────────────────────────
    "gold": {
        "display_name": "Gold",
        "proxy_type": "etf",
        "proxy_symbols": ["GLD", "IAU", "GDX", "GDXJ"],
        "candidate_symbols": ["NEM", "GOLD", "AEM"],
        "sector_tags": ["Materials", "Commodities"],
        "keywords": ["gold", "precious metals", "safe haven", "miners", "royalties"],
        "macro_sensitivities": ["real yields", "USD", "inflation", "geopolitical risk"],
    },

    "growth_stocks": {
        "display_name": "Growth Stocks",
        "proxy_type": "etf",
        "proxy_symbols": ["IWF", "IVW", "VUG", "QQQ"],
        "candidate_symbols": [],
        "sector_tags": ["Technology", "Consumer Discretionary"],
        "keywords": ["growth", "high growth", "momentum", "tech growth", "expansion"],
        "macro_sensitivities": ["real rates", "risk appetite", "EPS growth", "multiple expansion"],
    },

    # ── H ──────────────────────────────────────────────────────────────────────
    "healthcare": {
        "display_name": "Healthcare",
        "proxy_type": "etf",
        "proxy_symbols": ["XLV", "VHT", "IYH"],
        "candidate_symbols": ["LLY", "UNH", "JNJ", "ABBV"],
        "sector_tags": ["Healthcare"],
        "keywords": ["healthcare", "pharma", "medical", "drug", "hospital", "managed care"],
        "macro_sensitivities": ["drug pricing policy", "aging demographics", "M&A"],
    },

    "homebuilders": {
        "display_name": "Homebuilders",
        "proxy_type": "etf",
        "proxy_symbols": ["ITB", "XHB"],
        "candidate_symbols": ["DHI", "LEN", "PHM", "TOL"],
        "sector_tags": ["Consumer Discretionary", "Real Estate"],
        "keywords": ["homebuilders", "housing", "mortgage", "construction", "real estate"],
        "macro_sensitivities": ["mortgage rates", "housing starts", "consumer confidence"],
    },

    # ── I ──────────────────────────────────────────────────────────────────────
    "ibd50": {
        "display_name": "IBD 50",
        "proxy_type": "etf",
        "proxy_symbols": ["FFTY"],
        "candidate_symbols": [],
        "sector_tags": ["Growth", "Momentum"],
        "keywords": ["IBD 50", "momentum", "growth leaders", "breakouts", "CAN SLIM"],
        "macro_sensitivities": ["risk appetite", "earnings growth", "market breadth"],
    },

    "industrials": {
        "display_name": "Industrials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLI", "VIS"],
        "candidate_symbols": ["GE", "ETN", "PH", "CAT", "DE"],
        "sector_tags": ["Industrials"],
        "keywords": ["industrials", "manufacturing", "transportation", "capital goods"],
        "macro_sensitivities": ["PMI", "capex cycle", "global trade", "infrastructure"],
    },

    "insurance": {
        "display_name": "Insurance",
        "proxy_type": "etf",
        "proxy_symbols": ["KIE", "IAK"],
        "candidate_symbols": ["PGR", "CB", "ACGL", "ALL"],
        "sector_tags": ["Financials"],
        "keywords": ["insurance", "P&C", "life insurance", "reinsurance", "underwriting"],
        "macro_sensitivities": ["interest rates", "catastrophe risk", "claims trends"],
    },

    # ── L ──────────────────────────────────────────────────────────────────────
    "lithium_battery": {
        "display_name": "Lithium & Battery Tech",
        "proxy_type": "etf",
        "proxy_symbols": ["LIT", "BATT"],
        "candidate_symbols": ["ALB", "SQM", "LAC", "ENVX", "QS"],
        "sector_tags": ["Materials", "Industrials"],
        "keywords": ["lithium", "batteries", "EV", "energy storage", "solid-state"],
        "macro_sensitivities": ["EV adoption", "China supply", "critical minerals policy"],
        "aliases": ["lithium_batteries"],
    },

    "lng_gas": {
        "display_name": "LNG & Natural Gas",
        "proxy_type": "etf",
        # FCG = First Trust Natural Gas ETF (E&P + midstream); UNG = commodity front-month
        "proxy_symbols": ["FCG", "UNG"],
        "candidate_symbols": ["LNG", "WMB", "KMI", "FLNG", "NEXT"],
        "sector_tags": ["Energy"],
        "keywords": ["LNG", "natural gas", "liquefied natural gas", "midstream", "pipeline"],
        "macro_sensitivities": ["nat gas prices", "export demand", "Europe energy", "winter demand"],
    },

    # ── M ──────────────────────────────────────────────────────────────────────
    "medical_devices": {
        "display_name": "Medical Devices",
        "proxy_type": "etf",
        # IHI = iShares U.S. Medical Devices ETF (primary; no liquid alternatives)
        "proxy_symbols": ["IHI"],
        "candidate_symbols": ["MDT", "ABT", "SYK", "EW", "ISRG", "BDX", "GEHC"],
        "sector_tags": ["Healthcare"],
        "keywords": ["medical devices", "surgical robots", "MedTech", "diagnostics", "implants"],
        "macro_sensitivities": ["procedure volumes", "hospital budgets", "FDA approvals", "M&A"],
    },

    "memory_storage": {
        "display_name": "Memory & Storage",
        "proxy_type": "basket",
        # DRAM (Defiance DRAM Memory, Storage, and AI ETF) is primary per spec.
        # Service tries DRAM first; if unavailable or insufficient bars, uses SMH/SOXX median.
        "proxy_symbols": ["DRAM", "SMH", "SOXX"],
        "candidate_symbols": ["MU", "WDC", "STX", "SIMO"],
        "sector_tags": ["Technology", "Semiconductors"],
        "keywords": ["DRAM", "NAND", "HBM", "memory", "data storage", "flash"],
        "macro_sensitivities": ["AI demand", "data center capex", "PC/mobile upgrade cycle"],
    },

    "metals_mining": {
        "display_name": "Metals & Mining",
        "proxy_type": "etf",
        "proxy_symbols": ["XME", "PICK", "SLX"],
        "candidate_symbols": ["FCX", "CLF", "AA", "NUE", "STLD"],
        "sector_tags": ["Materials", "Mining"],
        "keywords": ["metals", "mining", "steel", "iron ore", "diversified miners"],
        "macro_sensitivities": ["China infrastructure", "global growth", "commodity cycle"],
    },

    "microcaps": {
        "display_name": "Microcaps",
        "proxy_type": "etf",
        "proxy_symbols": ["IWC", "FDM"],
        "candidate_symbols": [],
        "sector_tags": ["Broad Market", "Small Cap"],
        "keywords": ["microcap", "small company", "speculative", "illiquid"],
        "macro_sensitivities": ["risk appetite", "liquidity conditions", "M&A activity"],
    },

    # ── O ──────────────────────────────────────────────────────────────────────
    "oil_gas": {
        "display_name": "Oil & Gas",
        "proxy_type": "etf",
        "proxy_symbols": ["XOP", "XLE", "OIH"],
        "candidate_symbols": ["XOM", "CVX", "COP", "SLB", "EOG"],
        "sector_tags": ["Energy"],
        "keywords": ["oil", "natural gas", "E&P", "upstream", "OPEC", "crude"],
        "macro_sensitivities": ["crude oil price", "OPEC+ decisions", "global demand", "USD"],
    },

    "oil_services": {
        "display_name": "Oil Services",
        "proxy_type": "etf",
        # OIH = VanEck Oil Services ETF (primary; dedicated oil-field services basket)
        "proxy_symbols": ["OIH", "XES", "IEZ"],
        "candidate_symbols": ["SLB", "HAL", "BKR", "WTTR", "NRDY", "NE"],
        "sector_tags": ["Energy"],
        "keywords": ["oil services", "oilfield services", "drilling", "completion", "fracking"],
        "macro_sensitivities": ["oil price", "rig count", "E&P capex", "OPEC production levels"],
    },

    # ── Q ──────────────────────────────────────────────────────────────────────
    "quantum": {
        "display_name": "Quantum Computing",
        "proxy_type": "etf",
        # QTUM = Defiance Quantum ETF (only liquid ETF proxy for quantum + quantum-adjacent)
        "proxy_symbols": ["QTUM"],
        "candidate_symbols": ["IONQ", "RGTI", "QUBT", "QBTS", "IBM"],
        "sector_tags": ["Technology"],
        "keywords": ["quantum computing", "quantum hardware", "quantum software", "qubit"],
        "macro_sensitivities": ["R&D spending", "government grants", "AI adjacency"],
        "aliases": ["quantum_computing"],
    },

    # ── R ──────────────────────────────────────────────────────────────────────
    "rare_earth": {
        "display_name": "Rare Earth Metals",
        "proxy_type": "etf",
        "proxy_symbols": ["REMX"],
        "candidate_symbols": ["MP", "UUUU", "TMRC", "LYSDY"],
        "sector_tags": ["Materials", "Mining"],
        "keywords": ["rare earth", "critical minerals", "magnets", "defense supply chain"],
        "macro_sensitivities": ["China export controls", "EV demand", "defense spending"],
    },

    "regional_banks": {
        "display_name": "Regional Banks",
        "proxy_type": "etf",
        "proxy_symbols": ["KRE", "KBE", "IAT"],
        "candidate_symbols": ["WAL", "ZION", "CMA", "KEY"],
        "sector_tags": ["Financials"],
        "keywords": ["regional banks", "community banking", "CRE", "deposits", "NIM"],
        "macro_sensitivities": ["Fed rates", "CRE exposure", "deposit stability", "yield curve"],
    },

    "robotics_automation": {
        "display_name": "Robotics & Automation",
        "proxy_type": "etf",
        "proxy_symbols": ["BOTZ", "ROBO", "IRBO", "ARKQ"],
        "candidate_symbols": ["SYM", "ISRG", "TER", "ABBNY"],
        "sector_tags": ["Technology", "Industrials"],
        "keywords": ["robotics", "automation", "AI robotics", "industrial robots", "cobots"],
        "macro_sensitivities": ["labor costs", "AI adoption", "capex", "manufacturing reshoring"],
    },

    # ── S ──────────────────────────────────────────────────────────────────────
    "semicap_equipment": {
        "display_name": "Semiconductor Equipment",
        "proxy_type": "etf",
        # SOXX has ~20% weight in equipment makers (AMAT, LRCX, KLAC, ASML via ADR)
        # SMH is more fab-weighted; SOXX is closer to equipment.
        "proxy_symbols": ["SOXX", "SMH"],
        "candidate_symbols": ["ASML", "AMAT", "LRCX", "KLAC", "TER", "ACLS", "ONTO"],
        "sector_tags": ["Technology"],
        "keywords": ["semiconductor equipment", "lithography", "etch", "deposition", "ASML", "AMAT"],
        "macro_sensitivities": ["fab capex cycles", "leading-edge node ramp", "China restrictions"],
        "aliases": ["semicap", "semiconductor_equipment"],
    },

    "semiconductors": {
        "display_name": "Semiconductors",
        "proxy_type": "etf",
        # SMH = VanEck Semiconductor ETF (primary; broadest, most liquid)
        "proxy_symbols": ["SMH", "SOXX", "XSD", "PSI"],
        "candidate_symbols": ["NVDA", "AMD", "INTC", "QCOM", "TSM", "AVGO", "ASML", "AMAT", "LRCX"],
        "sector_tags": ["Technology"],
        "keywords": ["semiconductors", "chips", "fab", "GPU", "CPU", "AI chips", "fabless"],
        "macro_sensitivities": ["AI capex", "PC/mobile cycle", "China restrictions", "leading-edge node"],
        "aliases": ["chips", "chip_stocks"],
    },

    "silver": {
        "display_name": "Silver",
        "proxy_type": "etf",
        "proxy_symbols": ["SLV", "SIL", "SILJ"],
        "candidate_symbols": ["PAAS", "AG", "MAG"],
        "sector_tags": ["Materials", "Commodities"],
        "keywords": ["silver", "precious metals", "industrial metal", "solar demand"],
        "macro_sensitivities": ["gold price", "industrial demand", "real yields", "solar growth"],
    },

    "small_caps": {
        "display_name": "Small Caps",
        "proxy_type": "etf",
        "proxy_symbols": ["IWM", "IJR", "VB"],
        "candidate_symbols": [],
        "sector_tags": ["Broad Market"],
        "keywords": ["small cap", "Russell 2000", "domestic economy", "risk-on"],
        "macro_sensitivities": ["domestic growth", "credit conditions", "risk appetite", "USD"],
    },

    "software": {
        "display_name": "Software",
        "proxy_type": "etf",
        "proxy_symbols": ["IGV", "WCLD"],
        "candidate_symbols": ["CRM", "NOW", "DDOG", "SNOW", "MDB"],
        "sector_tags": ["Technology"],
        "keywords": ["software", "SaaS", "cloud software", "enterprise software", "AI software"],
        "macro_sensitivities": ["enterprise IT budgets", "AI adoption", "interest rates (multiples)"],
    },

    "solar": {
        "display_name": "Solar",
        "proxy_type": "etf",
        "proxy_symbols": ["TAN"],
        "candidate_symbols": ["FSLR", "ENPH", "SEDG", "ARRY"],
        "sector_tags": ["Energy", "Utilities", "Clean Tech"],
        "keywords": ["solar", "photovoltaic", "rooftop solar", "utility-scale", "panels"],
        "macro_sensitivities": ["IRA/solar policy", "China competition", "interest rates", "power demand"],
    },

    "space": {
        "display_name": "Space Economy",
        "proxy_type": "etf",
        # ARKX = ARK Space Exploration & Innovation ETF (primary)
        # UFO = Procure Space ETF (pure-play satellite/launch)
        "proxy_symbols": ["ARKX", "UFO"],
        "candidate_symbols": ["RKLB", "ASTS", "LUNR", "SPIR", "SATL"],
        "sector_tags": ["Technology", "Industrials"],
        "keywords": ["space", "satellites", "launch vehicles", "orbital economy", "defense space"],
        "macro_sensitivities": ["government space contracts", "Starlink competition", "constellation buildout"],
        "aliases": ["space_economy"],
    },

    "speculative_tech": {
        "display_name": "Speculative Tech",
        "proxy_type": "etf",
        "proxy_symbols": ["ARKK", "ARKW", "ARKQ"],
        "candidate_symbols": ["PLTR", "COIN", "TSLA", "ROKU"],
        "sector_tags": ["Technology", "Innovation"],
        "keywords": ["speculative", "disruptive", "high-beta", "innovation", "ARK"],
        "macro_sensitivities": ["risk appetite", "interest rates", "AI hype cycle", "liquidity"],
    },

    # ── T ──────────────────────────────────────────────────────────────────────
    "tech_equal_weight": {
        "display_name": "Tech Equal-Weight",
        "proxy_type": "etf",
        # RYT = Invesco S&P 500 Equal Weight Technology ETF (primary per spec)
        # QQEW = First Trust NASDAQ-100 Equal Weighted Index ETF
        "proxy_symbols": ["RYT", "QQEW"],
        "candidate_symbols": [],
        "sector_tags": ["Technology"],
        "keywords": ["tech equal weight", "equal weight", "diversified tech"],
        "macro_sensitivities": ["interest rates", "AI adoption", "earnings breadth"],
    },

    "tech_mega_caps": {
        "display_name": "Tech Mega Caps",
        "proxy_type": "basket",
        # MAGS = Roundhill Magnificent Seven ETF (primary per spec)
        "proxy_symbols": ["MAGS", "QQQ"],
        "candidate_symbols": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA"],
        "sector_tags": ["Technology", "Consumer Discretionary"],
        "keywords": ["mega cap", "Magnificent 7", "FAANG", "big tech"],
        "macro_sensitivities": ["AI capex", "antitrust risk", "interest rates", "ad market"],
    },

    "travel_transportation": {
        "display_name": "Travel & Transportation",
        "proxy_type": "etf",
        "proxy_symbols": ["IYT", "JETS", "XTN"],
        "candidate_symbols": ["UAL", "DAL", "UPS", "FDX"],
        "sector_tags": ["Industrials", "Consumer Discretionary"],
        "keywords": ["travel", "airlines", "hotels", "cruise", "transportation", "logistics"],
        "macro_sensitivities": ["consumer spending", "fuel costs", "post-COVID recovery"],
    },

    # ── U ──────────────────────────────────────────────────────────────────────
    "uranium_nuclear": {
        "display_name": "Uranium & Nuclear Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["URA", "URNM", "NLR"],
        "candidate_symbols": ["CCJ", "LEU", "BWXT", "SMR", "OKLO"],
        "sector_tags": ["Energy", "Utilities"],
        "keywords": ["uranium", "nuclear", "small modular reactors", "SMR", "clean power"],
        "macro_sensitivities": ["AI power demand", "energy policy", "carbon neutrality"],
        "aliases": ["nuclear_uranium"],
    },

    "utilities": {
        "display_name": "Utilities",
        "proxy_type": "etf",
        "proxy_symbols": ["XLU", "VPU"],
        "candidate_symbols": ["NEE", "CEG", "VST", "SO", "DUK"],
        "sector_tags": ["Utilities"],
        "keywords": ["utilities", "electric", "regulated", "dividend", "defensive"],
        "macro_sensitivities": ["interest rates", "power demand", "regulatory environment"],
    },
}


ALL_PROXY_SYMBOLS: list[str] = sorted(
    set(sym for v in THEME_RS_UNIVERSE.values() for sym in v["proxy_symbols"])
)

ALL_CANDIDATE_SYMBOLS: list[str] = sorted(
    set(sym for v in THEME_RS_UNIVERSE.values() for sym in v["candidate_symbols"])
    - {""}   # guard against empty strings
)
