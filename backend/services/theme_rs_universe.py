"""
Canonical Theme & Sector Registry for Themes by Relative Strength.

60 entries — 11 SPDR broad sectors + 49 canonical themes/sub-themes.

Sector consolidation (v4)
-------------------------
11 SPDR sectors added as classification="sector":
  technology, materials, consumer_discretionary, consumer_staples,
  communication_services, real_estate   (6 new entries)
  energy, financials, healthcare, industrials, utilities             (5 existing, reclassified)

All remaining 49 entries tagged classification="theme" or "sub_theme"
with parent_sector pointing to the sector they belong under.

Dedup rules applied
  - No duplicate broad-sector concept kept as a separate theme.
  - Broad SPDR sector ETF is the parent sector row.
  - Narrower themes/sub-themes coexist alongside the sector.

Structure per entry
-------------------
classification    "sector" | "theme" | "sub_theme"
parent_sector     sector theme_id this entry belongs under (None for sectors & market-wide themes)
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
        "classification": "theme",
        "parent_sector":  "consumer_staples",
        "display_name": "Agribusiness",
        "proxy_type": "etf",
        "proxy_symbols": ["MOO", "DBA", "VEGI"],
        "candidate_symbols": ["DE", "MOS", "ADM", "BG", "CF"],
        "sector_tags": ["Materials", "Consumer Staples"],
        "keywords": ["agriculture", "fertilizer", "crop", "farming", "food supply"],
        "macro_sensitivities": ["commodity prices", "weather", "food inflation", "USD"],
    },

    "ai_networking": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "AI Networking",
        "proxy_type": "custom",
        "proxy_symbols": ["ANET", "AVGO", "MRVL", "CRDO", "ALAB"],
        "candidate_symbols": [
            "ANET",  # Arista Networks — AI data center switches
            "AVGO",  # Broadcom — custom networking ASICs, optical DSPs
            "MRVL",  # Marvell Technology — custom silicon, optical DSPs
            "CRDO",  # Credo Technology — SerDes for high-speed links
            "ALAB",  # Astera Labs — PCIe/CXL retimers for AI clusters
            "CSCO",  # Cisco Systems — enterprise & DC networking
            "JNPR",  # Juniper Networks — AI-native networking
            "EXTR",  # Extreme Networks — data center fabrics
            "CIEN",  # Ciena — optical networking / 400G-800G coherent
            "INFN",  # Infinera — open optical networking
            "CALX",  # Calix — access/edge networking
            "AAOI",  # Applied Optoelectronics — high-speed transceivers
            "FN",    # Fabrinet — precision optical/networking manufacturing
            "VIAV",  # Viavi Solutions — optical test & measurement
            "COHR",  # Coherent — optical transceivers & components
            "LITE",  # Lumentum — optical components for networking
            "VISN",  # Vistance Networks — data center / enterprise communications infrastructure
        ],
        "sector_tags": ["Technology"],
        "keywords": ["AI networking", "data center networking", "optical interconnects", "switches", "silicon photonics"],
        "macro_sensitivities": ["AI capex", "data center buildout", "hyperscaler spending"],
    },

    # ── B ──────────────────────────────────────────────────────────────────────
    "banks": {
        "classification": "sub_theme",
        "parent_sector":  "financials",
        "display_name": "Banks",
        "proxy_type": "etf",
        "proxy_symbols": ["KBE", "XLF"],
        "candidate_symbols": ["JPM", "BAC", "WFC", "GS", "MS"],
        "sector_tags": ["Financials"],
        "keywords": ["banking", "credit", "lending", "deposits", "interest rates"],
        "macro_sensitivities": ["Fed rates", "yield curve", "credit spreads", "GDP"],
    },

    "biotech": {
        "classification": "sub_theme",
        "parent_sector":  "healthcare",
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
        "classification": "sub_theme",
        "parent_sector":  "materials",
        "display_name": "Chemicals & Materials",
        "proxy_type": "etf",
        "proxy_symbols": ["XLB", "IYM"],
        "candidate_symbols": ["LIN", "APD", "DD", "ECL", "SHW"],
        "sector_tags": ["Materials"],
        "keywords": ["chemicals", "specialty materials", "polymers", "industrial chemicals"],
        "macro_sensitivities": ["energy costs", "China demand", "construction activity"],
    },

    "clean_energy": {
        "classification": "theme",
        "parent_sector":  "utilities",
        "display_name": "Clean Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["ICLN", "PBW", "QCLN", "CNRG"],
        "candidate_symbols": ["ENPH", "RUN", "PLUG", "FSLR", "NEE", "CWEN", "BE"],
        "sector_tags": ["Energy", "Utilities", "Industrials"],
        "keywords": ["clean energy", "renewable", "wind", "solar", "hydrogen", "ESG"],
        "macro_sensitivities": ["IRA policy", "interest rates", "power demand", "carbon prices"],
    },

    "cloud_software": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
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

    # ── SECTOR: Communication Services ─────────────────────────────────────────
    "communication_services": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Communication Services",
        "proxy_type": "etf",
        # XLC = SPDR Communication Services ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLC", "VOX"],
        "candidate_symbols": ["META", "GOOGL", "NFLX", "DIS", "T", "VZ", "CMCSA"],
        "sector_tags": ["Communication Services"],
        "keywords": ["communication", "social media", "streaming", "telecom", "advertising"],
        "macro_sensitivities": ["ad market", "subscriber growth", "interest rates", "regulation"],
    },

    # ── SECTOR: Consumer Discretionary ────────────────────────────────────────
    "consumer_discretionary": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Consumer Discretionary",
        "proxy_type": "etf",
        # XLY = SPDR Consumer Discretionary ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLY", "VCR"],
        "candidate_symbols": ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX"],
        "sector_tags": ["Consumer Discretionary"],
        "keywords": ["consumer discretionary", "retail", "autos", "restaurants", "leisure"],
        "macro_sensitivities": ["consumer confidence", "wages", "credit conditions", "gasoline prices"],
    },

    "consumer_retail": {
        "classification": "sub_theme",
        "parent_sector":  "consumer_discretionary",
        "display_name": "Consumer Retail",
        "proxy_type": "etf",
        "proxy_symbols": ["XRT", "RTH"],
        "candidate_symbols": ["WMT", "COST", "TGT", "TJX", "AMZN"],
        "sector_tags": ["Consumer Discretionary", "Consumer Staples"],
        "keywords": ["retail", "consumer spending", "e-commerce", "discretionary"],
        "macro_sensitivities": ["consumer confidence", "wages", "inflation", "credit"],
    },

    # ── SECTOR: Consumer Staples ───────────────────────────────────────────────
    "consumer_staples": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Consumer Staples",
        "proxy_type": "etf",
        # XLP = SPDR Consumer Staples ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLP", "VDC"],
        "candidate_symbols": ["PG", "KO", "PEP", "WMT", "COST", "PM", "MDLZ"],
        "sector_tags": ["Consumer Staples"],
        "keywords": ["consumer staples", "defensive", "food", "beverages", "household products"],
        "macro_sensitivities": ["inflation", "consumer spending", "USD", "commodity costs"],
    },

    "copper_miners": {
        "classification": "sub_theme",
        "parent_sector":  "materials",
        "display_name": "Copper Miners",
        "proxy_type": "etf",
        # DBB = Invesco DB Base Metals Fund (broad base metals incl. copper, zinc, aluminum)
        # Added as secondary proxy to preserve coverage previously in old theme_service
        "proxy_symbols": ["COPX", "CPER", "DBB"],
        "candidate_symbols": ["FCX", "SCCO", "TECK", "HBM"],
        "sector_tags": ["Materials", "Mining"],
        "keywords": ["copper", "mining", "electrification", "EV infrastructure", "base metals"],
        "macro_sensitivities": ["China manufacturing", "electrification demand", "USD", "global growth"],
    },

    "crypto_equities": {
        "classification": "theme",
        "parent_sector":  "technology",
        "display_name": "Crypto Equities / Blockchain",
        "proxy_type": "etf",
        # BLOK = Amplify Transformational Data Sharing ETF (primary crypto equity basket)
        # BKCH = Global X Blockchain ETF — added as secondary proxy to preserve coverage
        #        previously tracked in old theme_service (was unique to that universe)
        "proxy_symbols": ["BLOK", "BITQ", "WGMI", "BKCH"],
        "candidate_symbols": ["MSTR", "COIN", "MARA", "CLSK", "HUT", "RIOT"],
        "sector_tags": ["Technology", "Financials"],
        "keywords": ["crypto", "blockchain", "bitcoin proxy", "mining", "digital assets"],
        "macro_sensitivities": ["bitcoin price", "regulatory risk", "risk appetite", "liquidity"],
        "aliases": ["blockchain", "crypto_equity"],
    },

    "cybersecurity": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
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
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Data Center Infrastructure",
        "proxy_type": "etf",
        # SRVR = Pacer Data & Infrastructure Real Estate ETF (primary)
        # Uranium/nuclear tickers excluded via exclude_tickers in theme_fmp_industry_map.json
        "proxy_symbols": ["SRVR", "VPN", "DTCR"],
        "candidate_symbols": [
            "EQIX",  # Equinix — global colocation, carrier-neutral data centers
            "DLR",   # Digital Realty — wholesale & colocation data centers
            "AMT",   # American Tower — data center REITs / tower infrastructure
            "SBAC",  # SBA Communications — tower + edge infra
            "CCI",   # Crown Castle — towers, small cells, fiber
            "SMCI",  # Super Micro Computer — high-density AI servers
            "IRM",   # Iron Mountain — data center colocation + storage
            "GDS",   # GDS Holdings — China data center operator
            "DBRG",  # DigitalBridge — data center-focused REIT/PE
            "NBIS",  # Nebius Group — AI-optimized data center infrastructure
            "VNET",  # 21Vianet Group — China carrier-neutral data center operator
            "IREN",  # Iris Energy — GPU cloud / AI data center operator
        ],
        "sector_tags": ["Technology", "Utilities", "Real Estate"],
        "keywords": ["data center", "REIT", "colocation", "hyperscaler infrastructure", "server infrastructure"],
        "macro_sensitivities": ["AI capex", "power demand", "interest rates", "cloud growth"],
        "aliases": ["datacenter_infrastructure", "data_center"],
    },

    "defense": {
        "classification": "sub_theme",
        "parent_sector":  "industrials",
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
        "classification": "sub_theme",
        "parent_sector":  "industrials",
        "display_name": "Drones",
        "proxy_type": "hybrid",
        "proxy_symbols": ["XAR", "ITA"],
        "candidate_symbols": ["AVAV", "KTOS", "RCAT", "JOBY", "ACHR", "EH"],
        "sector_tags": ["Technology", "Industrials", "Defense"],
        "keywords": ["drones", "UAV", "eVTOL", "autonomous flight", "air taxi"],
        "macro_sensitivities": ["FAA regulation", "defense budgets", "AI automation"],
    },

    # ── E ──────────────────────────────────────────────────────────────────────
    # SECTOR: Energy (existing entry reclassified)
    "energy": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Energy",
        "proxy_type": "etf",
        # XLE = SPDR Energy ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLE", "VDE", "XOP"],
        "candidate_symbols": ["XOM", "CVX", "COP"],
        "sector_tags": ["Energy"],
        "keywords": ["energy", "oil", "gas", "petroleum", "upstream", "downstream"],
        "macro_sensitivities": ["crude oil price", "OPEC", "global growth", "USD"],
    },

    # ── F ──────────────────────────────────────────────────────────────────────
    # SECTOR: Financials (existing entry reclassified)
    "financials": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Financials",
        "proxy_type": "etf",
        # XLF = SPDR Financial ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLF", "VFH"],
        "candidate_symbols": ["JPM", "BAC", "GS", "MS", "BRK-B"],
        "sector_tags": ["Financials"],
        "keywords": ["financials", "banks", "insurance", "asset managers", "brokers"],
        "macro_sensitivities": ["Fed rates", "credit conditions", "economic cycle"],
    },

    "fintech": {
        "classification": "sub_theme",
        "parent_sector":  "financials",
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
        "classification": "theme",
        "parent_sector":  "materials",
        "display_name": "Gold",
        "proxy_type": "etf",
        "proxy_symbols": ["GLD", "IAU", "GDX", "GDXJ"],
        "candidate_symbols": ["NEM", "GOLD", "AEM"],
        "sector_tags": ["Materials", "Commodities"],
        "keywords": ["gold", "precious metals", "safe haven", "miners", "royalties"],
        "macro_sensitivities": ["real yields", "USD", "inflation", "geopolitical risk"],
    },

    # ── H ──────────────────────────────────────────────────────────────────────
    # SECTOR: Healthcare (existing entry reclassified)
    "healthcare": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Health Care",
        "proxy_type": "etf",
        # XLV = SPDR Health Care ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLV", "VHT", "IYH"],
        "candidate_symbols": ["LLY", "UNH", "JNJ", "ABBV"],
        "sector_tags": ["Healthcare"],
        "keywords": ["healthcare", "pharma", "medical", "drug", "hospital", "managed care"],
        "macro_sensitivities": ["drug pricing policy", "aging demographics", "M&A"],
    },

    "homebuilders": {
        "classification": "sub_theme",
        "parent_sector":  "consumer_discretionary",
        "display_name": "Homebuilders",
        "proxy_type": "etf",
        "proxy_symbols": ["ITB", "XHB"],
        "candidate_symbols": ["DHI", "LEN", "PHM", "TOL"],
        "sector_tags": ["Consumer Discretionary", "Real Estate"],
        "keywords": ["homebuilders", "housing", "mortgage", "construction", "real estate"],
        "macro_sensitivities": ["mortgage rates", "housing starts", "consumer confidence"],
    },

    # ── I ──────────────────────────────────────────────────────────────────────
    # SECTOR: Industrials (existing entry reclassified)
    "industrials": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Industrials",
        "proxy_type": "etf",
        # XLI = SPDR Industrials ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLI", "VIS"],
        "candidate_symbols": ["GE", "ETN", "PH", "CAT", "DE"],
        "sector_tags": ["Industrials"],
        "keywords": ["industrials", "manufacturing", "transportation", "capital goods"],
        "macro_sensitivities": ["PMI", "capex cycle", "global trade", "infrastructure"],
    },

    "insurance": {
        "classification": "sub_theme",
        "parent_sector":  "financials",
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
        "classification": "theme",
        "parent_sector":  "materials",
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
        "classification": "sub_theme",
        "parent_sector":  "energy",
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
    # SECTOR: Materials (new entry)
    "materials": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Materials",
        "proxy_type": "etf",
        # XLB = SPDR Materials ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLB", "VAW"],
        "candidate_symbols": ["LIN", "APD", "ECL", "SHW", "FCX", "NEM", "NUE", "AA"],
        "sector_tags": ["Materials"],
        "keywords": ["materials", "chemicals", "metals", "mining", "construction materials"],
        "macro_sensitivities": ["global growth", "China demand", "commodity cycle", "USD"],
    },

    "medical_devices": {
        "classification": "sub_theme",
        "parent_sector":  "healthcare",
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
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Memory & Storage",
        "proxy_type": "basket",
        # DRAM (Defiance DRAM Memory, Storage, and AI ETF) is primary per spec.
        # Service tries DRAM first; if unavailable or insufficient bars, uses SMH/SOXX median.
        "proxy_symbols": ["DRAM", "SMH", "SOXX"],
        "candidate_symbols": [
            "MU",    # Micron: DRAM, NAND, HBM
            "WDC",   # Western Digital: NAND flash, SSD controllers
            "STX",   # Seagate: HDD/SSD storage
            "SNDK",  # SanDisk: NAND flash
            "SIMO",  # Silicon Motion: NAND flash controllers
            "RMBS",  # Rambus: memory interface IP / HBM IP
            "NVEC",  # NVE Corp: MRAM / spintronic memory
            "MRAM",  # Everspin Technologies: MRAM
            "FORM",  # FormFactor: DRAM/HBM test equipment
            "AEHR",  # Aehr Test Systems: memory burn-in testing
            "PSTG",  # Pure Storage: all-flash arrays
            "NTAP",  # NetApp: hybrid cloud storage / NVMe arrays
        ],
        "sector_tags": ["Technology", "Semiconductors"],
        "keywords": ["DRAM", "NAND", "HBM", "memory", "data storage", "flash",
                     "MRAM", "storage controller", "memory interface"],
        "macro_sensitivities": ["AI demand", "data center capex", "PC/mobile upgrade cycle"],
    },

    "metals_mining": {
        "classification": "sub_theme",
        "parent_sector":  "materials",
        "display_name": "Metals & Mining",
        "proxy_type": "etf",
        "proxy_symbols": ["XME", "PICK", "SLX"],
        "candidate_symbols": ["FCX", "CLF", "AA", "NUE", "STLD"],
        "sector_tags": ["Materials", "Mining"],
        "keywords": ["metals", "mining", "steel", "iron ore", "diversified miners"],
        "macro_sensitivities": ["China infrastructure", "global growth", "commodity cycle"],
    },

    # ── O ──────────────────────────────────────────────────────────────────────
    "oil_gas": {
        "classification": "sub_theme",
        "parent_sector":  "energy",
        "display_name": "Oil & Gas",
        "proxy_type": "etf",
        "proxy_symbols": ["XOP", "XLE", "OIH"],
        "candidate_symbols": ["XOM", "CVX", "COP", "SLB", "EOG"],
        "sector_tags": ["Energy"],
        "keywords": ["oil", "natural gas", "E&P", "upstream", "OPEC", "crude"],
        "macro_sensitivities": ["crude oil price", "OPEC+ decisions", "global demand", "USD"],
    },

    "oil_services": {
        "classification": "sub_theme",
        "parent_sector":  "energy",
        "display_name": "Oil Services",
        "proxy_type": "etf",
        # OIH = VanEck Oil Services ETF (primary; dedicated oil-field services basket)
        "proxy_symbols": ["OIH", "XES", "IEZ"],
        "candidate_symbols": ["SLB", "HAL", "BKR", "WTTR", "NRDY", "NE"],
        "sector_tags": ["Energy"],
        "keywords": ["oil services", "oilfield services", "drilling", "completion", "fracking"],
        "macro_sensitivities": ["oil price", "rig count", "E&P capex", "OPEC production levels"],
    },

    # ── P ──────────────────────────────────────────────────────────────────────
    "photonics_lasers": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Photonics / Lasers",
        "proxy_type": "custom",
        "proxy_symbols": ["IPGP", "COHR", "LITE", "AAOI"],
        "candidate_symbols": [
            "IPGP",  # IPG Photonics — fiber lasers, leader in industrial/medical lasers
            "COHR",  # Coherent Corp — lasers + optical components (merged II-VI)
            "LITE",  # Lumentum — laser chips, photonic components, 3D sensing
            "AAOI",  # Applied Optoelectronics — transceivers, optical components
            "VIAV",  # Viavi Solutions — optical test instruments
            "FN",    # Fabrinet — precision photonics manufacturing
            "LPTH",  # Light Path Technologies — optical components
            "MKSI",  # MKS Instruments — laser power supplies, photonics tools
            "LASR",  # nLIGHT Inc — high-power fiber lasers for defense, industrial, semiconductor
            "RKLY",  # Rockley Photonics — silicon photonics integrated circuits for health/comms
        ],
        "sector_tags": ["Technology"],
        "keywords": ["photonics", "lasers", "optical components", "silicon photonics", "datacom optics", "fiber laser", "LiDAR"],
        "macro_sensitivities": ["AI data center buildout", "telecom capex", "defense optics spending"],
    },

    "power_cooling": {
        "classification": "sub_theme",
        "parent_sector":  "industrials",
        "display_name": "Power / Cooling",
        "proxy_type": "custom",
        "proxy_symbols": ["VRT", "ETN", "GEV", "TT"],
        "candidate_symbols": [
            "VRT",   # Vertiv Holdings — DC power, thermal management, critical infrastructure
            "ETN",   # Eaton Corp — power distribution, PDUs, UPS, switchgear
            "GEV",   # GE Vernova — power systems, grid solutions
            "TT",    # Trane Technologies — HVAC, cooling systems
            "IR",    # Ingersoll Rand — compressed air, industrial cooling
            "NVT",   # nVent Electric — enclosures, power distribution
            "GNRC",  # Generac — backup generators, standby power
            "HUBB",  # Hubbell — electrical infrastructure, wiring devices
            "BE",    # Bloom Energy — fuel cells for clean data center power
            "GTLS",  # Chart Industries — heat exchangers, cryogenic/cooling equipment for DCs
            "AAON",  # AAON Inc — commercial HVAC rooftop units and cooling equipment
            "SPX",   # SPX Technologies — HVAC, cooling towers, power delivery systems
        ],
        "sector_tags": ["Industrials", "Technology"],
        "keywords": ["power", "cooling", "data center power", "thermal management", "UPS", "generators", "HVAC", "liquid cooling"],
        "macro_sensitivities": ["AI data center buildout", "power grid demand", "industrial capex"],
    },

    # ── Q ──────────────────────────────────────────────────────────────────────
    "quantum": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Quantum Computing",
        # proxy_type="custom" intentionally — QTUM ETF was too broad (held INTC, AMD, MU etc.)
        # Use pure-play quantum stocks as proxy anchors for RS tracking instead.
        "proxy_type": "custom",
        "proxy_symbols": ["IONQ", "RGTI", "QUBT"],
        "candidate_symbols": [
            "IONQ",  # IonQ — trapped-ion quantum computers
            "RGTI",  # Rigetti Computing — superconducting QPUs
            "QUBT",  # Quantum Computing Inc — photonic / annealing QC
            "QBTS",  # D-Wave Quantum — quantum annealing systems
            "ARQQ",  # ArQit Quantum — quantum encryption / QKD
            "IBM",   # IBM — IBM Quantum (superconducting, Qiskit ecosystem)
        ],
        "sector_tags": ["Technology"],
        "keywords": ["quantum computing", "quantum hardware", "quantum software", "qubit", "quantum error correction"],
        "macro_sensitivities": ["R&D spending", "government grants", "AI adjacency"],
        "aliases": ["quantum_computing"],
    },

    # ── R ──────────────────────────────────────────────────────────────────────
    "rare_earth": {
        "classification": "sub_theme",
        "parent_sector":  "materials",
        "display_name": "Rare Earth Metals",
        "proxy_type": "etf",
        "proxy_symbols": ["REMX"],
        "candidate_symbols": ["MP", "UUUU", "TMRC", "LYSDY"],
        "sector_tags": ["Materials", "Mining"],
        "keywords": ["rare earth", "critical minerals", "magnets", "defense supply chain"],
        "macro_sensitivities": ["China export controls", "EV demand", "defense spending"],
    },

    # SECTOR: Real Estate (new entry)
    "real_estate": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Real Estate",
        "proxy_type": "etf",
        # XLRE = SPDR Real Estate ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLRE", "VNQ"],
        "candidate_symbols": ["PLD", "AMT", "EQIX", "SPG", "CCI", "PSA", "DLR"],
        "sector_tags": ["Real Estate"],
        "keywords": ["real estate", "REIT", "commercial property", "residential", "industrial REIT"],
        "macro_sensitivities": ["interest rates", "cap rates", "property valuations", "occupancy"],
    },

    "regional_banks": {
        "classification": "sub_theme",
        "parent_sector":  "financials",
        "display_name": "Regional Banks",
        "proxy_type": "etf",
        "proxy_symbols": ["KRE", "KBE", "IAT"],
        "candidate_symbols": ["WAL", "ZION", "CMA", "KEY"],
        "sector_tags": ["Financials"],
        "keywords": ["regional banks", "community banking", "CRE", "deposits", "NIM"],
        "macro_sensitivities": ["Fed rates", "CRE exposure", "deposit stability", "yield curve"],
    },

    "robotics_automation": {
        "classification": "sub_theme",
        "parent_sector":  "industrials",
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
        "classification": "sub_theme",
        "parent_sector":  "technology",
        # Display label updated to "Semi Equipment & Materials" to reflect that
        # the basket also covers process chemicals, specialty gases, and compound
        # semiconductor materials companies alongside pure-play equipment makers.
        # theme_id "semicap_equipment" is FROZEN — changing it would break all
        # Neon theme_ticker_overrides rows and category_overrides entries.
        "display_name": "Semi Equipment & Materials",
        "proxy_type": "custom",
        # custom proxy: candidate_symbols ARE the canonical universe so all 16 seeds
        # enter static_syms unconditionally regardless of ETF file availability.
        # SOXX/SMH still listed as proxy_symbols for reference; ETF holdings provide
        # supplemental coverage if the files are on disk.
        "proxy_symbols": ["SOXX", "SMH"],
        "candidate_symbols": [
            "ASML",  # ASML Holding — EUV/DUV lithography monopoly
            "AMAT",  # Applied Materials — CVD, PVD, etch, CMP leader
            "LRCX",  # Lam Research — etch & deposition systems
            "KLAC",  # KLA Corp — process control, inspection, metrology
            "TER",   # Teradyne — semiconductor test systems
            "ACLS",  # Axcelis Technologies — ion implant systems
            "ONTO",  # Onto Innovation — metrology, inspection
            "AEHR",  # Aehr Test Systems — wafer-level burn-in
            "FORM",  # FormFactor — probe cards for wafer testing
            "CAMT",  # Camtek — optical inspection, metrology
            "UCTT",  # Ultra Clean Holdings — process components
            "MKSI",  # MKS Instruments — process control instruments
            "ENTG",  # Entegris — advanced materials / process chemicals
            "COHU",  # Cohu — test handlers, contactors
            "ACMR",  # ACM Research — wet-clean / surface preparation equipment
            "AZTA",  # Azenta (fka Brooks Automation) — semiconductor handling & cryogenics
            "ICHR",  # Ichor Holdings — fluid/gas delivery systems for etch & deposition fabs
            "AEIS",  # Advanced Energy Industries — precision RF/DC power for semicap processes
            "NVMI",  # Nova Ltd — in-line process control metrology (OCD, XRF, XPS)
        ],
        "sector_tags": ["Technology"],
        "keywords": [
            "semiconductor equipment", "lithography", "etch", "deposition",
            "ASML", "AMAT", "metrology", "inspection",
            "semiconductor materials", "semi materials", "process chemicals",
            "specialty gases", "compound semiconductors", "semicap",
        ],
        "macro_sensitivities": ["fab capex cycles", "leading-edge node ramp", "China restrictions"],
        # All aliases resolve to this same theme_id.
        # Used by _build_index() in theme_ticker_mapper for title-cased label matching.
        "aliases": [
            "semicap",
            "semiconductor_equipment",
            "semicap_equipment",
            "semi_equipment",
            "semi_materials",
            "semiconductor_materials",
            "semi_equipment_and_materials",
        ],
    },

    "semiconductors": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Semiconductors",
        "proxy_type": "etf",
        # SMH = VanEck Semiconductor ETF (primary; broadest, most liquid)
        "proxy_symbols": ["SMH", "SOXX", "XSD", "PSI"],
        "candidate_symbols": ["NVDA", "AMD", "INTC", "QCOM", "TSM", "AVGO", "ASML", "AMAT", "LRCX", "AMBA", "TXN"],
        "sector_tags": ["Technology"],
        "keywords": ["semiconductors", "chips", "fab", "GPU", "CPU", "AI chips", "fabless"],
        "macro_sensitivities": ["AI capex", "PC/mobile cycle", "China restrictions", "leading-edge node"],
        "aliases": ["chips", "chip_stocks"],
    },

    "silver": {
        "classification": "theme",
        "parent_sector":  "materials",
        "display_name": "Silver",
        "proxy_type": "etf",
        "proxy_symbols": ["SLV", "SIL", "SILJ"],
        "candidate_symbols": ["PAAS", "AG", "MAG"],
        "sector_tags": ["Materials", "Commodities"],
        "keywords": ["silver", "precious metals", "industrial metal", "solar demand"],
        "macro_sensitivities": ["gold price", "industrial demand", "real yields", "solar growth"],
    },

    "software": {
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Software",
        "proxy_type": "etf",
        "proxy_symbols": ["IGV", "WCLD"],
        "candidate_symbols": ["CRM", "NOW", "DDOG", "SNOW", "MDB"],
        "sector_tags": ["Technology"],
        "keywords": ["software", "SaaS", "cloud software", "enterprise software", "AI software"],
        "macro_sensitivities": ["enterprise IT budgets", "AI adoption", "interest rates (multiples)"],
    },

    "substrates_packaging": {  # OSAT / advanced packaging / substrate suppliers
        "classification": "sub_theme",
        "parent_sector":  "technology",
        "display_name": "Substrates / Packaging",
        "proxy_type": "custom",
        "proxy_symbols": ["AMKR", "ASX"],
        "candidate_symbols": [
            "AMKR",   # Amkor Technology — largest US-listed OSAT
            "ASX",    # ASE Technology — largest global OSAT (SiC/advanced pkg)
            "IBIDF",  # Ibiden — ABF buildup substrate supplier for Intel/TSMC
            "UMICF",  # Unimicron Technology — ABF/PCB substrate supplier
            "KLIC",   # Kulicke & Soffa — wire bonding + advanced bonding equipment
            "BESI",   # BE Semiconductor — die-attach, flip-chip bonding
            "ONTO",   # Onto Innovation — advanced packaging metrology/inspection
            "ASMVF",  # ASM Pacific Technology — back-end assembly equipment
        ],
        "sector_tags": ["Technology", "Semiconductors"],
        "keywords": ["advanced packaging", "chiplets", "CoWoS", "HBM substrate", "ABF substrate", "OSAT", "interposers"],
        "macro_sensitivities": ["AI chip demand", "fab capex cycles", "leading-edge packaging ramp"],
    },

    "solar": {
        "classification": "sub_theme",
        "parent_sector":  "utilities",
        "display_name": "Solar",
        "proxy_type": "etf",
        "proxy_symbols": ["TAN"],
        "candidate_symbols": ["FSLR", "ENPH", "SEDG", "ARRY"],
        "sector_tags": ["Energy", "Utilities", "Clean Tech"],
        "keywords": ["solar", "photovoltaic", "rooftop solar", "utility-scale", "panels"],
        "macro_sensitivities": ["IRA/solar policy", "China competition", "interest rates", "power demand"],
    },

    "space": {
        "classification": "sub_theme",
        "parent_sector":  "industrials",
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

    # ── T ──────────────────────────────────────────────────────────────────────
    # SECTOR: Technology (new entry)
    "technology": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Technology",
        "proxy_type": "etf",
        # XLK = SPDR Technology ETF (primary SPDR sector ETF)
        "proxy_symbols": ["XLK", "VGT"],
        "candidate_symbols": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CRM", "AMD"],
        "sector_tags": ["Technology"],
        "keywords": ["technology", "software", "hardware", "semiconductors", "AI", "cloud"],
        "macro_sensitivities": ["AI capex", "enterprise IT spending", "interest rates", "antitrust"],
    },

    "travel_transportation": {
        "classification": "sub_theme",
        "parent_sector":  "industrials",
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
        "classification": "sub_theme",
        "parent_sector":  "utilities",
        "display_name": "Uranium & Nuclear Energy",
        "proxy_type": "etf",
        "proxy_symbols": ["URA", "URNM", "NLR"],
        "candidate_symbols": ["CCJ", "LEU", "BWXT", "SMR", "OKLO"],
        "sector_tags": ["Energy", "Utilities"],
        "keywords": ["uranium", "nuclear", "small modular reactors", "SMR", "clean power"],
        "macro_sensitivities": ["AI power demand", "energy policy", "carbon neutrality"],
        "aliases": ["nuclear_uranium"],
    },

    # SECTOR: Utilities (existing entry reclassified)
    "utilities": {
        "classification": "sector",
        "parent_sector":  None,
        "display_name": "Utilities",
        "proxy_type": "etf",
        # XLU = SPDR Utilities ETF (primary SPDR sector ETF)
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
