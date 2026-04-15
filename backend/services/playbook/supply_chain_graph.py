"""
Supply Chain Graph — curated multi-layer chain model for Serenity discovery.

Layer structure:
  0 = Giant / End-Demand Anchor
  1 = Core Systems / Direct Integrators
  2 = Key Components / Subsystems
  3 = Constrained Subcomponents / Bottleneck Positions
  4 = Upstream Materials / Tooling / Support

NODE_REGISTRY is the authoritative curated company database.
Graph is NOT inferred by LLM — curated maps only.
Perplexity/Gemini called only for shortlisted validation in discovery_enrichment.py.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from services.playbook.discovery_types import ChainNode, ChainLayer

# ── Layer metadata ─────────────────────────────────────────────────────────────

LAYER_LABELS: Dict[int, str] = {
    0: "Giant / End-Demand Anchor",
    1: "Core Systems / Direct Integrators",
    2: "Key Components / Subsystems",
    3: "Constrained Subcomponents / Bottlenecks",
    4: "Upstream Materials / Tooling / Support",
}

LAYER_DESCRIPTIONS: Dict[int, str] = {
    0: "Platform companies or capex cycles that anchor downstream demand",
    1: "Companies directly integrating into the giant's product or data center",
    2: "Key component suppliers one step removed from the end platform",
    3: "Highly constrained, hard-to-substitute suppliers — peak bottleneck positions",
    4: "Upstream materials, tooling, and process support enabling all layers above",
}

# ── Curated Node Registry ──────────────────────────────────────────────────────
# ticker → node dict
# Foreign tickers use native format; us_access_proxy = US-listed ADR/ETF

NODE_REGISTRY: Dict[str, Dict[str, Any]] = {

    # ───────────────────────────── AI / GPU SILICON ───────────────────────────

    "NVDA": {
        "company_name": "NVIDIA", "country": "US", "exchange": "NASDAQ",
        "role": "AI GPU platform — H100/H200/B200 training and inference anchor",
        "themes": ["ai_infrastructure", "advanced_packaging_test", "memory", "ai_power_energy"],
        "layer": 0, "bottleneck_score": 95, "confidence": "high",
        "evidence": ["Dominant AI GPU supplier globally", "H100/H200 >85% data center AI market"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "TSM": {
        "company_name": "TSMC", "country": "TW", "exchange": "TSE",
        "role": "Leading-node foundry — manufactures 100% of NVDA's GPUs; CoWoS packaging",
        "themes": ["semicap_supply_chain", "advanced_packaging_test"],
        "layer": 1, "bottleneck_score": 92, "confidence": "high",
        "evidence": ["100% of NVDA H100/H200 fabbed at TSMC", "Only scaled CoWoS capacity globally"],
        "us_access_proxy": "TSM", "adr_ticker": "TSM",
        "giant_anchors": ["NVDA", "AVGO", "GOOGL", "META"],
    },
    "ASML": {
        "company_name": "ASML Holding", "country": "NL", "exchange": "AMS",
        "role": "Sole EUV scanner supplier — required for all leading-edge nodes",
        "themes": ["semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 98, "confidence": "high",
        "evidence": ["Only company producing EUV scanners", "TSMC, Samsung, Intel all dependent"],
        "us_access_proxy": "ASML", "adr_ticker": "ASML",
        "giant_anchors": ["TSM", "NVDA", "AVGO"],
    },
    "MU": {
        "company_name": "Micron Technology", "country": "US", "exchange": "NASDAQ",
        "role": "HBM memory stacked on AI accelerators — 3-player global market",
        "themes": ["memory", "ai_infrastructure"],
        "layer": 2, "bottleneck_score": 83, "confidence": "high",
        "evidence": ["HBM3/HBM3E for NVDA H100/H200", "Only 3 HBM suppliers globally"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "GOOGL", "META"],
    },
    "AMKR": {
        "company_name": "Amkor Technology", "country": "US", "exchange": "NASDAQ",
        "role": "Advanced OSAT packaging for AI and mobile chips",
        "themes": ["advanced_packaging_test"],
        "layer": 2, "bottleneck_score": 78, "confidence": "high",
        "evidence": ["CoWoS-like packaging capability", "TSMC OSAT partner"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "AVGO"],
    },
    "ENTG": {
        "company_name": "Entegris", "country": "US", "exchange": "NASDAQ",
        "role": "CMP slurries, photoresist, and process chemicals — approved at EUV nodes",
        "themes": ["semicap_supply_chain", "advanced_packaging_test"],
        "layer": 3, "bottleneck_score": 82, "confidence": "high",
        "evidence": ["Sole-approved photoresist at TSMC EUV process", "Critical materials bottleneck"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM", "NVDA"],
    },
    "AMAT": {
        "company_name": "Applied Materials", "country": "US", "exchange": "NASDAQ",
        "role": "Etch, deposition, and ion-implant — largest semicap equipment company",
        "themes": ["semicap_supply_chain", "advanced_packaging_test"],
        "layer": 3, "bottleneck_score": 88, "confidence": "high",
        "evidence": ["$27B revenue FY24", "Critical for every advanced logic and memory node"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM", "NVDA"],
    },
    "LRCX": {
        "company_name": "Lam Research", "country": "US", "exchange": "NASDAQ",
        "role": "Dominant etch equipment for memory and logic advanced nodes",
        "themes": ["semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 83, "confidence": "high",
        "evidence": ["~60% etch market share for memory", "NAND/DRAM process critical path"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "KLAC": {
        "company_name": "KLA Corporation", "country": "US", "exchange": "NASDAQ",
        "role": "Process control and yield management — no viable alternative at 3nm/2nm",
        "themes": ["semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 85, "confidence": "high",
        "evidence": ["Dominant process control at advanced nodes", "~50% inspection market share"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "ONTO": {
        "company_name": "Onto Innovation", "country": "US", "exchange": "NYSE",
        "role": "Metrology for advanced packaging — HBM and chiplet inspection",
        "themes": ["advanced_packaging_test", "semicap_supply_chain"],
        "layer": 4, "bottleneck_score": 77, "confidence": "high",
        "evidence": ["Leading metrology for CoWoS/HBM packaging inspection"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "TSM"],
    },
    "FORM": {
        "company_name": "FormFactor", "country": "US", "exchange": "NASDAQ",
        "role": "Probe cards and test sockets — wafer qualification for AI chips",
        "themes": ["advanced_packaging_test"],
        "layer": 4, "bottleneck_score": 73, "confidence": "high",
        "evidence": ["Dominant probe card for NVDA, AMD, Intel advanced chips"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA"],
    },
    "ACLS": {
        "company_name": "Axcelis Technologies", "country": "US", "exchange": "NASDAQ",
        "role": "Ion implant for SiC / wide-bandgap semiconductors — thin market",
        "themes": ["semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 73, "confidence": "high",
        "evidence": ["Dominant ion implantation for SiC power devices", "EV/industrial supply chain"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "MKSI": {
        "company_name": "MKS Instruments", "country": "US", "exchange": "NASDAQ",
        "role": "Gas delivery, pressure management, RF power — inside AMAT/LRCX/KLAC tools",
        "themes": ["semicap_supply_chain"],
        "layer": 4, "bottleneck_score": 72, "confidence": "high",
        "evidence": ["Critical subsystems inside every major semicap tool"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "COHU": {
        "company_name": "Cohu", "country": "US", "exchange": "NASDAQ",
        "role": "Semiconductor test handlers and contactors — packaging test",
        "themes": ["advanced_packaging_test"],
        "layer": 4, "bottleneck_score": 65, "confidence": "medium",
        "evidence": ["Test handlers for memory and power semiconductor qualification"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA"],
    },

    # ─────────────────────────── PHOTONICS / OPTICAL INTERCONNECT ─────────────

    "LITE": {
        "company_name": "Lumentum Holdings", "country": "US", "exchange": "NASDAQ",
        "role": "Dominant CPO and coherent optical modules for AI datacenters",
        "themes": ["photonics_cpo", "ai_infrastructure"],
        "layer": 2, "bottleneck_score": 86, "confidence": "high",
        "evidence": ["Coherent optics to MSFT, GOOGL, META DC networks", "CPO design-in wins"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["MSFT", "GOOGL", "META", "AMZN"],
    },
    "COHR": {
        "company_name": "Coherent Corp", "country": "US", "exchange": "NYSE",
        "role": "High-speed photonic components for AI interconnects and defense",
        "themes": ["photonics_cpo"],
        "layer": 2, "bottleneck_score": 82, "confidence": "high",
        "evidence": ["800G/1.6T transceivers for hyperscaler spine-leaf networks"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["MSFT", "GOOGL", "META"],
    },
    "AAOI": {
        "company_name": "Applied Optoelectronics", "country": "US", "exchange": "NASDAQ",
        "role": "400G/800G datacenter transceivers — concentrated supplier pool",
        "themes": ["photonics_cpo"],
        "layer": 2, "bottleneck_score": 78, "confidence": "high",
        "evidence": ["AWS, Microsoft datacenter transceiver customer"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["AMZN", "MSFT"],
    },
    "CRDO": {
        "company_name": "Credo Technology", "country": "US", "exchange": "NASDAQ",
        "role": "AEC/DSP ICs for AI spine-leaf fabric — GPU interconnect bottleneck",
        "themes": ["photonics_cpo", "ai_infrastructure"],
        "layer": 3, "bottleneck_score": 80, "confidence": "high",
        "evidence": ["Active electrical cables for AI cluster spine-leaf", "AWS design win"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "AMZN"],
    },
    "II": {
        "company_name": "II-VI (legacy name)", "country": "US", "exchange": "NASDAQ",
        "role": "InP substrates — key material for DFB lasers in photonic ICs",
        "themes": ["photonics_cpo"],
        "layer": 3, "bottleneck_score": 74, "confidence": "high",
        "evidence": ["InP wafers required for 400G/800G DFB laser sources"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["MSFT", "GOOGL"],
    },
    "IPGP": {
        "company_name": "IPG Photonics", "country": "US", "exchange": "NASDAQ",
        "role": "High-power fiber laser for industrial machining and defense cutting",
        "themes": ["photonics_cpo", "defense_optics"],
        "layer": 2, "bottleneck_score": 75, "confidence": "high",
        "evidence": [">40% global high-power fiber laser market share"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },

    # ───────────────────────────── AI POWER / GRID ────────────────────────────

    "ETN": {
        "company_name": "Eaton Corporation", "country": "US", "exchange": "NYSE",
        "role": "Power management and distribution for hyperscale data centers",
        "themes": ["grid_transformers", "ai_power_energy"],
        "layer": 2, "bottleneck_score": 82, "confidence": "high",
        "evidence": ["PDUs in all major hyperscaler DCs", "2-3yr transformer backlog"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["MSFT", "GOOGL", "META", "AMZN"],
    },
    "VRT": {
        "company_name": "Vertiv Holdings", "country": "US", "exchange": "NYSE",
        "role": "Cooling and UPS for hyperscale AI GPU racks",
        "themes": ["ai_power_energy"],
        "layer": 2, "bottleneck_score": 82, "confidence": "high",
        "evidence": ["Direct liquid cooling solutions for Blackwell GPU racks", "MSFT/META design wins"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["MSFT", "GOOGL", "META", "AMZN", "xAI"],
    },
    "GEV": {
        "company_name": "GE Vernova", "country": "US", "exchange": "NYSE",
        "role": "Gas turbines for off-grid AI datacenter power — limited competition",
        "themes": ["ai_power_energy"],
        "layer": 1, "bottleneck_score": 82, "confidence": "high",
        "evidence": ["Only scaled US gas turbine supplier", "3-year+ backlog for large turbines"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["Hyperscalers_AI_Capex", "AI_Power_Buildout"],
    },
    "PWR": {
        "company_name": "Quanta Services", "country": "US", "exchange": "NYSE",
        "role": "Electrical construction and installation for grid and datacenter buildout",
        "themes": ["grid_transformers", "ai_power_energy"],
        "layer": 2, "bottleneck_score": 73, "confidence": "high",
        "evidence": ["Primary electrical contractor for hyperscaler DCs and utility grid"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["AI_Power_Buildout"],
    },
    "HUBB": {
        "company_name": "Hubbell", "country": "US", "exchange": "NYSE",
        "role": "Electrical grid components — connectors, breakers, switchgear",
        "themes": ["grid_transformers"],
        "layer": 3, "bottleneck_score": 72, "confidence": "high",
        "evidence": ["Specialty connectors for grid upgrade buildout"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["AI_Power_Buildout"],
    },
    "NVT": {
        "company_name": "nVent Electric", "country": "US", "exchange": "NYSE",
        "role": "Data center enclosures and power distribution infrastructure",
        "themes": ["grid_transformers", "ai_power_energy"],
        "layer": 3, "bottleneck_score": 73, "confidence": "high",
        "evidence": ["Thermal management and DC power solutions for GPU racks"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["Hyperscalers_AI_Capex"],
    },
    "ATKR": {
        "company_name": "Atkore", "country": "US", "exchange": "NYSE",
        "role": "Electrical conduit and cable management for grid buildout",
        "themes": ["grid_transformers"],
        "layer": 4, "bottleneck_score": 68, "confidence": "high",
        "evidence": ["Conduit systems for datacenter and solar farm electrical"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["AI_Power_Buildout"],
    },

    # ────────────────────────────── DEFENSE ───────────────────────────────────

    "KTOS": {
        "company_name": "Kratos Defense", "country": "US", "exchange": "NASDAQ",
        "role": "Drone and directed energy systems — sole-source USAF programs",
        "themes": ["defense_optics"],
        "layer": 2, "bottleneck_score": 75, "confidence": "high",
        "evidence": ["ALTIUS/Valkyrie UAV programs — sole-source contracts"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "AVAV": {
        "company_name": "AeroVironment", "country": "US", "exchange": "NASDAQ",
        "role": "Tactical UAV for US Army — Switchblade loitering munitions",
        "themes": ["defense_optics"],
        "layer": 2, "bottleneck_score": 72, "confidence": "high",
        "evidence": ["Switchblade sole-source Army program", "JUMP 20 logistics UAV"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "LHX": {
        "company_name": "L3Harris Technologies", "country": "US", "exchange": "NYSE",
        "role": "Electro-optical ISR systems and electronic warfare platforms",
        "themes": ["defense_optics"],
        "layer": 1, "bottleneck_score": 70, "confidence": "high",
        "evidence": ["F-35 EO/IR targeting systems", "SATCOM terminals"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "DRS": {
        "company_name": "Leonardo DRS", "country": "US", "exchange": "NASDAQ",
        "role": "Thermal imaging and EO sensors for US defense",
        "themes": ["defense_optics"],
        "layer": 2, "bottleneck_score": 73, "confidence": "high",
        "evidence": ["FLIR-like thermal sensors for ground vehicle and drone programs"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },

    # ────────────────────────────── MEMORY ────────────────────────────────────

    "WDC": {
        "company_name": "Western Digital", "country": "US", "exchange": "NASDAQ",
        "role": "NAND flash storage — cloud and device supply chain",
        "themes": ["memory"],
        "layer": 2, "bottleneck_score": 68, "confidence": "high",
        "evidence": ["Major NAND supplier to cloud storage"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["AMZN", "MSFT", "GOOGL"],
    },
    "RMBS": {
        "company_name": "Rambus", "country": "US", "exchange": "NASDAQ",
        "role": "Memory interface chips and PHY IP for high-speed DDR/HBM — proprietary",
        "themes": ["memory"],
        "layer": 3, "bottleneck_score": 70, "confidence": "high",
        "evidence": ["DDR5 controller/PHY IP licensed to all major memory fabs"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA"],
    },

    # ────────────────────────────── SPACE ─────────────────────────────────────

    "RKLB": {
        "company_name": "Rocket Lab USA", "country": "US", "exchange": "NASDAQ",
        "role": "Small satellite launch — limited global launch providers",
        "themes": ["space"],
        "layer": 1, "bottleneck_score": 75, "confidence": "high",
        "evidence": ["Electron rocket for smallsat dedicated launch — growing market"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "ASTS": {
        "company_name": "AST SpaceMobile", "country": "US", "exchange": "NASDAQ",
        "role": "Direct-to-cell satellite broadband — novel spectrum / space play",
        "themes": ["space"],
        "layer": 2, "bottleneck_score": 68, "confidence": "medium",
        "evidence": ["BlueBird satellite broadband direct to smartphones"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "PL": {
        "company_name": "Planet Labs", "country": "US", "exchange": "NYSE",
        "role": "Daily satellite imagery — geospatial intelligence at scale",
        "themes": ["space"],
        "layer": 2, "bottleneck_score": 65, "confidence": "medium",
        "evidence": ["Largest constellation for daily global Earth observation"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },

    # ───────────────────── FOREIGN — JAPAN ────────────────────────────────────

    "6857.T": {
        "company_name": "Advantest", "country": "JP", "exchange": "TSE",
        "role": "AI chip test equipment — T2000 SoC tester for HBM and NVDA GPUs",
        "themes": ["advanced_packaging_test", "semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 88, "confidence": "high",
        "evidence": ["T2000 used for NVDA H100/H200 final test", "Only scaled HBM SoC tester"],
        "us_access_proxy": "ATEYY", "adr_ticker": "ATEYY",
        "giant_anchors": ["NVDA"],
    },
    "4063.T": {
        "company_name": "Shin-Etsu Chemical", "country": "JP", "exchange": "TSE",
        "role": "Silicon wafer monopoly + EUV photoresist — critical materials chokepoint",
        "themes": ["semicap_supply_chain"],
        "layer": 4, "bottleneck_score": 92, "confidence": "high",
        "evidence": ["~30% global silicon wafer market share", "EUV photoresist leader"],
        "us_access_proxy": "SHECY", "adr_ticker": "SHECY",
        "giant_anchors": ["TSM", "ASML"],
    },
    "4523.T": {
        "company_name": "Eisai Co.", "country": "JP", "exchange": "TSE",
        "role": "Alzheimer's biologics — FDA-approved Leqembi with Biogen",
        "themes": ["biotech_catalyst"],
        "layer": 2, "bottleneck_score": 72, "confidence": "medium",
        "evidence": ["FDA accelerated approval for Leqembi (lecanemab)"],
        "us_access_proxy": "ESALY", "adr_ticker": "ESALY",
        "giant_anchors": [],
    },
    "6723.T": {
        "company_name": "Renesas Electronics", "country": "JP", "exchange": "TSE",
        "role": "Automotive MCU >40% global share + embedded AI processors",
        "themes": ["semicap_supply_chain"],
        "layer": 2, "bottleneck_score": 78, "confidence": "medium",
        "evidence": ["#1 automotive MCU globally after IDT/Intersil acquisitions"],
        "us_access_proxy": "RNECY", "adr_ticker": "RNECY",
        "giant_anchors": [],
    },
    "6506.T": {
        "company_name": "Yaskawa Electric", "country": "JP", "exchange": "TSE",
        "role": "Industrial robotics and motion control — semiconductor fab automation",
        "themes": ["industrial_onshoring"],
        "layer": 3, "bottleneck_score": 76, "confidence": "medium",
        "evidence": ["AC servo drives and robot controllers for semiconductor fabs and EV lines"],
        "us_access_proxy": "YASKY", "adr_ticker": "YASKY",
        "giant_anchors": ["TSM"],
    },

    # ───────────────────── FOREIGN — SOUTH KOREA ──────────────────────────────

    "000660.KS": {
        "company_name": "SK Hynix", "country": "KR", "exchange": "KSE",
        "role": "HBM3/HBM3E memory — sole HBM supplier for NVDA H100; most advanced HBM",
        "themes": ["memory", "ai_infrastructure"],
        "layer": 2, "bottleneck_score": 94, "confidence": "high",
        "evidence": ["Sole HBM supplier for NVDA H100 launch", "HBM3E yield leader"],
        "us_access_proxy": "HXSCL", "adr_ticker": None,
        "giant_anchors": ["NVDA"],
    },
    "005930.KS": {
        "company_name": "Samsung Electronics", "country": "KR", "exchange": "KSE",
        "role": "2nd largest foundry + HBM3E ramp for NVDA B200 + NAND",
        "themes": ["semicap_supply_chain", "memory", "advanced_packaging_test"],
        "layer": 1, "bottleneck_score": 90, "confidence": "high",
        "evidence": ["HBM3E ramp for NVDA Blackwell", "2nd largest foundry globally"],
        "us_access_proxy": "SSNLF", "adr_ticker": None,
        "giant_anchors": ["NVDA", "GOOGL"],
    },
    "009150.KS": {
        "company_name": "Samsung Electro-Mechanics", "country": "KR", "exchange": "KSE",
        "role": "MLCC capacitors and FC-BGA substrates for AI chip packaging",
        "themes": ["advanced_packaging_test"],
        "layer": 3, "bottleneck_score": 80, "confidence": "high",
        "evidence": ["Dominant MLCC for 5G/AI", "FC-BGA substrate for server CPUs"],
        "us_access_proxy": "SEMCY", "adr_ticker": "SEMCY",
        "giant_anchors": ["NVDA", "AVGO"],
    },

    # ───────────────────── FOREIGN — NETHERLANDS ──────────────────────────────

    "BESI.AS": {
        "company_name": "BE Semiconductor (Besi)", "country": "NL", "exchange": "AMS",
        "role": "Hybrid bonding and die attach — sole tool supplier for 3D chip stacking",
        "themes": ["advanced_packaging_test"],
        "layer": 3, "bottleneck_score": 86, "confidence": "high",
        "evidence": ["Only scaled hybrid bonding tool maker", "Required for chiplet 3D integration"],
        "us_access_proxy": "BESIY", "adr_ticker": "BESIY",
        "giant_anchors": ["TSM", "NVDA"],
    },

    # ───────────────────── FOREIGN — GERMANY ──────────────────────────────────

    "AIXA.DE": {
        "company_name": "AIXTRON", "country": "DE", "exchange": "FRA",
        "role": "MOCVD reactors for GaN/SiC/InP compound semiconductor deposition",
        "themes": ["semicap_supply_chain", "defense_optics"],
        "layer": 4, "bottleneck_score": 83, "confidence": "high",
        "evidence": ["Only scaled MOCVD reactor for GaN power devices and photonic ICs"],
        "us_access_proxy": "AIXG", "adr_ticker": "AIXG",
        "giant_anchors": [],
    },

    # ───────────────────── FOREIGN — TAIWAN (beyond TSM) ──────────────────────

    "ASX": {
        "company_name": "Advanced Semiconductor Engineering", "country": "TW", "exchange": "NYSE",
        "role": "Largest OSAT — CoW, SiP, 2.5D interposer; Apple Watch/AirPods packaging",
        "themes": ["advanced_packaging_test"],
        "layer": 2, "bottleneck_score": 80, "confidence": "high",
        "evidence": ["Largest OSAT by revenue", "SiP for Apple and AI custom silicon"],
        "us_access_proxy": "ASX", "adr_ticker": None,
        "giant_anchors": ["AVGO", "NVDA"],
    },
    "HIMX": {
        "company_name": "Himax Technologies", "country": "TW", "exchange": "NASDAQ",
        "role": "Display driver ICs for AR/VR and automotive — concentrated niche",
        "themes": ["ai_infrastructure"],
        "layer": 3, "bottleneck_score": 65, "confidence": "medium",
        "evidence": ["#1 display driver IC for LCD panels", "Growing AR/VR exposure"],
        "us_access_proxy": "HIMX", "adr_ticker": None,
        "giant_anchors": [],
    },

    # ───────────────────── FRANCE ─────────────────────────────────────────────

    "STM": {
        "company_name": "STMicroelectronics", "country": "FR", "exchange": "NYSE",
        "role": "SiC power devices for EV and industrial — European supply chain anchor",
        "themes": ["energy_transition", "semicap_supply_chain"],
        "layer": 2, "bottleneck_score": 75, "confidence": "high",
        "evidence": ["#2 SiC MOSFET globally", "Tesla, BYD, Stellantis customer"],
        "us_access_proxy": "STM", "adr_ticker": None,
        "giant_anchors": [],
    },

    # ──────────────────── NEW PHASE 4 — US ADDITIONS ──────────────────────────

    "MRVL": {
        "company_name": "Marvell Technology", "country": "US", "exchange": "NASDAQ",
        "role": "Custom AI ASIC and data center networking silicon — Inphi CPO DSP, hyperscaler XPU programs",
        "themes": ["ai_infrastructure", "photonics_cpo", "advanced_packaging_test"],
        "layer": 2, "bottleneck_score": 80, "confidence": "high",
        "evidence": ["Custom XPU design wins at Google, Amazon, Microsoft", "InPhi CPO optical DSP leader"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "GOOGL", "AMZN", "MSFT", "AVGO"],
    },
    "SMCI": {
        "company_name": "Super Micro Computer", "country": "US", "exchange": "NASDAQ",
        "role": "AI GPU server systems integrator — direct-liquid cooling leadership; sells into all hyperscalers",
        "themes": ["ai_infrastructure", "ai_power_energy", "cooling_thermal"],
        "layer": 1, "bottleneck_score": 72, "confidence": "high",
        "evidence": ["Direct liquid cooling (DLC) servers for NVDA GB200 NVL72", "AI rack systems leader"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "MSFT", "GOOGL", "META", "AMZN", "CoreWeave_Neocloud"],
    },
    "TER": {
        "company_name": "Teradyne", "country": "US", "exchange": "NASDAQ",
        "role": "Semiconductor test equipment — ATE for advanced logic, HBM memory, and SoC chips",
        "themes": ["advanced_packaging_test", "semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 80, "confidence": "high",
        "evidence": ["50%+ global SoC test market share", "HBM test ramp as SK Hynix/Samsung scale"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM", "NVDA"],
    },
    "PLAB": {
        "company_name": "Photronics", "country": "US", "exchange": "NASDAQ",
        "role": "Photomask manufacturer for leading-edge logic and memory — TSMC, Samsung, Intel customer",
        "themes": ["semicap_supply_chain"],
        "layer": 4, "bottleneck_score": 76, "confidence": "high",
        "evidence": ["Largest independent photomask maker globally", "Critical for every advanced node tape-out"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "UCTT": {
        "company_name": "Ultra Clean Holdings", "country": "US", "exchange": "NASDAQ",
        "role": "Gas delivery subsystems and precision cleaning for AMAT, Lam Research, and KLAC tools",
        "themes": ["semicap_supply_chain"],
        "layer": 4, "bottleneck_score": 73, "confidence": "high",
        "evidence": ["Largest gas panel supplier to AMAT/Lam", "Critical process supply chain for etch/dep tools"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "KLIC": {
        "company_name": "Kulicke & Soffa", "country": "US", "exchange": "NASDAQ",
        "role": "Wire bonders and flip-chip tools for advanced IC packaging — hybrid bonding transition",
        "themes": ["advanced_packaging_test"],
        "layer": 3, "bottleneck_score": 72, "confidence": "high",
        "evidence": ["#1 wire bonder globally", "Flip chip tools for OSAT customers growing"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "WOLF": {
        "company_name": "Wolfspeed", "country": "US", "exchange": "NYSE",
        "role": "Silicon carbide (SiC) substrates and power devices — EV drivetrain and datacenter power supply",
        "themes": ["soi_substrates_materials", "energy_transition", "ai_power_energy"],
        "layer": 3, "bottleneck_score": 74, "confidence": "medium",
        "evidence": ["Only US-based vertically integrated SiC substrate maker", "EV drivetrain key supplier"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },
    "CDNS": {
        "company_name": "Cadence Design Systems", "country": "US", "exchange": "NASDAQ",
        "role": "EDA software for chip design — required for every advanced ASIC, FPGA, and SoC tape-out",
        "themes": ["semicap_supply_chain", "ai_infrastructure"],
        "layer": 4, "bottleneck_score": 83, "confidence": "high",
        "evidence": ["Duopoly with Synopsys in EDA — no viable alternative", "Every AI chip tapes out using Cadence or Synopsys"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "AVGO", "TSM"],
    },
    "SNPS": {
        "company_name": "Synopsys", "country": "US", "exchange": "NASDAQ",
        "role": "EDA software + semiconductor IP — duopoly with Cadence; silicon lifecycle tools",
        "themes": ["semicap_supply_chain", "ai_infrastructure"],
        "layer": 4, "bottleneck_score": 83, "confidence": "high",
        "evidence": ["Duopoly EDA market with Cadence", "AI chip design flow essential dependency"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["NVDA", "AVGO", "TSM"],
    },
    "BE": {
        "company_name": "Bloom Energy", "country": "US", "exchange": "NYSE",
        "role": "Solid oxide fuel cell power for AI datacenter — behind-the-meter generation, bypasses utility grid",
        "themes": ["ai_power_energy"],
        "layer": 2, "bottleneck_score": 68, "confidence": "medium",
        "evidence": ["SK Telecom, Microsoft datacenter fuel cell deployments", "Bypasses grid constraint for AI compute power"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["MSFT", "xAI"],
    },
    "MTZ": {
        "company_name": "MasTec", "country": "US", "exchange": "NYSE",
        "role": "Electrical infrastructure EPC contractor — transmission lines, substation build for AI datacenter",
        "themes": ["grid_transformers", "ai_power_energy"],
        "layer": 2, "bottleneck_score": 66, "confidence": "medium",
        "evidence": ["#1 US transmission line contractor", "Datacenter power delivery capacity constrained"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": ["AI_Power_Buildout"],
    },
    "HWM": {
        "company_name": "Howmet Aerospace", "country": "US", "exchange": "NYSE",
        "role": "Titanium structural castings and jet engine components — defense and aerospace sole-source chokepoint",
        "themes": ["defense_optics"],
        "layer": 2, "bottleneck_score": 75, "confidence": "high",
        "evidence": ["Sole qualified supplier for several USAF castings", "Titanium melt shop bottleneck post-Russia"],
        "us_access_proxy": None, "adr_ticker": None,
        "giant_anchors": [],
    },

    # ──────────────── NEW PHASE 4 — JAPAN FOREIGN ADDITIONS ──────────────────

    "8035.T": {
        "company_name": "Tokyo Electron (TEL)", "country": "JP", "exchange": "TSE",
        "role": "Japan's leading semicap equipment maker — etch, CVD, cleaning, coater/developer; #3 globally",
        "themes": ["semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 88, "confidence": "high",
        "evidence": ["#3 semicap equipment globally after AMAT and Lam", "TSMC and Samsung critical supplier for etch/CVD"],
        "us_access_proxy": "TOELY", "adr_ticker": "TOELY",
        "giant_anchors": ["TSM", "NVDA"],
    },
    "6981.T": {
        "company_name": "Murata Manufacturing", "country": "JP", "exchange": "TSE",
        "role": "MLCC capacitors (~40% global share) and RF modules for 5G and AI server boards",
        "themes": ["advanced_packaging_test", "semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 82, "confidence": "high",
        "evidence": ["~40% global MLCC market share — every AI server PCB uses Murata MLCCs", "RF front-end leader for 5G"],
        "us_access_proxy": "MRAAY", "adr_ticker": "MRAAY",
        "giant_anchors": ["NVDA", "MSFT"],
    },
    "3436.T": {
        "company_name": "Sumco Corporation", "country": "JP", "exchange": "TSE",
        "role": "Silicon wafer production — 2nd largest globally; 300mm polished wafers for logic/memory",
        "themes": ["semicap_supply_chain", "soi_substrates_materials"],
        "layer": 4, "bottleneck_score": 80, "confidence": "high",
        "evidence": ["#2 silicon wafer globally after Shin-Etsu", "300mm capacity constrained vs AI fab expansion"],
        "us_access_proxy": "SUMCF", "adr_ticker": None,
        "giant_anchors": ["TSM"],
    },
    "6146.T": {
        "company_name": "Disco Corporation", "country": "JP", "exchange": "TSE",
        "role": "Wafer dicing saws and grinding wheels — near-monopoly for die singulation before packaging",
        "themes": ["advanced_packaging_test", "semicap_supply_chain"],
        "layer": 4, "bottleneck_score": 84, "confidence": "high",
        "evidence": ["~70-80% global share in wafer dicing saws", "Every OSAT globally uses Disco equipment"],
        "us_access_proxy": "DISCF", "adr_ticker": None,
        "giant_anchors": ["TSM", "NVDA"],
    },
    "4901.T": {
        "company_name": "Fujifilm Holdings", "country": "JP", "exchange": "TSE",
        "role": "EUV photoresist materials and semiconductor process chemicals — TSMC and Samsung approved supplier",
        "themes": ["semicap_supply_chain", "soi_substrates_materials"],
        "layer": 4, "bottleneck_score": 79, "confidence": "high",
        "evidence": ["EUV photoresist qualification at TSMC and Samsung", "Critical materials for 3nm/2nm nodes"],
        "us_access_proxy": "FUJIY", "adr_ticker": "FUJIY",
        "giant_anchors": ["TSM", "ASML"],
    },
    "7735.T": {
        "company_name": "SCREEN Holdings", "country": "JP", "exchange": "TSE",
        "role": "Wafer cleaning and single-wafer processing equipment — key wet etch and clean step for advanced nodes",
        "themes": ["semicap_supply_chain"],
        "layer": 3, "bottleneck_score": 76, "confidence": "high",
        "evidence": ["Leading wafer scrubber/cleaner globally", "TSMC and Samsung fab cleaning tools"],
        "us_access_proxy": "DINRY", "adr_ticker": "DINRY",
        "giant_anchors": ["TSM"],
    },

    # ──────────────── NEW PHASE 4 — GERMANY FOREIGN ADDITIONS ────────────────

    "IFNNY": {
        "company_name": "Infineon Technologies", "country": "DE", "exchange": "OTCMKTS",
        "role": "SiC and GaN power devices for EV and industrial — #1 automotive power semiconductor globally",
        "themes": ["energy_transition", "semicap_supply_chain", "ai_power_energy"],
        "layer": 2, "bottleneck_score": 77, "confidence": "high",
        "evidence": ["#1 global automotive power semiconductor", "SiC for Tesla, BYD, and tier-1 EV OEMs"],
        "us_access_proxy": "IFNNY", "adr_ticker": "IFNNY",
        "giant_anchors": [],
    },

    # ──────────────── NEW PHASE 4 — UK FOREIGN ADDITIONS ─────────────────────

    "IQE.L": {
        "company_name": "IQE plc", "country": "GB", "exchange": "LSE",
        "role": "Compound semiconductor epitaxial wafers (GaAs, InP, GaN) — sole/dual source for 5G RF and VCSEL",
        "themes": ["photonics_cpo", "semicap_supply_chain", "defense_optics"],
        "layer": 4, "bottleneck_score": 78, "confidence": "medium",
        "evidence": ["Sole or dual-source epi wafer for Apple Face ID VCSEL chips", "GaN on SiC for 5G RF front-end"],
        "us_access_proxy": "IQEPY", "adr_ticker": "IQEPY",
        "giant_anchors": [],
    },
}


def _canon_ticker(ticker: str, node: Dict[str, Any]) -> str:
    """Return the canonical ticker for a node (ADR if available for foreign, else native)."""
    if node.get("us_access_proxy"):
        return node["us_access_proxy"]
    return ticker


def get_node(ticker: str) -> Optional[Dict[str, Any]]:
    """Look up a node by ticker (exact match or uppercase)."""
    return NODE_REGISTRY.get(ticker) or NODE_REGISTRY.get(ticker.upper())


def get_chain_for_theme(
    theme_id: str,
    max_depth: int = 4,
    include_foreign: bool = False,
    country_filters: Optional[List[str]] = None,
) -> List[ChainLayer]:
    """
    Return layered chain nodes for a given theme.
    Filters NODE_REGISTRY by theme membership.
    """
    layers: Dict[int, List[ChainNode]] = {i: [] for i in range(max_depth + 1)}
    seen: Set[str] = set()

    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        depth = node.get("layer", 2)
        if depth > max_depth:
            continue
        if theme_id not in node.get("themes", []):
            continue

        country = node.get("country", "US")
        if not include_foreign and country != "US":
            continue
        if country_filters and country not in country_filters:
            continue

        canon = _canon_ticker(ticker, node)
        if canon in seen:
            continue
        seen.add(canon)

        layers[depth].append(ChainNode(
            ticker=canon,
            company_name=node["company_name"],
            country=country,
            exchange=node.get("exchange", ""),
            layer=depth,
            layer_label=LAYER_LABELS.get(depth, ""),
            themes=node.get("themes", []),
            role=node.get("role", ""),
            bottleneck_score=float(node.get("bottleneck_score", 50)),
            confidence=node.get("confidence", "medium"),
            evidence=node.get("evidence", []),
            us_access_proxy=node.get("us_access_proxy"),
            adr_ticker=node.get("adr_ticker"),
        ))

    result: List[ChainLayer] = []
    for i in range(max_depth + 1):
        nodes_sorted = sorted(layers[i], key=lambda n: n.bottleneck_score, reverse=True)
        if nodes_sorted:
            result.append(ChainLayer(
                layer_index=i,
                label=LAYER_LABELS.get(i, f"Layer {i}"),
                description=LAYER_DESCRIPTIONS.get(i, ""),
                nodes=nodes_sorted,
            ))
    return result


def get_chain_for_giant(
    giant_id: str,
    max_depth: int = 4,
    themes_filter: Optional[List[str]] = None,
    include_foreign: bool = False,
    country_filters: Optional[List[str]] = None,
) -> List[ChainLayer]:
    """
    Return supply chain layers for a given giant anchor.
    Filters by themes that match the giant's relevant themes.
    """
    from services.playbook.giant_map import GIANT_MAP

    giant_key = giant_id.upper()
    giant = None
    for k, v in GIANT_MAP.items():
        if k.upper() == giant_key:
            giant = v
            break

    relevant_themes: Set[str] = set(giant.get("themes", [])) if giant else set()
    if themes_filter:
        if relevant_themes:
            relevant_themes = relevant_themes & set(themes_filter)
        else:
            relevant_themes = set(themes_filter)

    layers: Dict[int, List[ChainNode]] = {i: [] for i in range(max_depth + 1)}
    seen: Set[str] = set()

    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        depth = node.get("layer", 2)
        if depth > max_depth:
            continue

        country = node.get("country", "US")
        if not include_foreign and country != "US":
            continue
        if country_filters and country not in country_filters:
            continue

        node_themes: Set[str] = set(node.get("themes", []))
        if relevant_themes and not (node_themes & relevant_themes):
            continue

        canon = _canon_ticker(ticker, node)
        if canon in seen:
            continue
        seen.add(canon)

        layers[depth].append(ChainNode(
            ticker=canon,
            company_name=node["company_name"],
            country=country,
            exchange=node.get("exchange", ""),
            layer=depth,
            layer_label=LAYER_LABELS.get(depth, ""),
            themes=list(node_themes),
            role=node.get("role", ""),
            bottleneck_score=float(node.get("bottleneck_score", 50)),
            confidence=node.get("confidence", "medium"),
            evidence=node.get("evidence", []),
            us_access_proxy=node.get("us_access_proxy"),
            adr_ticker=node.get("adr_ticker"),
        ))

    result: List[ChainLayer] = []
    for i in range(max_depth + 1):
        nodes_sorted = sorted(layers[i], key=lambda n: n.bottleneck_score, reverse=True)
        if nodes_sorted:
            result.append(ChainLayer(
                layer_index=i,
                label=LAYER_LABELS.get(i, f"Layer {i}"),
                description=LAYER_DESCRIPTIONS.get(i, ""),
                nodes=nodes_sorted,
            ))
    return result


def get_all_tickers_for_themes(
    theme_ids: List[str],
    include_foreign: bool = False,
    country_filters: Optional[List[str]] = None,
) -> List[str]:
    """Return unique canonical ticker list for a set of themes."""
    result: List[str] = []
    seen: Set[str] = set()
    theme_set = set(theme_ids)

    for ticker, node in NODE_REGISTRY.items():
        if node is None:
            continue
        country = node.get("country", "US")
        if not include_foreign and country != "US":
            continue
        if country_filters and country not in country_filters:
            continue
        node_themes = set(node.get("themes", []))
        if not (node_themes & theme_set):
            continue
        canon = _canon_ticker(ticker, node)
        if canon not in seen:
            seen.add(canon)
            result.append(canon)
    return result
