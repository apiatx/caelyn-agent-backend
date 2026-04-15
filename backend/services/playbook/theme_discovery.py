"""
Theme Discovery — extended Serenity theme taxonomy with rich discovery metadata.

Each theme entry includes:
  - label / description
  - giant_anchors: platform companies driving this theme
  - chain_layers: named supply chain layers for discovery framing
  - keyword_rules: phrases for description/news matching
  - example_companies: well-known names in this theme
  - preferred_countries: countries with significant supply chains
  - policy_linkage: US/EU legislation relevant to this theme
  - serenity_priority: how central this theme is to Serenity strategy

Themes are a superset of theme_map.py ALL_THEMES — this module is discovery-only.
It does NOT change the scoring engine.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

THEME_TAXONOMY: Dict[str, Dict[str, Any]] = {

    "photonics_cpo": {
        "label":        "Photonics & Co-Packaged Optics",
        "description":  "Silicon photonics, co-packaged optics (CPO), and high-speed coherent interconnects for AI datacenter spine-leaf networks. Bottleneck: few scaled transceiver/CPO suppliers.",
        "giant_anchors": ["NVDA", "GOOGL", "META", "MSFT", "AMZN", "AVGO"],
        "chain_layers":  ["gpu_to_switch_interconnect", "coherent_optical_modules",
                          "transceiver_ics", "inp_substrates", "fiber_laser_sources"],
        "keyword_rules": ["co-packaged optics", "silicon photonics", "cpo", "coherent optical",
                          "transceiver", "vcsel", "optical interconnect", "datacom transceiver",
                          "photonic integrated circuit", "800g", "1.6t", "dfb laser"],
        "example_companies": ["LITE", "COHR", "AAOI", "CRDO", "II", "IPGP"],
        "preferred_countries": ["US", "TW", "JP"],
        "policy_linkage": "CHIPS Act indirectly supports domestic photonic IC capacity",
        "serenity_priority": "high",
    },

    "ai_power_energy": {
        "label":        "AI Power & Energy Infrastructure",
        "description":  "Data center power demand surge from AI GPU clusters. Includes UPS, cooling, gas turbines, and on-site generation. Constraint: 2-3yr lead times for large power equipment.",
        "giant_anchors": ["MSFT", "GOOGL", "META", "AMZN", "xAI", "Hyperscalers_AI_Capex", "AI_Power_Buildout"],
        "chain_layers":  ["on_site_power_generation", "ups_and_pdu", "liquid_cooling",
                          "thermal_management", "power_conversion"],
        "keyword_rules": ["data center power", "ai power", "hyperscale power",
                          "power infrastructure", "thermal management", "cooling solution",
                          "uninterruptible power", "liquid cooling", "direct liquid cooling",
                          "power density", "gpu rack power"],
        "example_companies": ["VRT", "GEV", "ETN", "NVT", "SMCI"],
        "preferred_countries": ["US", "DE", "JP"],
        "policy_linkage": "IRA clean power credits; nuclear small modular reactor incentives",
        "serenity_priority": "high",
    },

    "grid_transformers": {
        "label":        "Grid Transformers & Switchgear",
        "description":  "Power grid upgrade needed for AI load growth. Transformer lead times are 2-3 years; domestic manufacturing capacity is extremely tight.",
        "giant_anchors": ["AI_Power_Buildout", "Hyperscalers_AI_Capex"],
        "chain_layers":  ["high_voltage_transformers", "distribution_switchgear",
                          "substation_equipment", "electrical_construction"],
        "keyword_rules": ["transformer", "switchgear", "grid infrastructure",
                          "electrical distribution", "power distribution", "substation",
                          "utility-scale", "high-voltage", "grid buildout", "transmission"],
        "example_companies": ["ETN", "HUBB", "PWR", "NVT", "ATKR"],
        "preferred_countries": ["US", "DE", "FR"],
        "policy_linkage": "Infrastructure Investment and Jobs Act ($73B grid investment)",
        "serenity_priority": "high",
    },

    "advanced_packaging_test": {
        "label":        "Advanced Packaging & Test",
        "description":  "2.5D/3D chip stacking, chiplets, HBM packaging, CoWoS — the bottleneck layer between GPU design and physical chip output. Only a handful of suppliers globally.",
        "giant_anchors": ["NVDA", "AVGO", "GOOGL", "META", "TSM"],
        "chain_layers":  ["osat_packaging", "hybrid_bonding", "substrate_fc_bga",
                          "test_equipment", "metrology_inspection"],
        "keyword_rules": ["advanced packaging", "chiplet", "2.5d", "3d ic",
                          "hbm packaging", "test socket", "burn-in", "cowos",
                          "wafer level packaging", "flip chip", "substrate", "hybrid bonding",
                          "sip packaging", "foplp"],
        "example_companies": ["AMKR", "ASX", "ONTO", "FORM", "COHU", "BESI.AS", "009150.KS"],
        "preferred_countries": ["US", "TW", "KR", "NL", "JP"],
        "policy_linkage": "CHIPS Act domestic packaging subsidies ($11B+ for R&D)",
        "serenity_priority": "high",
    },

    "semicap_supply_chain": {
        "label":        "Semiconductor Capital Equipment",
        "description":  "Tools and materials required to manufacture chips — etch, deposition, lithography, process control. Extremely concentrated market with high barriers to entry.",
        "giant_anchors": ["TSM", "NVDA", "AVGO"],
        "chain_layers":  ["lithography", "etch_deposition", "process_control",
                          "ion_implantation", "advanced_materials", "gas_delivery"],
        "keyword_rules": ["semiconductor equipment", "etch", "deposition",
                          "lithography", "wafer fabrication", "process control",
                          "ion implant", "epitaxial", "cmp slurry", "mocvd",
                          "euv", "extreme ultraviolet", "photoresist"],
        "example_companies": ["ASML", "AMAT", "LRCX", "KLAC", "ENTG", "ONTO", "FORM", "ACLS", "MKSI"],
        "preferred_countries": ["US", "NL", "JP", "DE"],
        "policy_linkage": "CHIPS Act R&D + export controls on advanced semicap to China",
        "serenity_priority": "high",
    },

    "memory_hbm": {
        "label":        "Memory — HBM / DRAM / NAND",
        "description":  "HBM3/3E is the AI memory layer — stacked on every H100/H200/B200 GPU. Only 3 suppliers globally. SK Hynix currently holds HBM technology lead.",
        "giant_anchors": ["NVDA", "AVGO", "GOOGL"],
        "chain_layers":  ["hbm_memory", "dram_modules", "nand_storage", "memory_interface_ip"],
        "keyword_rules": ["hbm", "dram", "nand", "flash memory",
                          "memory technology", "high bandwidth memory",
                          "memory interface", "storage controller", "hbm3", "hbm3e"],
        "example_companies": ["MU", "WDC", "RMBS", "000660.KS", "005930.KS"],
        "preferred_countries": ["US", "KR", "JP"],
        "policy_linkage": "CHIPS Act funding for US domestic HBM capacity (Micron)",
        "serenity_priority": "medium",
    },

    "soi_substrates_materials": {
        "label":        "SOI Substrates & Advanced Materials",
        "description":  "Silicon-on-Insulator substrates, silicon wafers, specialty gases, and photoresist chemicals. Extremely concentrated market — dominated by 2-3 Japanese and European suppliers.",
        "giant_anchors": ["TSM", "ASML"],
        "chain_layers":  ["silicon_wafers", "soi_substrates", "photoresist",
                          "cmp_slurries", "specialty_gases", "epitaxial_wafers"],
        "keyword_rules": ["silicon wafer", "soi substrate", "photoresist", "cmp slurry",
                          "specialty gas", "epitaxial", "substrate material", "epi wafer"],
        "example_companies": ["ENTG", "4063.T", "MKSI"],
        "preferred_countries": ["US", "JP", "DE"],
        "policy_linkage": "CHIPS Act materials R&D; export controls on specialty chemicals",
        "serenity_priority": "medium",
    },

    "cooling_thermal": {
        "label":        "Cooling & Thermal Management",
        "description":  "Direct liquid cooling for 700W+ GPU racks — immersion cooling, cold plates, precision cooling systems. Bottleneck: engineering expertise and long lead times.",
        "giant_anchors": ["Hyperscalers_AI_Capex", "xAI", "MSFT", "META"],
        "chain_layers":  ["direct_liquid_cooling", "cold_plate_systems",
                          "precision_air_cooling", "immersion_cooling", "coolant_distribution"],
        "keyword_rules": ["liquid cooling", "direct liquid cooling", "immersion cooling",
                          "cold plate", "precision cooling", "thermal management",
                          "data center cooling", "coolant distribution unit", "cdu"],
        "example_companies": ["VRT", "NVT"],
        "preferred_countries": ["US", "DE"],
        "policy_linkage": "Energy efficiency standards for data centers (DOE)",
        "serenity_priority": "medium",
    },

    "neocloud": {
        "label":        "Neocloud / GPU-as-a-Service",
        "description":  "Cloud providers built specifically for AI GPU workloads — CoreWeave, Lambda Labs, and hyperscalers offering GPU-as-a-service. Enables inference scaling for AI labs.",
        "giant_anchors": ["NVDA", "MSFT", "AMZN", "GOOGL"],
        "chain_layers":  ["gpu_cloud_infrastructure", "ai_compute_platform",
                          "inference_serving", "networking"],
        "keyword_rules": ["gpu cloud", "ai compute", "inference cloud", "neocloud",
                          "ai cloud", "cloud gpu", "gpu-as-a-service", "ai inference"],
        "example_companies": ["NET", "SNOW", "DDOG"],
        "preferred_countries": ["US"],
        "policy_linkage": None,
        "serenity_priority": "low",
    },

    "defense_optics": {
        "label":        "Defense Optics & UAV",
        "description":  "Electro-optical/infrared sensors, directed energy, tactical UAV, and counter-drone systems. Growing US DoD budget and allied nations re-arming.",
        "giant_anchors": [],
        "chain_layers":  ["eo_ir_sensor_systems", "tactical_uav", "directed_energy",
                          "counter_drone", "satellite_comms"],
        "keyword_rules": ["defense optics", "electro-optical", "directed energy",
                          "sensor fusion", "lidar", "unmanned aerial", "drone",
                          "counter-uas", "surveillance system", "infrared sensor",
                          "loitering munition", "eoir"],
        "example_companies": ["KTOS", "AVAV", "DRS", "LHX", "IPGP"],
        "preferred_countries": ["US", "UK", "IL"],
        "policy_linkage": "NDAA multi-year UAV procurement; directed energy program of record",
        "serenity_priority": "high",
    },

    "space_sensing": {
        "label":        "Space & Satellite",
        "description":  "Small satellite launch, satellite broadband, Earth observation, and space-based sensing. Growing commercial and defense demand.",
        "giant_anchors": [],
        "chain_layers":  ["launch_services", "satellite_bus", "payload_sensors",
                          "ground_segment", "data_analytics"],
        "keyword_rules": ["satellite", "launch vehicle", "space", "leo",
                          "geostationary", "orbital", "smallsat", "cubesat",
                          "space sensing", "earth observation"],
        "example_companies": ["RKLB", "ASTS", "PL"],
        "preferred_countries": ["US", "UK", "NZ"],
        "policy_linkage": "NDAA space systems procurement; NASA commercial contracts",
        "serenity_priority": "medium",
    },

    "industrial_onshoring": {
        "label":        "Industrial Onshoring & Automation",
        "description":  "US/EU semiconductor and defense manufacturing onshoring — robotics, automation, and precision equipment for new fab construction.",
        "giant_anchors": ["TSM", "AI_Power_Buildout"],
        "chain_layers":  ["factory_automation", "precision_robotics",
                          "cnc_machining", "material_handling"],
        "keyword_rules": ["industrial automation", "robotics", "factory automation",
                          "onshoring", "reshoring", "nearshoring", "semiconductor fab",
                          "precision equipment", "servo motor"],
        "example_companies": ["6506.T"],
        "preferred_countries": ["US", "JP", "DE"],
        "policy_linkage": "CHIPS Act fab construction; IRA domestic manufacturing incentives",
        "serenity_priority": "medium",
    },

    "ai_infrastructure": {
        "label":        "AI Infrastructure (Silicon & Systems)",
        "description":  "Custom AI ASICs, GPU servers, networking silicon, and data center systems powering the AI training and inference stack.",
        "giant_anchors": ["NVDA", "AVBO", "GOOGL", "META"],
        "chain_layers":  ["ai_chip_design", "server_integration", "networking_asic",
                          "inference_systems"],
        "keyword_rules": ["ai chip", "gpu", "ai accelerator", "ai inference",
                          "ai training", "neural processing", "data center",
                          "language model", "llm", "generative ai", "tpu", "asic"],
        "example_companies": ["NVDA", "AMD", "AVBO", "MRVL", "SMCI", "PLTR"],
        "preferred_countries": ["US", "TW"],
        "policy_linkage": "CHIPS Act; export controls on AI accelerators to China",
        "serenity_priority": "medium",
    },

    "energy_transition": {
        "label":        "Energy Transition (Solar/Wind/Storage/SiC)",
        "description":  "Clean energy buildout — solar panels, wind turbines, battery storage, EV charging, and SiC power semiconductors for EV powertrains.",
        "giant_anchors": ["AI_Power_Buildout"],
        "chain_layers":  ["solar_manufacturing", "wind_turbine", "battery_storage",
                          "sic_power_devices", "ev_charging"],
        "keyword_rules": ["solar", "wind", "battery storage", "electrolysis",
                          "fuel cell", "ev charging", "renewable energy",
                          "clean energy", "silicon carbide", "sic mosfet"],
        "example_companies": ["STM", "ENPH", "FSLR", "SEDG"],
        "preferred_countries": ["US", "FR", "CN", "KR"],
        "policy_linkage": "IRA clean energy tax credits; EU Green Deal manufacturing",
        "serenity_priority": "low",
    },
}


def get_theme(theme_id: str) -> Optional[Dict[str, Any]]:
    """Return theme metadata by ID. None if not found."""
    return THEME_TAXONOMY.get(theme_id)


def list_themes() -> List[Dict[str, Any]]:
    """Return all themes with their full metadata for the /themes endpoint."""
    result = []
    for tid, meta in THEME_TAXONOMY.items():
        result.append({
            "id":                 tid,
            "label":              meta["label"],
            "description":        meta["description"],
            "giant_anchors":      meta["giant_anchors"],
            "chain_layers":       meta["chain_layers"],
            "example_companies":  meta["example_companies"],
            "preferred_countries": meta["preferred_countries"],
            "policy_linkage":     meta.get("policy_linkage"),
            "serenity_priority":  meta.get("serenity_priority", "medium"),
        })
    return result


def get_themes_for_giant(giant_id: str) -> List[str]:
    """Return theme IDs relevant to a given giant anchor."""
    from services.playbook.giant_map import GIANT_MAP
    giant = None
    for k, v in GIANT_MAP.items():
        if k.upper() == giant_id.upper():
            giant = v
            break
    if giant is None:
        return []
    return giant.get("themes", [])
