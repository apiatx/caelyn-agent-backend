"""
Giant Platform Anchor Map — maps major platform companies and capex cycles
to their downstream supply chain structure for Serenity discovery.

Each giant defines:
  - name / description
  - themes:            relevant Serenity themes
  - chain_layers:      ordered list of dependency layer names
  - bottleneck_buckets: supply-chain bucket types to target for discovery
  - supplier_classes:  company-type taxonomy (what supplies into this giant)
  - foreign_exposure:  ISO-2 country codes with meaningful participation
  - capex_scale:       context string for the frontend

Supported giant IDs (case-insensitive lookup):
  NVDA, MSFT, GOOGL, META, AMZN, TSM, AVGO, xAI,
  Hyperscalers_AI_Capex, AI_Power_Buildout
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

GIANT_MAP: Dict[str, Dict[str, Any]] = {
    "NVDA": {
        "name":             "NVIDIA Corporation",
        "description":      "AI GPU platform — training and inference for foundation models; H100/H200/B200 cycle",
        "themes":           ["ai_infrastructure", "advanced_packaging_test", "memory", "photonics_cpo", "ai_power_energy", "semicap_supply_chain"],
        "chain_layers":     ["gpu_silicon_design", "leading_node_foundry", "hbm_memory_stacking",
                             "advanced_packaging_osat", "optical_interconnect", "datacenter_power_cooling", "semicap_tooling"],
        "bottleneck_buckets": ["lithography", "hbm_memory", "osat_packaging", "photonics",
                               "datacenter_power", "process_control", "materials"],
        "supplier_classes": ["foundry", "memory", "packaging", "photonics", "power_mgmt",
                             "semicap_equipment", "substrate", "test_socket"],
        "foreign_exposure": ["TW", "KR", "JP", "NL"],
        "capex_scale":      "$40B+ AI datacenter investment cycle annually",
        "anchor_ticker":    "NVDA",
    },
    "MSFT": {
        "name":             "Microsoft Corporation",
        "description":      "Azure AI cloud platform — Copilot, OpenAI integration, custom Maia silicon",
        "themes":           ["neocloud", "ai_infrastructure", "ai_power_energy", "grid_transformers"],
        "chain_layers":     ["ai_cloud_platform", "custom_silicon_design", "datacenter_construction",
                             "power_grid_infrastructure", "optical_networking", "cooling_thermal"],
        "bottleneck_buckets": ["datacenter_power", "grid_transformers", "photonics", "cooling_thermal", "osat_packaging"],
        "supplier_classes": ["power_management", "grid_construction", "cooling", "optical_networking",
                             "custom_asic", "foundry"],
        "foreign_exposure": ["NL", "DE", "JP"],
        "capex_scale":      "$80B+ datacenter buildout planned for 2025",
        "anchor_ticker":    "MSFT",
    },
    "GOOGL": {
        "name":             "Alphabet / Google",
        "description":      "AI-native cloud + TPU v6 custom silicon + DeepMind; largest AI capex by usage",
        "themes":           ["ai_infrastructure", "neocloud", "advanced_packaging_test", "photonics_cpo"],
        "chain_layers":     ["custom_tpu_silicon", "leading_node_foundry", "advanced_packaging_osat",
                             "optical_interconnect", "datacenter_power"],
        "bottleneck_buckets": ["foundry", "osat_packaging", "photonics", "datacenter_power", "process_control"],
        "supplier_classes": ["foundry", "packaging", "photonics", "power_management", "semicap_equipment"],
        "foreign_exposure": ["TW", "KR", "JP", "NL"],
        "capex_scale":      "$75B+ capex 2025",
        "anchor_ticker":    "GOOGL",
    },
    "META": {
        "name":             "Meta Platforms",
        "description":      "Social AI + custom MTIA silicon + AR/VR hardware; aggressive AI infra build",
        "themes":           ["ai_infrastructure", "advanced_packaging_test", "photonics_cpo", "ai_power_energy"],
        "chain_layers":     ["custom_mtia_silicon", "leading_node_foundry", "advanced_packaging",
                             "optical_interconnect", "datacenter_power"],
        "bottleneck_buckets": ["foundry", "osat_packaging", "photonics", "datacenter_power"],
        "supplier_classes": ["foundry", "packaging", "photonics", "power_management"],
        "foreign_exposure": ["TW", "NL", "KR"],
        "capex_scale":      "$60-65B capex 2025",
        "anchor_ticker":    "META",
    },
    "AMZN": {
        "name":             "Amazon Web Services",
        "description":      "Largest cloud platform + Trainium/Inferentia custom AI silicon; AWS dominates cloud",
        "themes":           ["neocloud", "ai_infrastructure", "ai_power_energy", "grid_transformers"],
        "chain_layers":     ["cloud_ai_platform", "custom_trainium_silicon", "datacenter_power",
                             "power_grid_infrastructure", "optical_networking"],
        "bottleneck_buckets": ["datacenter_power", "grid_transformers", "photonics", "foundry"],
        "supplier_classes": ["power_management", "grid_construction", "optical_networking", "foundry"],
        "foreign_exposure": ["TW", "NL", "JP"],
        "capex_scale":      "$104B capex 2025",
        "anchor_ticker":    "AMZN",
    },
    "TSM": {
        "name":             "TSMC",
        "description":      "World's leading foundry — every leading-edge chip flows through TSMC; gateway for all AI silicon",
        "themes":           ["semicap_supply_chain", "advanced_packaging_test"],
        "chain_layers":     ["leading_node_foundry", "semicap_tooling", "advanced_materials",
                             "process_control", "osat_packaging"],
        "bottleneck_buckets": ["lithography", "process_control", "materials", "etch", "osat_packaging"],
        "supplier_classes": ["lithography", "etch_deposition", "process_control", "materials_cmp",
                             "packaging", "substrate"],
        "foreign_exposure": ["NL", "JP", "KR", "DE"],
        "capex_scale":      "$38B capex 2025; CoWoS capacity gating factor for AI",
        "anchor_ticker":    "TSM",
    },
    "AVGO": {
        "name":             "Broadcom",
        "description":      "Custom AI ASIC + networking silicon + hyperscaler XPU programs; $60-90B TAM by 2027",
        "themes":           ["ai_infrastructure", "semicap_supply_chain", "photonics_cpo"],
        "chain_layers":     ["custom_xpu_design", "leading_node_foundry", "optical_interconnect",
                             "advanced_packaging", "semicap_tooling"],
        "bottleneck_buckets": ["photonics", "foundry", "osat_packaging", "process_control"],
        "supplier_classes": ["foundry", "photonics", "packaging", "etch_deposition"],
        "foreign_exposure": ["TW", "NL", "JP"],
        "capex_scale":      "Custom XPU TAM $60-90B by 2027 (Broadcom estimate)",
        "anchor_ticker":    "AVGO",
    },
    "xAI": {
        "name":             "xAI (Grok / Colossus)",
        "description":      "Elon Musk's AI company — Colossus GPU cluster (200K+ H100/H200); massive power and photonics demand",
        "themes":           ["ai_infrastructure", "ai_power_energy", "photonics_cpo"],
        "chain_layers":     ["gpu_cluster_build", "datacenter_power", "optical_interconnect", "cooling_thermal"],
        "bottleneck_buckets": ["datacenter_power", "photonics", "cooling_thermal", "grid_transformers"],
        "supplier_classes": ["power_management", "photonics", "cooling", "grid_construction"],
        "foreign_exposure": ["NL", "JP"],
        "capex_scale":      "Colossus: 200K+ H100 cluster; Phase 2 expansion ongoing",
        "anchor_ticker":    None,
    },
    "Hyperscalers_AI_Capex": {
        "name":             "Hyperscaler AI Capex Cycle",
        "description":      "Combined MSFT/GOOGL/META/AMZN AI infrastructure build — ~$320-350B in 2025; most important capex cycle in tech history",
        "themes":           ["ai_power_energy", "grid_transformers", "photonics_cpo",
                             "advanced_packaging_test", "ai_infrastructure"],
        "chain_layers":     ["datacenter_construction", "power_grid_infrastructure", "optical_networking",
                             "cooling_thermal", "advanced_packaging", "semicap_tooling"],
        "bottleneck_buckets": ["datacenter_power", "grid_transformers", "photonics",
                               "cooling_thermal", "osat_packaging", "lithography"],
        "supplier_classes": ["power_management", "grid_construction", "optical_networking",
                             "cooling", "packaging", "foundry", "semicap_equipment"],
        "foreign_exposure": ["NL", "TW", "KR", "JP", "DE"],
        "capex_scale":      "$320-350B combined hyperscaler capex 2025",
        "anchor_ticker":    None,
    },
    "AI_Power_Buildout": {
        "name":             "AI Power & Grid Infrastructure",
        "description":      "Electricity demand surge from AI data centers — US grid upgrade supercycle; IEA forecasts 160% AI power demand rise by 2030",
        "themes":           ["ai_power_energy", "grid_transformers", "energy_transition"],
        "chain_layers":     ["power_generation", "grid_transmission", "distribution_substation",
                             "datacenter_power_mgmt"],
        "bottleneck_buckets": ["grid_transformers", "datacenter_power", "power_generation"],
        "supplier_classes": ["transformer_manufacturers", "switchgear", "power_management",
                             "grid_construction", "gas_turbine"],
        "foreign_exposure": ["DE", "FR", "JP"],
        "capex_scale":      "$1T+ US grid investment by 2030; transformer lead times 2-3 years",
        "anchor_ticker":    None,
    },
    "CoreWeave_Neocloud": {
        "name":             "Neocloud / AI-Native Cloud Compute",
        "description":      "CoreWeave, Lambda Labs, and neocloud peers — pure-play GPU cloud providers; fastest-growing AI compute demand; entirely dependent on NVDA H100/H200/GB200 supply",
        "themes":           ["ai_infrastructure", "neocloud", "ai_power_energy", "cooling_thermal", "photonics_cpo"],
        "chain_layers":     ["gpu_cluster_procurement", "datacenter_power", "optical_interconnect",
                             "cooling_thermal", "advanced_packaging_osat"],
        "bottleneck_buckets": ["hbm_memory", "osat_packaging", "datacenter_power",
                               "photonics", "cooling_thermal"],
        "supplier_classes": ["gpu_memory", "packaging", "power_management", "photonics",
                             "cooling", "optical_networking"],
        "foreign_exposure": ["TW", "KR", "NL", "JP"],
        "capex_scale":      "CoreWeave: $23B committed NVDA GPU orders; Lambda, Together, SambaNova adding capacity",
        "anchor_ticker":    None,
    },
}


def get_giant(giant_id: str) -> Optional[Dict[str, Any]]:
    """Return giant definition. Case-insensitive. Returns None if not found."""
    if giant_id in GIANT_MAP:
        return GIANT_MAP[giant_id]
    upper = giant_id.upper()
    for k, v in GIANT_MAP.items():
        if k.upper() == upper:
            return v
    return None


def list_giants() -> List[Dict[str, Any]]:
    """Return all giant anchors with summary metadata."""
    result = []
    for gid, g in GIANT_MAP.items():
        result.append({
            "id":               gid,
            "name":             g["name"],
            "description":      g["description"],
            "themes":           g["themes"],
            "capex_scale":      g["capex_scale"],
            "anchor_ticker":    g.get("anchor_ticker"),
            "foreign_exposure": g["foreign_exposure"],
            "bottleneck_buckets": g["bottleneck_buckets"],
        })
    return result
