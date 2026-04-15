"""
Theme alignment and bottleneck exposure scoring.

Three signal layers (combined for final score):
  1. Manual curated map     — highest confidence, override-level
  2. Company description    — keyword matching from FMP profile text
  3. Industry tags          — fallback inference from FMP "industry" field

theme_alignment   = broad thematic fit with investable macro narratives
bottleneck_exposure = specific constrained value-capture point inside a theme
                    (distinct from theme: a company can be in the AI theme
                     without being a physical chokepoint supplier)
"""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Set

# ── Theme taxonomy ────────────────────────────────────────────────────────────

ALL_THEMES = [
    "neocloud",
    "photonics_cpo",
    "ai_power_energy",
    "grid_transformers",
    "advanced_packaging_test",
    "defense_optics",
    "memory",
    "space",
    "ai_infrastructure",
    "semicap_supply_chain",
    "ai_software",
    "energy_transition",
    "biotech_catalyst",
]

THEME_LABELS: Dict[str, str] = {
    "neocloud":               "Neocloud / GPU-as-a-Service",
    "photonics_cpo":          "Photonics & Co-Packaged Optics",
    "ai_power_energy":        "AI Power & Energy Infrastructure",
    "grid_transformers":      "Grid Transformers & Switchgear",
    "advanced_packaging_test":"Advanced Packaging & Test",
    "defense_optics":         "Defense Optics & UAV",
    "memory":                 "Memory (HBM/DRAM/NAND)",
    "space":                  "Space & Satellite",
    "ai_infrastructure":      "AI Infrastructure (Silicon & Systems)",
    "semicap_supply_chain":   "Semiconductor Capital Equipment",
    "ai_software":            "AI Software & Platforms",
    "energy_transition":      "Energy Transition (Solar/Wind/Storage)",
    "biotech_catalyst":       "Biotech Catalyst",
}

# ── Manual ticker → theme map (highest confidence) ───────────────────────────

MANUAL_THEME_MAP: Dict[str, List[str]] = {
    "NVDA":  ["ai_infrastructure", "ai_power_energy", "advanced_packaging_test", "memory"],
    "AMD":   ["ai_infrastructure", "semicap_supply_chain"],
    "AVGO":  ["ai_infrastructure", "semicap_supply_chain"],
    "AMAT":  ["semicap_supply_chain", "advanced_packaging_test"],
    "LRCX":  ["semicap_supply_chain"],
    "KLAC":  ["semicap_supply_chain"],
    "ASML":  ["semicap_supply_chain"],
    "TSM":   ["semicap_supply_chain", "advanced_packaging_test"],
    "MRVL":  ["ai_infrastructure"],
    "SMCI":  ["ai_infrastructure", "ai_power_energy"],
    "CRDO":  ["ai_infrastructure", "photonics_cpo"],
    "LITE":  ["photonics_cpo", "ai_infrastructure"],
    "COHR":  ["photonics_cpo"],
    "AAOI":  ["photonics_cpo"],
    "II":    ["photonics_cpo"],
    "IPGP":  ["photonics_cpo"],
    "FNSR":  ["photonics_cpo"],
    "ETN":   ["grid_transformers", "ai_power_energy"],
    "GEV":   ["ai_power_energy"],
    "VRT":   ["ai_power_energy"],
    "PWR":   ["grid_transformers", "ai_power_energy"],
    "HUBB":  ["grid_transformers"],
    "ATKR":  ["grid_transformers"],
    "NVT":   ["grid_transformers", "ai_power_energy"],
    "AMETEK":["ai_power_energy"],
    "ENTG":  ["semicap_supply_chain", "advanced_packaging_test"],
    "ONTO":  ["semicap_supply_chain", "advanced_packaging_test"],
    "FORM":  ["advanced_packaging_test"],
    "ACLS":  ["semicap_supply_chain"],
    "MKSI":  ["semicap_supply_chain"],
    "AMKR":  ["advanced_packaging_test"],
    "ASX":   ["advanced_packaging_test"],
    "COHU":  ["advanced_packaging_test"],
    "MU":    ["memory", "ai_infrastructure"],
    "WDC":   ["memory"],
    "RMBS":  ["memory"],
    "KTOS":  ["defense_optics"],
    "AVAV":  ["defense_optics"],
    "DRS":   ["defense_optics"],
    "LHX":   ["defense_optics"],
    "LMT":   ["defense_optics", "space"],
    "NOC":   ["defense_optics", "space"],
    "RTX":   ["defense_optics"],
    "AXON":  ["defense_optics"],
    "BBAI":  ["ai_software", "defense_optics"],
    "RKLB":  ["space"],
    "ASTS":  ["space"],
    "PL":    ["space"],
    "PLTR":  ["ai_infrastructure", "ai_software"],
    "MSFT":  ["ai_infrastructure", "neocloud"],
    "GOOGL": ["ai_infrastructure", "neocloud"],
    "META":  ["ai_infrastructure"],
    "AMZN":  ["neocloud", "ai_infrastructure"],
    "NET":   ["neocloud"],
    "SNOW":  ["neocloud", "ai_software"],
    "DDOG":  ["neocloud", "ai_software"],
    "CRM":   ["ai_software"],
    "NOW":   ["ai_software"],
    "AI":    ["ai_software"],
    "UPST":  ["ai_software"],
    "INTC":  ["semicap_supply_chain", "ai_infrastructure", "photonics_cpo"],
    "ENPH":  ["energy_transition", "ai_power_energy"],
    "FSLR":  ["energy_transition"],
    "SEDG":  ["energy_transition"],
    "BE":    ["energy_transition", "ai_power_energy"],
    "PLUG":  ["energy_transition"],
    "MRNA":  ["biotech_catalyst"],
    "BNTX":  ["biotech_catalyst"],
    "REGN":  ["biotech_catalyst"],
    "VRTX":  ["biotech_catalyst"],
    "BEAM":  ["biotech_catalyst"],
    "EDIT":  ["biotech_catalyst"],
    "NTLA":  ["biotech_catalyst"],
    "CRSP":  ["biotech_catalyst"],
    "AKRO":  ["biotech_catalyst"],
    "ALNY":  ["biotech_catalyst"],
}

# ── Keyword-based detection from company description ─────────────────────────

THEME_KEYWORDS: Dict[str, List[str]] = {
    "neocloud": [
        "gpu cloud", "ai compute", "inference cloud", "neocloud",
        "ai cloud", "cloud gpu", "gpu-as-a-service",
    ],
    "photonics_cpo": [
        "co-packaged optics", "silicon photonics", "cpo",
        "optical interconnect", "datacom transceiver", "photonic",
        "coherent optical", "optical module", "transceiver", "vcsel",
    ],
    "ai_power_energy": [
        "data center power", "ai power", "hyperscale power",
        "power infrastructure", "thermal management", "cooling solution",
        "uninterruptible power", "ups system", "data center cooling",
    ],
    "grid_transformers": [
        "transformer", "switchgear", "grid infrastructure",
        "electrical distribution", "power distribution", "substation",
        "utility-scale", "high-voltage",
    ],
    "advanced_packaging_test": [
        "advanced packaging", "chiplet", "2.5d", "3d ic",
        "hbm packaging", "test socket", "burn-in",
        "wafer level packaging", "flip chip", "substrate",
    ],
    "defense_optics": [
        "defense optics", "electro-optical", "directed energy",
        "sensor fusion", "lidar", "unmanned aerial", "drone",
        "counter-uas", "surveillance system",
    ],
    "memory": [
        "hbm", "dram", "nand", "flash memory",
        "memory technology", "high bandwidth memory",
        "memory interface", "storage controller",
    ],
    "space": [
        "satellite", "launch vehicle", "space", "leo",
        "geostationary", "orbital", "smallsat", "cubesat",
    ],
    "ai_infrastructure": [
        "ai chip", "gpu", "ai accelerator", "ai inference",
        "ai training", "neural processing", "data center",
        "language model", "llm", "generative ai",
    ],
    "semicap_supply_chain": [
        "semiconductor equipment", "etch", "deposition",
        "lithography", "wafer fabrication", "process control",
        "ion implant", "epitaxial", "cmp slurry",
    ],
    "ai_software": [
        "ai platform", "machine learning platform", "mlops",
        "natural language", "computer vision", "ai-powered",
        "predictive analytics", "ai model",
    ],
    "energy_transition": [
        "solar", "wind", "battery storage", "electrolysis",
        "fuel cell", "ev charging", "renewable energy",
        "clean energy",
    ],
    "biotech_catalyst": [
        "fda approval", "clinical trial", "drug candidate",
        "gene therapy", "mrna vaccine", "crispr", "cell therapy",
        "biologic", "oncology pipeline",
    ],
}

# ── Industry → theme inference (lowest confidence) ───────────────────────────

INDUSTRY_THEME_MAP: Dict[str, List[str]] = {
    "Semiconductors":                        ["ai_infrastructure", "semicap_supply_chain"],
    "Semiconductor Equipment & Materials":   ["semicap_supply_chain"],
    "Electronic Components":                 ["photonics_cpo", "advanced_packaging_test"],
    "Aerospace & Defense":                   ["defense_optics", "space"],
    "Communication Equipment":               ["photonics_cpo"],
    "Electrical Equipment & Parts":          ["grid_transformers", "ai_power_energy"],
    "Software—Application":                  ["ai_software"],
    "Software—Infrastructure":               ["neocloud", "ai_infrastructure"],
    "Computer Hardware":                     ["ai_infrastructure", "ai_power_energy"],
    "Data Storage":                          ["memory", "ai_infrastructure"],
    "Solar":                                 ["energy_transition"],
    "Biotechnology":                         ["biotech_catalyst"],
    "Drug Manufacturers—General":            ["biotech_catalyst"],
}


def score_theme_alignment(
    ticker: str,
    description: str,
    industry: str,
    preferred_themes: List[str],
) -> "FactorDetail":
    """
    Score theme alignment: how well does this ticker match the playbook's preferred themes?

    Scoring:
      - Manual map match:    +1.0 confidence per theme
      - Description keyword: +0.7 confidence per theme detected
      - Industry inference:  +0.4 confidence per theme detected

    Final score = (matched_confidence / max_possible) scaled to 0-100,
    weighted by whether the matched theme is in preferred_themes.

    Returns FactorDetail with matched themes list in source_tags.
    """
    from services.playbook.playbook_types import FactorDetail

    t = ticker.upper().strip()
    desc_lower = (description or "").lower()
    ind_lower  = (industry or "").lower()

    # Confidence accumulator per theme
    theme_conf: Dict[str, float] = {}

    # 1. Manual map
    for theme in MANUAL_THEME_MAP.get(t, []):
        theme_conf[theme] = theme_conf.get(theme, 0) + 1.0

    # 2. Description keywords
    for theme, keywords in THEME_KEYWORDS.items():
        for kw in keywords:
            if kw in desc_lower:
                theme_conf[theme] = max(theme_conf.get(theme, 0), 0.7)
                break

    # 3. Industry inference
    for ind_key, themes in INDUSTRY_THEME_MAP.items():
        if ind_key.lower() in ind_lower or ind_lower in ind_key.lower():
            for theme in themes:
                if theme not in theme_conf:
                    theme_conf[theme] = 0.4

    matched_themes = [th for th, conf in theme_conf.items() if conf >= 0.4]

    if not matched_themes:
        return FactorDetail(
            score=40.0,
            status="heuristic",
            reasons=["No theme match found for this ticker"],
            source_tags=[],
        )

    # Preferred-theme bonus: matching preferred theme counts double
    pref_set: Set[str] = set(preferred_themes)
    total_score = 0.0
    for theme, conf in theme_conf.items():
        weight = 2.0 if theme in pref_set else 1.0
        total_score += conf * weight

    # Normalize: max possible if all preferred themes match at conf=1.0
    max_possible = max(1, len(pref_set)) * 2.0 + max(0, len(matched_themes) - len(pref_set)) * 1.0
    normalized = min(100.0, (total_score / max_possible) * 100.0)

    # Boost for multiple preferred matches
    pref_matches = [t for t in matched_themes if t in pref_set]
    if len(pref_matches) >= 2:
        normalized = min(100.0, normalized + 8.0)

    reasons: List[str] = []
    if pref_matches:
        labels = [THEME_LABELS.get(th, th) for th in pref_matches[:3]]
        reasons.append(f"Preferred theme match: {', '.join(labels)}")
    non_pref = [t for t in matched_themes if t not in pref_set]
    if non_pref:
        reasons.append(f"Other themes: {', '.join(non_pref[:3])}")

    source = "manual" if t in MANUAL_THEME_MAP else "heuristic"

    return FactorDetail(
        score=round(normalized, 1),
        status=source,
        reasons=reasons,
        source_tags=matched_themes,
    )


# ── Bottleneck exposure ───────────────────────────────────────────────────────

BOTTLENECK_MAP: Dict[str, Dict] = {
    "ASML":  {"score": 95, "bucket": "lithography",            "reason": "Sole global EUV scanner supplier — no substitute"},
    "AMAT":  {"score": 85, "bucket": "semicap_supply_chain",   "reason": "Largest semiconductor equipment company globally"},
    "LRCX":  {"score": 83, "bucket": "semicap_supply_chain",   "reason": "Dominant etch equipment — no easy substitute"},
    "KLAC":  {"score": 82, "bucket": "semicap_supply_chain",   "reason": "Process control monopoly for advanced nodes"},
    "ENTG":  {"score": 80, "bucket": "advanced_packaging_test","reason": "Critical CMP slurries / photoresist bottleneck"},
    "AMKR":  {"score": 78, "bucket": "advanced_packaging_test","reason": "Advanced OSAT packaging — thin substitution pool"},
    "LITE":  {"score": 85, "bucket": "photonics_cpo",          "reason": "Dominant CPO and coherent optical module supplier"},
    "COHR":  {"score": 80, "bucket": "photonics_cpo",          "reason": "High-speed photonic components — AI interconnect bottleneck"},
    "AAOI":  {"score": 77, "bucket": "photonics_cpo",          "reason": "400G/800G datacenter transceiver — limited supplier pool"},
    "CRDO":  {"score": 78, "bucket": "photonics_cpo",          "reason": "AEC/DSP IC for AI fabric — concentrated market"},
    "II":    {"score": 74, "bucket": "photonics_cpo",          "reason": "InP substrates — niche supplier for photonic ICs"},
    "IPGP":  {"score": 75, "bucket": "photonics_cpo",          "reason": "High-power fiber laser — industrial & defense chokepoint"},
    "ETN":   {"score": 80, "bucket": "grid_transformers",      "reason": "Power management / transformer market leader"},
    "GEV":   {"score": 82, "bucket": "ai_power_energy",        "reason": "Gas turbine for off-grid AI power — limited competition"},
    "VRT":   {"score": 80, "bucket": "ai_power_energy",        "reason": "Data center power / cooling — concentrated market"},
    "SMCI":  {"score": 75, "bucket": "ai_power_energy",        "reason": "GPU server integration — specialized thermal expertise"},
    "NVT":   {"score": 73, "bucket": "grid_transformers",      "reason": "Data center enclosures and power infrastructure"},
    "PWR":   {"score": 72, "bucket": "grid_transformers",      "reason": "Electrical construction for grid buildout"},
    "HUBB":  {"score": 72, "bucket": "grid_transformers",      "reason": "Grid connectors / components — specialty supplier"},
    "ONTO":  {"score": 75, "bucket": "advanced_packaging_test","reason": "Metrology for advanced packaging — thin vendor pool"},
    "FORM":  {"score": 73, "bucket": "advanced_packaging_test","reason": "Test socket / burn-in — specialized niche supplier"},
    "KTOS":  {"score": 75, "bucket": "defense_optics",         "reason": "Drone and directed energy systems — defense supply chain"},
    "AVAV":  {"score": 72, "bucket": "defense_optics",         "reason": "Tactical UAV — defense-specific supplier"},
    "LHX":   {"score": 70, "bucket": "defense_optics",         "reason": "Electro-optical ISR systems"},
    "MU":    {"score": 78, "bucket": "memory",                 "reason": "HBM memory for AI accelerators — 3-player market"},
    "RKLB":  {"score": 75, "bucket": "space",                  "reason": "Small satellite launch — limited launch providers"},
    "MKSI":  {"score": 72, "bucket": "semicap_supply_chain",   "reason": "Gas / pressure management for semicap — specialized"},
    "ACLS":  {"score": 73, "bucket": "semicap_supply_chain",   "reason": "Ion implant for SiC / EV chips — thin market"},
    "RMBS":  {"score": 70, "bucket": "memory",                 "reason": "Memory interface chips — small addressable market"},
    "INTC":  {"score": 68, "bucket": "semicap_supply_chain",   "reason": "Integrated device manufacturer with foundry ambitions"},
    "TSM":   {"score": 88, "bucket": "semicap_supply_chain",   "reason": "Leading-node foundry — globally critical chokepoint"},
}

BOTTLENECK_KEYWORDS: Dict[str, int] = {
    "sole source":            88,
    "only supplier":          87,
    "sole provider":          86,
    "sole-source":            88,
    "co-packaged optics":     83,
    "cpo":                    80,
    "silicon photonics":      79,
    "advanced packaging":     75,
    "chiplet":                74,
    "high bandwidth memory":  78,
    "hbm":                    77,
    "test socket":            73,
    "burn-in":                72,
    "directed energy":        76,
    "electro-optical":        73,
    "ion implant":            74,
    "process control":        72,
    "grid transformer":       78,
    "gas turbine":            72,
    "etch chamber":           75,
    "euv":                    82,
    "extreme ultraviolet":    82,
    "lithography":            78,
    "wafer level packaging":  75,
    "3d ic":                  74,
    "2.5d":                   73,
    "coherent optical":       79,
    "transceiver module":     70,
    "cmp slurry":             75,
    "photoresist":            74,
}


def score_bottleneck_exposure(
    ticker: str,
    description: str,
    industry: str,
) -> "FactorDetail":
    """
    Score bottleneck exposure: is this company a constrained chokepoint supplier
    in a larger macro buildout?

    Three layers:
      1. Manual map lookup (authoritative, highest confidence)
      2. Description keyword match (heuristic)
      3. Industry heuristic fallback

    Higher score = more direct bottleneck exposure.
    """
    from services.playbook.playbook_types import FactorDetail

    t = ticker.upper().strip()
    desc_lower = (description or "").lower()

    # 1. Manual map — highest trust
    if t in BOTTLENECK_MAP:
        entry = BOTTLENECK_MAP[t]
        return FactorDetail(
            score=float(entry["score"]),
            status="manual",
            reasons=[entry["reason"]],
            source_tags=[entry["bucket"]],
        )

    # 2. Description keyword match
    keyword_scores: List[int] = []
    matched_keywords: List[str] = []
    for kw, score_val in BOTTLENECK_KEYWORDS.items():
        if kw in desc_lower:
            keyword_scores.append(score_val)
            matched_keywords.append(kw)

    if keyword_scores:
        # Take the top-2 average (don't triple-count compound descriptions)
        top2 = sorted(keyword_scores, reverse=True)[:2]
        avg_score = sum(top2) / len(top2)
        return FactorDetail(
            score=round(avg_score, 1),
            status="heuristic",
            reasons=[f"Description keywords: {', '.join(matched_keywords[:3])}"],
            source_tags=matched_keywords[:4],
        )

    # 3. Industry fallback — broad exposure only
    ind_lower = (industry or "").lower()
    if any(kw in ind_lower for kw in ("semiconductor equipment", "electronic component", "aerospace")):
        return FactorDetail(
            score=55.0,
            status="heuristic",
            reasons=["Industry suggests possible supply-chain exposure (not confirmed)"],
            source_tags=["industry_inference"],
        )

    return FactorDetail(
        score=30.0,
        status="fallback",
        reasons=["No bottleneck exposure signal found"],
        source_tags=[],
    )
