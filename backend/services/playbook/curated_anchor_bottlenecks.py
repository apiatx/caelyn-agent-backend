"""
Serenity Curated Anchor Bottleneck Maps — static, deterministic, no-LLM.

Source of truth: user-provided curated public-company supply-chain research.
Do NOT call OpenAI / Claude / web-search from this module.
"""

from __future__ import annotations

from typing import Optional

LAST_CURATED_AT: str = "2026-06-20"

_LAYER_NAMES: dict[int, str] = {
    0: "Platform Anchor",
    1: "Systems Integrator",
    2: "Key Component",
    3: "Constrained Bottleneck",
    4: "Upstream Specialty",
}

_GRADE_WHY_HIDDEN: dict[str, str] = {
    "A": (
        "Relationship is formally disclosed in SEC filings or earnings calls and "
        "institutionally tracked, but supply-chain depth is rarely modeled by the buy-side."
    ),
    "B": (
        "Relationship is documented in public industry sources but not prominently disclosed; "
        "most buy-side screens miss this dependency."
    ),
    "C": (
        "Relationship is inferred from product category, disclosed customer mix, or "
        "industry knowledge; institutional screens rarely surface this name."
    ),
}


def _rel_type(rel_spec: str) -> str:
    return {
        "confirmed_supplier":       "direct",
        "confirmed_partner":        "direct",
        "confirmed_asset_partner":  "infrastructure",
        "direct":                   "direct",
        "ecosystem":                "indirect",
        "critical_upstream":        "direct",
        "strong_proxy":             "public_proxy",
        "category_proxy":           "public_proxy",
    }.get(rel_spec, "direct")


def _why_it_matters(anchor_name: str, co_name: str, layer: int, role: str, score: int) -> str:
    ln = _LAYER_NAMES.get(layer, f"Layer {layer}")
    return (
        f"{co_name} is a {ln} node in the {anchor_name} supply chain, "
        f"providing {role.lower().rstrip('.')}. "
        f"Bottleneck score {score}/100 reflects structural scarcity and switching cost."
    )


def _why_now(anchor_name: str, role: str) -> str:
    return (
        f"{anchor_name}'s accelerating capital expenditure and production ramp are "
        f"driving immediate demand for {role.lower().rstrip('.')}."
    )


def _what_breaks(rel_spec: str, anchor_name: str, ticker: str) -> str:
    if rel_spec in ("confirmed_supplier", "direct", "critical_upstream"):
        return (
            f"{anchor_name} qualifies an alternative supplier or brings "
            f"{ticker}'s product category in-house."
        )
    return (
        f"{anchor_name} expands captive supply, the relationship ends, "
        f"or the thesis becomes fully priced into {ticker}'s valuation."
    )


def _fill_node(anchor_key: str, anchor_name: str, n: dict) -> dict:
    ticker   = n["ticker"]
    co_name  = n["company_name"]
    tv       = n.get("tradingview_symbol", ticker)
    layer    = n["layer"]
    role     = n["supply_chain_role"]
    score    = n["bottleneck_score"]
    grade    = n.get("evidence_grade", "B")
    gr_rsn   = n.get("evidence_grade_reason", "")
    rel_spec = n.get("relationship_specificity", "direct")
    conf     = n.get("confidence") or ("high" if score >= 70 else "medium" if score >= 50 else "low")
    themes   = n.get("themes", [])
    evidence = n.get("evidence", [])
    src_urls = n.get("source_urls", [])
    # Derive anchor_theme / theme from themes list (first entry) or anchor key
    primary_theme = themes[0] if themes else anchor_key.lower()

    return {
        # ── Chain Reaction–compatible fields (preserve existing drawer/table) ──
        "bottleneck_ticker":        ticker,
        "company_name":             co_name,
        "anchor_ticker":            anchor_key,
        "giant_anchors":            [anchor_key],
        "supply_chain_role":        role,
        "layer":                    layer,
        "themes":                   themes,
        "bottleneck_score":         float(score),
        "confidence":               conf,
        "evidence":                 evidence,
        "relationship_type":        _rel_type(rel_spec),
        "source_urls":              src_urls,
        "why_it_matters":           n.get("why_it_matters")           or _why_it_matters(anchor_name, co_name, layer, role, int(score)),
        "why_hidden":               n.get("why_hidden")               or _GRADE_WHY_HIDDEN.get(grade, _GRADE_WHY_HIDDEN["B"]),
        "why_now":                  n.get("why_now")                  or _why_now(anchor_name, role),
        "what_would_break_thesis":  n.get("what_would_break_thesis")  or _what_breaks(rel_spec, anchor_name, ticker),
        # ── Fallback fields to match /api/bottlenecks/current row shape ────────
        # Score components: curated rows carry no live scoring; use None so the
        # frontend can distinguish "not computed" from "zero".
        "final_score":              float(score),      # best deterministic proxy
        "theme_alignment_score":    100.0,             # curated = fully aligned
        "bottleneck_type":          "supply_chain",    # static category for curated rows
        "bottleneckReason":         role,              # camelCase alias of supply_chain_role
        "anchor_theme":             primary_theme,
        "theme":                    primary_theme,
        "discovery_sources":        ["curated_static"],
        "lastUpdated":              LAST_CURATED_AT,
        # Live market fields — not available without a quote lookup; None signals
        # "not enriched" to the frontend (same pattern as optional current rows).
        "momentum_score":           None,
        "volume_score":             None,
        "fundamental_score":        None,
        "social_score":             None,
        "options_score":            None,
        "change_percent_1d":        None,
        "revenueSignal":            None,
        "exchange":                 None,
        "country":                  None,
        "market_cap":               None,
        "marketCap":                None,
        "marketCapBucket":          None,
        # ── Curated-specific fields ────────────────────────────────────────────
        "anchor_key":               anchor_key,
        "anchor_name":              anchor_name,
        "tradingview_symbol":       tv,
        "layer_name":               _LAYER_NAMES.get(layer, ""),
        "evidence_grade":           grade,
        "evidence_grade_reason":    gr_rsn,
        "relationship_specificity": rel_spec,
        "source_type":              "curated_static",
        "manual_added":             False,
        "last_curated_at":          LAST_CURATED_AT,
    }


# ── Raw curated node lists (compact; _fill_node expands to full shape) ─────────

_ANCHOR_RAW: dict[str, dict] = {

    # ═══════════════════════════════════════════════════════════════════════════
    # SPCX — SpaceX
    # ═══════════════════════════════════════════════════════════════════════════
    "SPCX": {
        "anchor_name": "SpaceX",
        "nodes": [
            {
                "ticker": "LIN",
                "company_name": "Linde plc",
                "layer": 3,
                "supply_chain_role": "Liquid oxygen and liquid methane supply for Starship/Falcon propulsion",
                "bottleneck_score": 88,
                "evidence_grade": "A",
                "evidence_grade_reason": "Long-term industrial-gas supply contracts for launch-site cryogenics; named in SpaceX site infrastructure disclosures.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["cryogenic_systems", "launch_supply_chain"],
                "evidence": [
                    "Linde provides liquid oxygen and liquid methane at SpaceX's Boca Chica (Starbase) and Cape Canaveral launch sites under long-term supply agreements.",
                    "SpaceX's Starship requires ~5,000 tonnes of liquid oxygen per flight; Linde is the primary industrial gas supplier for both launch complexes.",
                    "Linde's space-cryogenics business is referenced in customer-concentration disclosures across aerospace launch clients.",
                ],
            },
            {
                "ticker": "ATI",
                "company_name": "ATI Inc.",
                "layer": 3,
                "supply_chain_role": "Titanium and nickel superalloys for Falcon 9 and Raptor engine components",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "SpaceX disclosed as a significant aerospace customer in ATI's 2022 10-K; high-single-digit HPMC revenue concentration.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["propulsion_materials", "launch_supply_chain"],
                "evidence": [
                    "ATI's 2022 annual report lists SpaceX among significant aerospace customers of its High Performance Materials & Components segment.",
                    "ATI supplies titanium 6-4 and nickel alloys (Inconel 625, 718) used in Falcon 9 and Raptor turbopump blades and engine structures.",
                    "ATI's specialty alloy capacity is a structural bottleneck: qualification cycles for new aerospace alloy sources span 18–36 months.",
                ],
                "source_urls": ["https://www.ati.com/investors/annual-reports/2022-annual-report"],
            },
            {
                "ticker": "HWM",
                "company_name": "Howmet Aerospace",
                "layer": 2,
                "supply_chain_role": "Precision investment castings and forgings for Raptor and Merlin engine components",
                "bottleneck_score": 82,
                "evidence_grade": "A",
                "evidence_grade_reason": "Howmet is the leading public-company provider of aerospace investment castings; SpaceX propulsion reliance confirmed in aerospace customer mix.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["propulsion_materials", "launch_supply_chain"],
                "evidence": [
                    "Howmet supplies superalloy turbine blades, structural castings, and forged engine components used across multiple SpaceX propulsion programs.",
                    "Howmet's Engines segment produces complex near-net-shape castings used in rocket turbomachinery — a process with decades-long qualification barriers.",
                    "SpaceX's Falcon 9 and Raptor engines both use turbopumps that rely on precision investment castings; Howmet and its peers are the only qualified suppliers at volume.",
                ],
            },
            {
                "ticker": "HXL",
                "company_name": "Hexcel Corporation",
                "layer": 3,
                "supply_chain_role": "Carbon fiber prepregs and composite materials for Falcon 9 fairings and structures",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "Hexcel is the dominant aerospace-grade carbon fiber prepreg supplier; SpaceX fairing composites dependency is industry-documented.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["launch_supply_chain", "propulsion_materials"],
                "evidence": [
                    "Hexcel's HexPly carbon fiber prepregs are used in SpaceX Falcon 9 composite fairing panels and interstage structures.",
                    "SpaceX's Starship nose cone and payload fairing use large-format carbon fiber lay-up; Hexcel's aerospace-grade materials are a primary input.",
                    "Hexcel investor presentations cite launch vehicle structures as a growing aerospace end-market.",
                ],
                "source_urls": ["https://investors.hexcel.com/investor-presentations"],
            },
            {
                "ticker": "CRS",
                "company_name": "Carpenter Technology Corporation",
                "layer": 3,
                "supply_chain_role": "Premium specialty stainless and high-strength steels for SpaceX Starship structures",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "Carpenter's PH 17-4 and 304L stainless steels match SpaceX Starship structural specifications; relationship inferred from product qualification and industry filings.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["propulsion_materials", "launch_supply_chain"],
                "evidence": [
                    "SpaceX's Starship is constructed from custom 304L stainless steel; Carpenter Technology is a leading qualified domestic producer of aerospace-grade stainless.",
                    "Carpenter's aerospace special alloy capacity is a U.S. onshoring priority under NDAA, benefiting SpaceX's Starship stainless steel sourcing.",
                ],
            },
            {
                "ticker": "MTRN",
                "company_name": "Materion Corporation",
                "layer": 3,
                "supply_chain_role": "Beryllium, copper-beryllium alloys, and advanced engineered materials for SpaceX launch hardware",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "Materion is the primary U.S. supplier of beryllium alloys for aerospace applications; presence in SpaceX programs inferred from launch hardware materials specifications.",
                "relationship_specificity": "critical_upstream",
                "themes": ["propulsion_materials", "critical_materials_rare_earth"],
                "evidence": [
                    "Materion supplies beryllium and copper-beryllium precision parts for aerospace actuators, guidance systems, and cryogenic valves used by SpaceX.",
                    "Beryllium is a controlled material with a single dominant U.S. supplier (Materion); SpaceX uses beryllium alloys in avionics and precision mechanical components.",
                ],
            },
            {
                "ticker": "STM",
                "company_name": "STMicroelectronics N.V.",
                "tradingview_symbol": "STM",
                "layer": 2,
                "supply_chain_role": "Power management and microcontroller ICs for Starlink user terminal electronics",
                "bottleneck_score": 75,
                "evidence_grade": "B",
                "evidence_grade_reason": "STM is a confirmed supplier of MCUs and power ICs to the consumer electronics supply chain that manufactures Starlink user terminals.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems", "semicap_supply_chain"],
                "evidence": [
                    "STMicroelectronics STM32 microcontrollers and power management ICs appear in Starlink user terminal teardowns across multiple hardware generations.",
                    "STM's power semiconductors are embedded in the dish motors and phased-array control circuits of Starlink Gen2 terminals.",
                ],
            },
            {
                "ticker": "FLTCF",
                "company_name": "Filtronic plc",
                "tradingview_symbol": "FLTCF",
                "layer": 2,
                "supply_chain_role": "mmWave RF power amplifiers for Starlink Direct-to-Cell satellite payloads",
                "bottleneck_score": 92,
                "evidence_grade": "A",
                "evidence_grade_reason": "Filtronic publicly disclosed a supply agreement with SpaceX for mmWave amplifiers used in Starlink V2 direct-to-cell payloads.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems", "space"],
                "evidence": [
                    "Filtronic disclosed a formal supply agreement with SpaceX for its Cerus 5G mmWave power amplifier modules, used in Starlink V2 satellites for Direct-to-Cell service.",
                    "Filtronic's mmWave amplifiers are custom-designed to SpaceX specifications; the company has no other customer of equivalent scale for this product line.",
                    "Filtronic stated that the SpaceX contract represents a transformational revenue event and constitutes the majority of its near-term order backlog.",
                ],
                "source_urls": ["https://www.filtronic.com/investors/rns-announcements"],
                "why_hidden": "Filtronic is a sub-£100M market cap UK-listed company rarely covered by U.S. equity analysts despite being a named, sole-source supplier to SpaceX's Direct-to-Cell program.",
            },
            {
                "ticker": "WNCWY",
                "company_name": "Wistron NeWeb Corporation",
                "tradingview_symbol": "WNCWY",
                "layer": 2,
                "supply_chain_role": "Contract manufacturer of Starlink user terminal dishes and RF modules",
                "bottleneck_score": 78,
                "evidence_grade": "A",
                "evidence_grade_reason": "Wistron NeWeb is publicly identified as SpaceX's primary Starlink user terminal contract manufacturer across multiple industry teardown and supply chain reports.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Wistron NeWeb (WNC) manufactures the Starlink flat-panel dish assembly and RF front-end modules; identified in multiple independent supply-chain teardowns.",
                    "WNC's Taoyuan facilities produce the bulk of Starlink Gen1 and Gen2 consumer dishes; the program represents a significant share of WNC's antenna/connectivity revenue.",
                ],
            },
            {
                "ticker": "CPILY",
                "company_name": "Chin-Poon Industrial Co., Ltd.",
                "tradingview_symbol": "CPILY",
                "layer": 3,
                "supply_chain_role": "High-frequency PCBs for Starlink user terminal phased-array antenna modules",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "Chin-Poon supplies RF PCBs to WNC and other Starlink terminal manufacturers; documented in Taiwan supply chain filings and industry reports.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Chin-Poon is a qualified Tier-2 PCB supplier for Starlink terminal phased-array antennas, providing PTFE-based high-frequency laminates to terminal assemblers.",
                    "Taiwan supply chain filings reference Chin-Poon as a key PCB vendor for the Starlink flat-panel dish production network.",
                ],
            },
            {
                "ticker": "SHTHY",
                "company_name": "Shenmao Technology Inc.",
                "tradingview_symbol": "SHTHY",
                "layer": 4,
                "supply_chain_role": "Solder paste and alloys for Starlink user terminal PCB assembly",
                "bottleneck_score": 58,
                "evidence_grade": "B",
                "evidence_grade_reason": "Shenmao is the dominant Taiwan-based solder materials supplier; presence in Starlink terminal assembly supply chain documented by electronics manufacturing analysts.",
                "relationship_specificity": "critical_upstream",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Shenmao Technology supplies lead-free solder pastes and alloys used across the Taiwanese Starlink terminal PCB assembly supply chain.",
                    "Shenmao holds dominant market share in Taiwan's electronics assembly materials market, making it a structural upstream supplier to all major Starlink assemblers.",
                ],
            },
            {
                "ticker": "CPQMF",
                "company_name": "Compeq Manufacturing Co., Ltd.",
                "tradingview_symbol": "CPQMF",
                "layer": 3,
                "supply_chain_role": "PCBs and substrates for Starlink user terminal electronics",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Compeq is a confirmed PCB supplier to Starlink terminal manufacturers including WNC.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Compeq Manufacturing is a Tier-2 PCB supplier for Starlink user terminal motherboards and RF modules, documented in supply chain teardowns.",
                ],
            },
            {
                "ticker": "6271",
                "company_name": "Tong Hsing Electronic Industries",
                "tradingview_symbol": "TW:6271",
                "layer": 3,
                "supply_chain_role": "Ceramic IC packages for Starlink satellite-grade electronics",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "Tong Hsing supplies ceramic electronic packages used in satellite and space-grade applications; Starlink relationship documented in Taiwan supply chain research.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Tong Hsing supplies ceramic packages and substrates for space-qualified ICs used in Starlink satellite electronics.",
                ],
            },
            {
                "ticker": "MEIOY",
                "company_name": "Meiko Electronics Co., Ltd.",
                "tradingview_symbol": "MEIOY",
                "layer": 3,
                "supply_chain_role": "High-density PCBs for Starlink satellite main board electronics",
                "bottleneck_score": 60,
                "evidence_grade": "B",
                "evidence_grade_reason": "Meiko Electronics is a Japan-based PCB maker supplying high-layer-count boards to the Starlink satellite manufacturing supply chain.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Meiko Electronics produces multi-layer PCBs for satellite electronics applications; industry reports cite Meiko as a supply chain participant for Starlink constellation manufacturing.",
                ],
            },
            {
                "ticker": "KNPOY",
                "company_name": "Kinpo Electronics Inc.",
                "tradingview_symbol": "KNPOY",
                "layer": 2,
                "supply_chain_role": "Consumer electronics manufacturing and assembly for Starlink terminal production",
                "bottleneck_score": 58,
                "evidence_grade": "B",
                "evidence_grade_reason": "Kinpo group companies participate in Starlink terminal contract manufacturing alongside WNC.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems"],
                "evidence": [
                    "Kinpo Electronics and its affiliates provide contract manufacturing services for Starlink user terminal accessories and sub-assemblies.",
                ],
            },
            {
                "ticker": "TSM",
                "company_name": "Taiwan Semiconductor Manufacturing Company",
                "layer": 3,
                "supply_chain_role": "Advanced logic foundry for Starlink phased-array antenna ICs and satellite processors",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "TSMC is the sole-source foundry for SpaceX's custom-designed Starlink phased-array silicon; confirmed in SpaceX antenna teardowns.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems", "semicap_supply_chain"],
                "evidence": [
                    "SpaceX's Starlink Gen2 uses a custom phased-array ASIC fabricated at TSMC; identified in independent teardown analyses.",
                    "SpaceX's proprietary antenna silicon requires advanced TSMC nodes (16nm/7nm); no alternative foundry can manufacture at equivalent performance.",
                ],
            },
            {
                "ticker": "ASML",
                "company_name": "ASML Holding N.V.",
                "layer": 4,
                "supply_chain_role": "EUV lithography equipment enabling TSMC production of Starlink phased-array ICs",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "ASML is the sole provider of EUV lithography equipment; upstream dependency for all TSMC advanced nodes used by SpaceX.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain", "satellite_systems"],
                "evidence": [
                    "ASML holds a global monopoly on EUV lithography; every advanced-node chip produced at TSMC for SpaceX's Starlink satellites depends on ASML EUV scanners.",
                ],
            },
            {
                "ticker": "AMAT",
                "company_name": "Applied Materials Inc.",
                "layer": 4,
                "supply_chain_role": "CVD/PVD deposition equipment for TSMC advanced nodes producing Starlink ICs",
                "bottleneck_score": 60,
                "evidence_grade": "B",
                "evidence_grade_reason": "AMAT is the largest semiconductor equipment company by revenue; provides deposition equipment critical to every TSMC node used by SpaceX.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Applied Materials deposition tools are used in every TSMC advanced node fab; SpaceX's Starlink phased-array IC production depends on AMAT-enabled TSMC capacity.",
                ],
            },
            {
                "ticker": "LRCX",
                "company_name": "Lam Research Corporation",
                "layer": 4,
                "supply_chain_role": "Etch and deposition equipment for TSMC fabs producing Starlink chips",
                "bottleneck_score": 58,
                "evidence_grade": "B",
                "evidence_grade_reason": "Lam Research is the leading etch equipment supplier; structural upstream dependency for all TSMC advanced node production.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Lam Research's etch systems are essential to the TSMC production process for advanced chips; SpaceX's phased-array satellite ICs are manufactured at TSMC using Lam equipment.",
                ],
            },
            {
                "ticker": "KLAC",
                "company_name": "KLA Corporation",
                "layer": 4,
                "supply_chain_role": "Wafer inspection and process control for TSMC advanced node production of Starlink chips",
                "bottleneck_score": 56,
                "evidence_grade": "B",
                "evidence_grade_reason": "KLA holds ~50% global share of process control and inspection; essential for yield management at every TSMC advanced node.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "KLA's inspection and metrology systems enable TSMC advanced-node yield ramp; SpaceX Starlink chip production at TSMC relies on KLA process control.",
                ],
            },
            {
                "ticker": "TMUS",
                "company_name": "T-Mobile US Inc.",
                "layer": 1,
                "supply_chain_role": "Terrestrial mobile network partner for SpaceX Starlink Direct-to-Cell service",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "T-Mobile and SpaceX announced a formal partnership to provide satellite-direct mobile coverage; regulatory approvals confirm commercial relationship.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["satellite_systems", "ground_infrastructure"],
                "evidence": [
                    "T-Mobile and SpaceX signed a commercial partnership to deliver Starlink Direct-to-Cell satellite service over the T-Mobile 1900 MHz PCS spectrum band.",
                    "The T-Mobile partnership is SpaceX's primary go-to-market channel for mobile satellite coverage in the United States; FCC regulatory filings confirm the commercial structure.",
                ],
            },
            {
                "ticker": "RKLB",
                "company_name": "Rocket Lab USA Inc.",
                "layer": 1,
                "supply_chain_role": "Spacecraft component supplier and launch ecosystem participant; Photon satellite bus used in SpaceX-adjacent missions",
                "bottleneck_score": 48,
                "evidence_grade": "B",
                "evidence_grade_reason": "Rocket Lab is a space infrastructure supplier whose components and mission services are referenced in SpaceX-adjacent satellite programs.",
                "relationship_specificity": "ecosystem",
                "themes": ["launch_supply_chain", "space"],
                "evidence": [
                    "Rocket Lab supplies reaction wheels, solar panels, and satellite bus components to operators in the broader LEO supply chain that overlaps with SpaceX-dependent infrastructure.",
                ],
            },
            {
                "ticker": "LHX",
                "company_name": "L3Harris Technologies Inc.",
                "layer": 2,
                "supply_chain_role": "Ground station equipment and satellite communication systems for space infrastructure",
                "bottleneck_score": 48,
                "evidence_grade": "C",
                "evidence_grade_reason": "L3Harris supplies government and commercial ground-station hardware for space applications; SpaceX ground network overlap is inferred from product category.",
                "relationship_specificity": "category_proxy",
                "themes": ["ground_infrastructure", "space"],
                "evidence": [
                    "L3Harris is a leading supplier of ground station equipment, tracking antennas, and space communication systems that serve the broader SpaceX mission-adjacent market.",
                ],
            },
            {
                "ticker": "KULR",
                "company_name": "KULR Technology Group Inc.",
                "layer": 3,
                "supply_chain_role": "Thermal management solutions for satellite and launch vehicle battery and power systems",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "KULR has publicly referenced SpaceX-adjacent thermal management work; specialty thermal interface materials are critical for Starlink satellite power systems.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["satellite_systems", "space", "cooling_thermal"],
                "evidence": [
                    "KULR Technology supplies carbon fiber thermal management materials and battery safety systems for space applications, with SpaceX-affiliated programs referenced in investor materials.",
                    "KULR's VIBE thermal interface material is qualified for satellite power management in high-vibration launch environments.",
                ],
            },
            {
                "ticker": "AMPX",
                "company_name": "Amprius Technologies Inc.",
                "layer": 3,
                "supply_chain_role": "Silicon-anode lithium-ion batteries for satellite power and ground support applications",
                "bottleneck_score": 55,
                "evidence_grade": "B",
                "evidence_grade_reason": "Amprius publicly cited SpaceX as a development partner for high-energy-density silicon-anode batteries for space applications.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["battery_grid_storage", "space"],
                "evidence": [
                    "Amprius Technologies disclosed that SpaceX is evaluating its silicon-anode battery technology for use in satellite and drone power systems.",
                    "Amprius batteries achieve >400 Wh/kg energy density, meeting the mass-constrained requirements of LEO satellite power systems.",
                ],
            },
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "layer": 2,
                "supply_chain_role": "AI compute and networking silicon for Starlink network operations and SpaceX AI-driven launch systems",
                "bottleneck_score": 45,
                "evidence_grade": "C",
                "evidence_grade_reason": "SpaceX's AI and simulation workloads are known to use GPU compute; NVDA's role is inferred from disclosed AI infrastructure at Starbase.",
                "relationship_specificity": "category_proxy",
                "themes": ["ai_infrastructure", "satellite_systems"],
                "evidence": [
                    "SpaceX uses GPU-accelerated AI for rocket simulation, trajectory optimization, and autonomous landing; NVIDIA GPUs are the dominant platform for these workloads.",
                ],
            },
            {
                "ticker": "AMD",
                "company_name": "Advanced Micro Devices Inc.",
                "layer": 2,
                "supply_chain_role": "Radiation-tolerant FPGAs and embedded processors for SpaceX avionics and satellite electronics",
                "bottleneck_score": 42,
                "evidence_grade": "C",
                "evidence_grade_reason": "AMD's Xilinx FPGAs are widely used in space avionics; SpaceX usage is inferred from product qualification and industry teardowns.",
                "relationship_specificity": "category_proxy",
                "themes": ["satellite_systems"],
                "evidence": [
                    "AMD's Xilinx Kintex/Virtex FPGAs are commonly used in satellite avionics for their radiation tolerance and reconfigurability; presence in SpaceX Starlink inferred from teardown reports.",
                ],
            },
            {
                "ticker": "SATS",
                "company_name": "EchoStar Corporation",
                "layer": 1,
                "supply_chain_role": "Satellite broadband infrastructure operator; competitive and ecosystem proxy to SpaceX Starlink",
                "bottleneck_score": 38,
                "evidence_grade": "C",
                "evidence_grade_reason": "EchoStar is a direct Starlink competitor in satellite broadband; included as a competitive dynamics proxy and satellite infrastructure bellwether.",
                "relationship_specificity": "strong_proxy",
                "themes": ["satellite_systems"],
                "evidence": [
                    "EchoStar operates satellite broadband networks that compete with and contextualise SpaceX Starlink's market opportunity.",
                ],
            },
        ],
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # ANTHROPIC — Anthropic
    # ═══════════════════════════════════════════════════════════════════════════
    "ANTHROPIC": {
        "anchor_name": "Anthropic",
        "nodes": [
            {
                "ticker": "AMZN",
                "company_name": "Amazon.com Inc.",
                "layer": 1,
                "supply_chain_role": "Primary cloud compute provider (AWS) for Anthropic model training and inference",
                "bottleneck_score": 98,
                "evidence_grade": "A",
                "evidence_grade_reason": "Amazon publicly committed up to $4B in investment in Anthropic; AWS is Anthropic's stated primary cloud.",
                "relationship_specificity": "confirmed_asset_partner",
                "themes": ["cloud_ai_infra", "ai_infrastructure"],
                "evidence": [
                    "Amazon committed up to $4 billion in investment in Anthropic and designated AWS as Anthropic's primary cloud provider for training and deployment.",
                    "Anthropic's Claude models run on AWS Trainium and Inferentia chips; Anthropic publishes on AWS Bedrock as a primary distribution channel.",
                ],
                "source_urls": ["https://www.anthropic.com/news/anthropic-amazon"],
            },
            {
                "ticker": "GOOGL",
                "company_name": "Alphabet Inc.",
                "layer": 1,
                "supply_chain_role": "Secondary cloud compute provider (GCP) for Anthropic AI model training via Google TPUs",
                "bottleneck_score": 90,
                "evidence_grade": "A",
                "evidence_grade_reason": "Google committed up to $2B in Anthropic; GCP TPU clusters are confirmed for Anthropic training runs.",
                "relationship_specificity": "confirmed_asset_partner",
                "themes": ["cloud_ai_infra", "ai_infrastructure"],
                "evidence": [
                    "Google committed up to $2 billion in Anthropic and provides GCP TPU infrastructure for model training.",
                    "Anthropic uses Google TPU v4/v5 pods for research training runs alongside AWS Trainium capacity.",
                ],
            },
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "layer": 2,
                "supply_chain_role": "GPU accelerators (H100/H200/GB200) for Anthropic model training on AWS and GCP infrastructure",
                "bottleneck_score": 95,
                "evidence_grade": "A",
                "evidence_grade_reason": "NVIDIA H100/H200 clusters are the primary GPU used on AWS and GCP for Anthropic large model training.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_infrastructure", "inference_hardware"],
                "evidence": [
                    "Anthropic trains Claude models on NVIDIA H100 GPU clusters provisioned through AWS and GCP; NVIDIA GPUs are the dominant compute platform for frontier AI training.",
                    "AWS's UltraCluster infrastructure for Anthropic uses NVIDIA H100 NVLink clusters with 3.2 Tbps intra-node bandwidth.",
                ],
            },
            {
                "ticker": "AVGO",
                "company_name": "Broadcom Inc.",
                "layer": 2,
                "supply_chain_role": "Custom AI accelerator (ASIC) development partner for Anthropic and Google TPU networking silicon",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "Broadcom is confirmed co-designer of Google's TPU ASICs and AWS Trainium custom silicon; Anthropic relies on both platforms.",
                "relationship_specificity": "critical_upstream",
                "themes": ["custom_silicon", "ai_infrastructure"],
                "evidence": [
                    "Broadcom co-designs Google's TPU custom ASICs and AWS Trainium networking interconnects; Anthropic's primary compute platforms both rely on Broadcom silicon.",
                    "Broadcom's Jericho switching fabric and custom SerDes are embedded in the AWS hyperscale network that carries Anthropic training traffic.",
                ],
            },
            {
                "ticker": "TSM",
                "company_name": "Taiwan Semiconductor Manufacturing Company",
                "layer": 3,
                "supply_chain_role": "Advanced node foundry for NVIDIA GPUs and Google/AWS custom AI ASICs used by Anthropic",
                "bottleneck_score": 92,
                "evidence_grade": "A",
                "evidence_grade_reason": "TSMC is sole-source foundry for NVIDIA H100/H200 and Google TPU v5; Anthropic's entire GPU/TPU compute supply depends on TSMC.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain", "ai_infrastructure"],
                "evidence": [
                    "TSMC manufactures 100% of NVIDIA's H100/H200 GPUs (4nm N4P) and Google TPU v5 (5nm); Anthropic's compute supply chain is entirely dependent on TSMC capacity.",
                ],
            },
            {
                "ticker": "MU",
                "company_name": "Micron Technology Inc.",
                "layer": 3,
                "supply_chain_role": "HBM3E and DRAM memory for NVIDIA GPUs and AWS Trainium used by Anthropic",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "Micron is a qualified HBM3E supplier for NVIDIA H200 and HBM for AWS Trainium; Anthropic's GPU memory supply depends on Micron.",
                "relationship_specificity": "critical_upstream",
                "themes": ["memory_hbm", "ai_infrastructure"],
                "evidence": [
                    "Micron supplies HBM3E for NVIDIA H200 GPUs used in Anthropic training runs; Micron is one of three qualified HBM suppliers globally.",
                    "AWS Trainium2 chips use HBM memory sourced from Micron and SK hynix; Anthropic's Trainium-based training clusters depend on this supply.",
                ],
            },
            {
                "ticker": "ASML",
                "company_name": "ASML Holding N.V.",
                "layer": 4,
                "supply_chain_role": "EUV lithography equipment enabling TSMC advanced node production of AI chips for Anthropic",
                "bottleneck_score": 88,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASML monopoly on EUV; every TSMC advanced node chip (NVIDIA H100, Google TPU) in Anthropic's compute supply chain flows through ASML equipment.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASML is the sole supplier of EUV lithography scanners; TSMC's 4nm/3nm nodes that produce NVIDIA H100/H200 and Google TPU v5 all depend on ASML EUV equipment.",
                ],
            },
            {
                "ticker": "MSFT",
                "company_name": "Microsoft Corporation",
                "layer": 1,
                "supply_chain_role": "Tertiary compute access via Azure (co-invested in OpenAI competitor infrastructure that Anthropic benchmarks against)",
                "bottleneck_score": 40,
                "evidence_grade": "C",
                "evidence_grade_reason": "Microsoft/Azure has no direct Anthropic relationship; included as competitor infrastructure context.",
                "relationship_specificity": "strong_proxy",
                "themes": ["cloud_ai_infra"],
                "evidence": [
                    "Microsoft Azure is the benchmark competitive infrastructure for Anthropic's AWS/GCP stack; Azure's AI capabilities define the market standard Anthropic Claude competes against.",
                ],
            },
            {
                "ticker": "CRWV",
                "company_name": "CoreWeave Inc.",
                "layer": 1,
                "supply_chain_role": "GPU cloud provider offering Anthropic additional H100/H200 GPU capacity outside AWS/GCP",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "CoreWeave is a confirmed buyer of NVIDIA GPU clusters and a cloud provider for AI labs; Anthropic is a known CoreWeave customer alongside other AI labs.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["neocloud", "ai_infrastructure"],
                "evidence": [
                    "CoreWeave provides supplemental H100/H200 GPU capacity to frontier AI labs including Anthropic for burst training and inference workloads.",
                ],
            },
            {
                "ticker": "AMAT",
                "company_name": "Applied Materials Inc.",
                "layer": 4,
                "supply_chain_role": "Deposition equipment for TSMC advanced node fabs producing AI chips used in Anthropic compute",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "AMAT is the largest semicap equipment company; structural upstream dependency for all TSMC advanced node production in Anthropic's supply chain.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Applied Materials deposition tools are integral to TSMC's 4nm/3nm processes used for NVIDIA H100 and Google TPU production serving Anthropic.",
                ],
            },
            {
                "ticker": "LRCX",
                "company_name": "Lam Research Corporation",
                "layer": 4,
                "supply_chain_role": "Etch systems for TSMC advanced node production of AI chips in Anthropic compute supply",
                "bottleneck_score": 70,
                "evidence_grade": "B",
                "evidence_grade_reason": "Lam Research dominant in etch; structural dependency for TSMC N4/N3 fabs producing Anthropic's GPU/TPU supply.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Lam Research etch systems are essential to TSMC N4P/N3 node patterning; every H100 GPU and Google TPU chip depends on Lam equipment.",
                ],
            },
            {
                "ticker": "KLAC",
                "company_name": "KLA Corporation",
                "layer": 4,
                "supply_chain_role": "Process control and wafer inspection for TSMC fabs producing AI chips for Anthropic",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "KLA holds ~50% global process control share; essential for yield in all TSMC advanced node production serving Anthropic.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "KLA's inspection tools enable TSMC yield management at N4/N3 nodes; every GPU and TPU in Anthropic's compute stack depends on KLA yield control.",
                ],
            },
            {
                "ticker": "ASMI",
                "company_name": "ASM International N.V.",
                "tradingview_symbol": "ASMI",
                "layer": 4,
                "supply_chain_role": "ALD (atomic layer deposition) systems for TSMC gate-all-around and advanced node production",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "ASMI holds ~80% share of standalone ALD equipment; critical for high-k dielectric deposition at TSMC advanced nodes.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASM International's ALD equipment is used in TSMC's N3/N2 node high-k metal gate structures; structural upstream dependency for all advanced AI chip production.",
                ],
            },
            {
                "ticker": "BESI",
                "company_name": "BE Semiconductor Industries N.V.",
                "tradingview_symbol": "BESI",
                "layer": 3,
                "supply_chain_role": "Advanced die attach and hybrid bonding equipment for AI GPU packaging",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "BESI holds dominant share of die-attach and hybrid bonding tools; critical for CoWoS/SoIC packaging of NVIDIA H100 at TSMC.",
                "relationship_specificity": "critical_upstream",
                "themes": ["advanced_packaging_test", "ai_infrastructure"],
                "evidence": [
                    "BE Semiconductor's Datacon die-attach and hybrid bonding equipment is used in TSMC CoWoS packaging for NVIDIA H100 GPUs; BESI's tools are the market standard for 3D IC bonding.",
                    "BESI's hybrid bonding tools achieve sub-2μm placement accuracy required for advanced AI chip stacking configurations.",
                ],
            },
            {
                "ticker": "TER",
                "company_name": "Teradyne Inc.",
                "layer": 3,
                "supply_chain_role": "Semiconductor test equipment for NVIDIA GPUs and memory chips in Anthropic compute supply",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "Teradyne is the dominant ATE supplier; every GPU and HBM module shipped to AI data centers goes through Teradyne test systems.",
                "relationship_specificity": "critical_upstream",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "Teradyne's MAGNUM and Ultrapoint ATE systems test NVIDIA GPUs and HBM chips before shipment; Anthropic's compute hardware depends on Teradyne quality gates.",
                ],
            },
            {
                "ticker": "GLW",
                "company_name": "Corning Incorporated",
                "layer": 3,
                "supply_chain_role": "Optical fiber and data center fiber connectivity for Anthropic AI training cluster networking",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "Corning is the dominant optical fiber manufacturer; AWS and GCP AI data centers use Corning fiber for inter-rack and campus connectivity.",
                "relationship_specificity": "critical_upstream",
                "themes": ["photonics_cpo", "data_center_infrastructure"],
                "evidence": [
                    "Corning supplies optical fiber and fiber assemblies to AWS and GCP data centers that house Anthropic's training infrastructure.",
                    "Corning holds ~50% global optical fiber market share; AI data center fiber density demand has created a structural supply constraint.",
                ],
            },
            {
                "ticker": "VRT",
                "company_name": "Vertiv Holdings Co.",
                "layer": 2,
                "supply_chain_role": "Data center power and cooling infrastructure for AWS and GCP AI training facilities serving Anthropic",
                "bottleneck_score": 78,
                "evidence_grade": "A",
                "evidence_grade_reason": "Vertiv supplies liquid cooling and power distribution to AWS and GCP AI data centers; Anthropic training clusters are direct end consumers.",
                "relationship_specificity": "critical_upstream",
                "themes": ["cooling_thermal", "data_center_infrastructure", "ai_power_energy"],
                "evidence": [
                    "Vertiv is the leading supplier of data center power and cooling infrastructure to AWS, Google, and other hyperscalers that host Anthropic's AI compute.",
                    "Vertiv's liquid cooling systems are required for high-density GPU clusters; the H100 GPU's 700W TDP mandates liquid cooling infrastructure.",
                ],
            },
            {
                "ticker": "ANET",
                "company_name": "Arista Networks Inc.",
                "layer": 2,
                "supply_chain_role": "Ethernet networking switching for AI training clusters in AWS and GCP used by Anthropic",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Arista is named as a primary spine-leaf switching vendor for AWS and GCP hyperscale AI networking fabrics.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["data_center_infrastructure", "ai_infrastructure"],
                "evidence": [
                    "Arista Networks provides 400G/800G Ethernet switches for the AWS and GCP data center spine-leaf networks that interconnect Anthropic's AI training GPU clusters.",
                ],
            },
            {
                "ticker": "MRVL",
                "company_name": "Marvell Technology Inc.",
                "layer": 2,
                "supply_chain_role": "Custom AI ASIC and networking silicon for AWS Trainium and cloud AI infrastructure",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Marvell is disclosed as co-designer of AWS Trainium networking silicon and custom AI infrastructure chips.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["custom_silicon", "ai_infrastructure"],
                "evidence": [
                    "Marvell co-develops custom networking ASICs for AWS's Trainium2 infrastructure; Anthropic's primary training platform (AWS Trainium) relies on Marvell interconnect silicon.",
                    "Marvell's electro-optic DSP chips enable the 800G optical networking in AWS AI data centers.",
                ],
            },
            {
                "ticker": "ETN",
                "company_name": "Eaton Corporation plc",
                "layer": 3,
                "supply_chain_role": "Power distribution and UPS systems for AWS and GCP AI data centers running Anthropic",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Eaton is a leading power management supplier to AWS and GCP; AI cluster power density has driven record Eaton data center order intake.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_power_energy", "data_center_infrastructure"],
                "evidence": [
                    "Eaton's power distribution units and UPS systems are deployed in AWS and GCP data centers running Anthropic AI training workloads.",
                ],
            },
            {
                "ticker": "HUBB",
                "company_name": "Hubbell Incorporated",
                "layer": 3,
                "supply_chain_role": "Electrical distribution components and switchgear for AI data center power infrastructure",
                "bottleneck_score": 58,
                "evidence_grade": "B",
                "evidence_grade_reason": "Hubbell electrical components are widely deployed in hyperscale data center power distribution for AWS and GCP facilities.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_power_energy", "data_center_infrastructure"],
                "evidence": [
                    "Hubbell supplies busway, connectors, and electrical distribution products to AWS and GCP hyperscale data centers hosting Anthropic compute.",
                ],
            },
            {
                "ticker": "POWL",
                "company_name": "Powell Industries Inc.",
                "layer": 3,
                "supply_chain_role": "Custom switchgear and power control systems for AI data center substations",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "Powell Industries builds custom medium-voltage switchgear for large industrial and data center power distribution; AI data center demand has driven backlog records.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_power_energy", "grid_transformers"],
                "evidence": [
                    "Powell Industries custom switchgear is used in the medium-voltage power distribution infrastructure for large-scale AI data centers built by AWS and GCP.",
                ],
            },
            {
                "ticker": "WULF",
                "company_name": "TeraWulf Inc.",
                "layer": 3,
                "supply_chain_role": "Low-carbon power infrastructure for AI data center energy sourcing near Anthropic compute nodes",
                "bottleneck_score": 45,
                "evidence_grade": "C",
                "evidence_grade_reason": "TeraWulf operates nuclear-powered data center capacity adjacent to AI compute demand; included as carbon-free power infrastructure proxy.",
                "relationship_specificity": "strong_proxy",
                "themes": ["nuclear_uranium_smr", "ai_power_energy"],
                "evidence": [
                    "TeraWulf operates nuclear-powered computing infrastructure at the Susquehanna nuclear plant; nuclear-adjacent clean power capacity is in demand for AI data center decarbonization.",
                ],
            },
            {
                "ticker": "CIFR",
                "company_name": "Cipher Mining Inc.",
                "layer": 3,
                "supply_chain_role": "Large-scale power infrastructure and data center capacity adjacent to AI compute demand",
                "bottleneck_score": 40,
                "evidence_grade": "C",
                "evidence_grade_reason": "Cipher Mining is pivoting to HPC/AI hosting; included as early-stage AI power infrastructure proxy.",
                "relationship_specificity": "strong_proxy",
                "themes": ["ai_power_energy"],
                "evidence": [
                    "Cipher Mining is transitioning large-scale power-contracted data center campuses to AI/HPC workloads; power access at scale is the scarcest AI infrastructure resource.",
                ],
            },
        ],
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # NVDA — NVIDIA
    # ═══════════════════════════════════════════════════════════════════════════
    "NVDA": {
        "anchor_name": "NVIDIA",
        "nodes": [
            {
                "ticker": "TSM",
                "company_name": "Taiwan Semiconductor Manufacturing Company",
                "layer": 3,
                "supply_chain_role": "Sole-source advanced node foundry for all NVIDIA GPU and AI accelerator production",
                "bottleneck_score": 98,
                "evidence_grade": "A",
                "evidence_grade_reason": "TSMC manufactures 100% of NVIDIA's H100/H200/GB200 at N4P/N3; confirmed in NVIDIA and TSMC filings.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain", "ai_infrastructure"],
                "evidence": [
                    "NVIDIA relies exclusively on TSMC for advanced node GPU production; H100 is manufactured at TSMC N4P, H200 and GB200 at TSMC N3/N4.",
                    "TSMC's CoWoS advanced packaging is used for all NVIDIA HBM integration in AI datacenter GPUs.",
                ],
            },
            {
                "ticker": "MU",
                "company_name": "Micron Technology Inc.",
                "layer": 3,
                "supply_chain_role": "HBM3E memory supply for NVIDIA H200 and Blackwell GPU modules",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "Micron is a confirmed HBM3E supplier for NVIDIA H200; Micron's 2024 investor day confirmed NVIDIA as a major HBM customer.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["memory_hbm", "ai_infrastructure"],
                "evidence": [
                    "Micron supplies HBM3E for NVIDIA H200 GPUs; Micron disclosed NVIDIA as a key HBM3E qualification partner in its fiscal 2024 investor presentations.",
                    "Micron's HBM3E achieves 9.2 Gbps data rate meeting NVIDIA's GPU bandwidth requirements.",
                ],
                "source_urls": ["https://investor.micron.com/events/event-details"],
            },
            {
                "ticker": "ASML",
                "company_name": "ASML Holding N.V.",
                "layer": 4,
                "supply_chain_role": "EUV lithography monopoly enabling TSMC production of all NVIDIA advanced GPUs",
                "bottleneck_score": 92,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASML sole-source EUV; every NVIDIA H100/H200/GB200 chip depends entirely on ASML scanners at TSMC.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASML EUV scanners are the sole lithography tool capable of printing NVIDIA's sub-5nm GPU architecture at TSMC; no alternative exists.",
                    "NVIDIA GPU production is gated by TSMC's EUV scanner capacity, which is in turn gated by ASML's output of ~60 EUV systems per year.",
                ],
            },
            {
                "ticker": "AMAT",
                "company_name": "Applied Materials Inc.",
                "layer": 4,
                "supply_chain_role": "CVD/PVD deposition equipment for TSMC N4/N3 production of NVIDIA GPUs",
                "bottleneck_score": 78,
                "evidence_grade": "A",
                "evidence_grade_reason": "AMAT is largest semicap tool maker; NVIDIA GPU production at TSMC N4 requires AMAT deposition, etch, and implant systems at every critical step.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Applied Materials provides the majority of TSMC's deposition, etch, and implant equipment; NVIDIA GPU yields at N4/N3 are directly tied to AMAT tool uptime.",
                ],
            },
            {
                "ticker": "LRCX",
                "company_name": "Lam Research Corporation",
                "layer": 4,
                "supply_chain_role": "Etch equipment for TSMC advanced node production of NVIDIA AI GPUs",
                "bottleneck_score": 76,
                "evidence_grade": "A",
                "evidence_grade_reason": "Lam Research dominant etch equipment market share at TSMC; critical for NVIDIA GPU patterning at N4/N3.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Lam Research's etch systems perform the critical patterning steps in NVIDIA's H100/H200 GPU die at TSMC N4P; Lam holds ~45% of advanced etch market share.",
                ],
            },
            {
                "ticker": "KLAC",
                "company_name": "KLA Corporation",
                "layer": 4,
                "supply_chain_role": "Wafer inspection and process control for TSMC NVIDIA GPU yield management",
                "bottleneck_score": 74,
                "evidence_grade": "A",
                "evidence_grade_reason": "KLA ~50% global process control share; NVIDIA GPU yield at TSMC N4/N3 directly depends on KLA inspection quality gates.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "KLA's patterned wafer inspection and defect review tools are deployed at TSMC N4/N3 to maximize NVIDIA GPU yield; KLA equipment approval is required for each technology node.",
                ],
            },
            {
                "ticker": "AMKR",
                "company_name": "Amkor Technology Inc.",
                "layer": 2,
                "supply_chain_role": "Advanced semiconductor packaging (CoWoS substrate and fan-out) for NVIDIA GPU modules",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Amkor is a confirmed packaging partner for NVIDIA GPU CoWoS substrate assembly and advanced fan-out packaging.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "Amkor Technology provides advanced packaging services for NVIDIA GPU CoWoS integration, including substrate interposer assembly for H100/H200.",
                    "Amkor's TSMC-qualified CoWoS packaging line is part of the critical path for NVIDIA GPU production capacity.",
                ],
            },
            {
                "ticker": "ASX",
                "company_name": "ASE Technology Holding Co., Ltd.",
                "tradingview_symbol": "ASX",
                "layer": 2,
                "supply_chain_role": "OSAT (outsourced semiconductor assembly and test) for NVIDIA GPU packaging and test",
                "bottleneck_score": 68,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASE (ASE/SPIL post-merger) is NVIDIA's primary OSAT partner for lower-layer GPU packaging and flip-chip assembly.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "ASE Technology is a primary OSAT for NVIDIA GPU packaging; ASE's substrate assembly and flip-chip capabilities are qualified for NVIDIA production.",
                ],
            },
            {
                "ticker": "COHR",
                "company_name": "Coherent Corp.",
                "layer": 2,
                "supply_chain_role": "Optical transceivers for NVIDIA GPU cluster InfiniBand and Ethernet networking",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Coherent is a primary supplier of 400G/800G optical transceivers used in NVIDIA InfiniBand networks for DGX clusters and NVLink switching.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo", "ai_infrastructure"],
                "evidence": [
                    "Coherent supplies 400G/800G optical transceivers for the InfiniBand fabric connecting NVIDIA H100 DGX clusters in AI training data centers.",
                    "Coherent's EMLs (electro-absorption modulated lasers) are specified in NVIDIA Quantum-2 InfiniBand switch port interfaces.",
                ],
            },
            {
                "ticker": "LITE",
                "company_name": "Lumentum Holdings Inc.",
                "layer": 2,
                "supply_chain_role": "High-speed laser components and transceivers for NVIDIA AI networking infrastructure",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "Lumentum supplies EML lasers and 400G+ transceivers used in NVIDIA InfiniBand and co-packaged optics for future GPU networking.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo"],
                "evidence": [
                    "Lumentum's VCSEL and EML laser chips power the optical transceiver components used in NVIDIA InfiniBand HDR/NDR switch fabrics.",
                ],
            },
            {
                "ticker": "FN",
                "company_name": "Fabrinet",
                "layer": 2,
                "supply_chain_role": "Contract manufacturing of optical transceivers for NVIDIA AI cluster networking",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "Fabrinet manufactures Coherent and Lumentum optical transceivers at scale; directly in the NVIDIA AI networking supply chain.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo", "ai_infrastructure"],
                "evidence": [
                    "Fabrinet is the primary contract manufacturer for Coherent's 400G/800G AI data center optical transceivers used in NVIDIA DGX cluster networking.",
                    "Fabrinet's Thailand facilities produce the majority of high-speed optical transceivers deployed in NVIDIA InfiniBand fabrics.",
                ],
            },
            {
                "ticker": "GLW",
                "company_name": "Corning Incorporated",
                "layer": 3,
                "supply_chain_role": "Optical fiber supply for NVIDIA AI data center and HPC interconnect infrastructure",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Corning is the dominant global optical fiber manufacturer; NVIDIA GPU cluster deployments drive fiber demand.",
                "relationship_specificity": "critical_upstream",
                "themes": ["photonics_cpo", "data_center_infrastructure"],
                "evidence": [
                    "Corning supplies optical fiber cable and preforms to data center networking suppliers in the NVIDIA GPU cluster interconnect supply chain.",
                ],
            },
            {
                "ticker": "VRT",
                "company_name": "Vertiv Holdings Co.",
                "layer": 2,
                "supply_chain_role": "Liquid cooling and power management for NVIDIA GPU data centers",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "Vertiv is explicitly cited in NVIDIA DGX BasePOD design documentation as the cooling infrastructure partner; critical for H100/H200 thermal management.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["cooling_thermal", "ai_power_energy"],
                "evidence": [
                    "Vertiv is the thermal management partner referenced in NVIDIA's DGX BasePOD and SuperPOD deployment guides; NVIDIA recommends Vertiv cooling for all DGX deployments.",
                    "NVIDIA's H100 GPU draws up to 700W; air-cooled deployments are impractical above 8 GPUs per rack without Vertiv direct liquid cooling solutions.",
                ],
                "source_urls": ["https://www.nvidia.com/en-us/data-center/dgx-basepod/"],
            },
            {
                "ticker": "ETN",
                "company_name": "Eaton Corporation plc",
                "layer": 3,
                "supply_chain_role": "Power distribution and UPS for NVIDIA GPU data centers and DGX deployments",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Eaton is a leading power management supplier to AI data centers; DGX SuperPOD deployments require Eaton-class power infrastructure.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_power_energy", "data_center_infrastructure"],
                "evidence": [
                    "Eaton power distribution and UPS systems are deployed in NVIDIA DGX SuperPOD reference architecture data centers.",
                ],
            },
            {
                "ticker": "ASMI",
                "company_name": "ASM International N.V.",
                "tradingview_symbol": "ASMI",
                "layer": 4,
                "supply_chain_role": "ALD equipment for TSMC gate dielectric and barrier layer deposition in NVIDIA GPU production",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "ASMI ~80% global ALD share; critical for NVIDIA GPU high-k metal gate formation at TSMC N4/N3.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASM International ALD tools form the high-k gate dielectric in every NVIDIA GPU manufactured at TSMC advanced nodes.",
                ],
            },
            {
                "ticker": "ONTO",
                "company_name": "Onto Innovation Inc.",
                "layer": 3,
                "supply_chain_role": "Advanced optical metrology and inspection for NVIDIA GPU CoWoS packaging at TSMC",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "Onto Innovation provides packaging-level metrology critical for TSMC CoWoS integration of NVIDIA GPUs with HBM.",
                "relationship_specificity": "critical_upstream",
                "themes": ["advanced_packaging_test", "semicap_supply_chain"],
                "evidence": [
                    "Onto Innovation's Dragonfly inspection system is used at TSMC CoWoS packaging lines to inspect NVIDIA GPU die-to-substrate alignment and bonding quality.",
                ],
            },
            {
                "ticker": "BESI",
                "company_name": "BE Semiconductor Industries N.V.",
                "tradingview_symbol": "BESI",
                "layer": 3,
                "supply_chain_role": "Die attach and hybrid bonding equipment for NVIDIA GPU CoWoS packaging",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "BESI dominates hybrid bonding tools; used in TSMC CoWoS packaging for all NVIDIA H100/H200 GPUs.",
                "relationship_specificity": "critical_upstream",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "BE Semiconductor hybrid bonding tools are used in TSMC CoWoS and SoIC packaging for NVIDIA H100/H200 GPU + HBM integration.",
                ],
            },
            {
                "ticker": "TOELY",
                "company_name": "Tokyo Electron Limited",
                "tradingview_symbol": "TOELY",
                "layer": 4,
                "supply_chain_role": "Coater/developer and etch systems for TSMC production of NVIDIA AI GPUs",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Tokyo Electron is the #2 semicap equipment company globally; its coater/developer and thermal processing tools are indispensable at TSMC N4/N3.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Tokyo Electron's CLEAN TRACK coater/developer and oxidation/diffusion systems are used at TSMC N4/N3 fabs producing NVIDIA GPUs.",
                    "TEL is the sole supplier of several critical process steps in TSMC's EUV lithography integration flow.",
                ],
            },
        ],
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # OPENAI — OpenAI
    # ═══════════════════════════════════════════════════════════════════════════
    "OPENAI": {
        "anchor_name": "OpenAI",
        "nodes": [
            {
                "ticker": "MSFT",
                "company_name": "Microsoft Corporation",
                "layer": 1,
                "supply_chain_role": "Primary cloud compute provider (Azure) for OpenAI model training and ChatGPT inference",
                "bottleneck_score": 98,
                "evidence_grade": "A",
                "evidence_grade_reason": "Microsoft has invested $13B+ in OpenAI; Azure is OpenAI's sole cloud provider per formal partnership disclosures.",
                "relationship_specificity": "confirmed_asset_partner",
                "themes": ["cloud_ai_infra", "ai_infrastructure"],
                "evidence": [
                    "Microsoft has invested over $13 billion in OpenAI and designated Azure as OpenAI's exclusive cloud compute provider.",
                    "OpenAI trains GPT-4 and o-series models on dedicated Azure GPU clusters; ChatGPT inference runs entirely on Azure.",
                ],
            },
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "layer": 2,
                "supply_chain_role": "H100/H200/GB200 GPU accelerators for OpenAI model training on Azure",
                "bottleneck_score": 97,
                "evidence_grade": "A",
                "evidence_grade_reason": "OpenAI uses NVIDIA H100/H200 clusters exclusively for frontier model training; confirmed in OpenAI and Microsoft disclosures.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["ai_infrastructure", "inference_hardware"],
                "evidence": [
                    "OpenAI's GPT-4 training used a cluster of 25,000+ NVIDIA A100/H100 GPUs on Azure; o-series model training uses H100/H200 clusters.",
                    "Microsoft's Azure announced 100,000+ H100 GPU deployment in 2024, the majority dedicated to OpenAI workloads.",
                ],
            },
            {
                "ticker": "TSM",
                "company_name": "Taiwan Semiconductor Manufacturing Company",
                "layer": 3,
                "supply_chain_role": "Advanced node foundry for NVIDIA GPUs and Microsoft/OpenAI custom AI chips",
                "bottleneck_score": 95,
                "evidence_grade": "A",
                "evidence_grade_reason": "TSMC sole-source for NVIDIA H100/H200 at N4P; Microsoft Maia 100 AI chip also manufactured at TSMC.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain", "ai_infrastructure"],
                "evidence": [
                    "TSMC manufactures NVIDIA H100/H200 (N4P) used in OpenAI clusters and Microsoft Maia 100 custom AI chip (N5); all OpenAI compute is TSMC-dependent.",
                ],
            },
            {
                "ticker": "AVGO",
                "company_name": "Broadcom Inc.",
                "layer": 2,
                "supply_chain_role": "Custom AI ASIC (Google TPU partner, AWS Trainium) and networking silicon for AI infrastructure",
                "bottleneck_score": 82,
                "evidence_grade": "A",
                "evidence_grade_reason": "Broadcom is co-designing OpenAI's custom AI chip (Project Stargate); disclosed as OpenAI's ASIC development partner.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["custom_silicon", "ai_infrastructure"],
                "evidence": [
                    "Broadcom is disclosed as co-designer of OpenAI's proprietary AI training chip under the Stargate Project; Broadcom's ASIC design division leads the silicon architecture.",
                    "Broadcom's networking ASICs and Tomahawk switches provide the data center fabric for OpenAI's Azure-based GPU clusters.",
                ],
            },
            {
                "ticker": "ORCL",
                "company_name": "Oracle Corporation",
                "layer": 1,
                "supply_chain_role": "Co-investor and cloud infrastructure partner for OpenAI Stargate Project ($500B AI initiative)",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "Oracle is a named Stargate Project partner alongside Microsoft, SoftBank, and OpenAI; Oracle Cloud provides dedicated GPU capacity.",
                "relationship_specificity": "confirmed_asset_partner",
                "themes": ["cloud_ai_infra", "neocloud"],
                "evidence": [
                    "Oracle is a founding partner of the $500B Stargate Project with OpenAI and SoftBank; Oracle Cloud provides additional GPU cluster capacity for OpenAI.",
                    "Oracle disclosed a 131,000 H100 GPU cluster in fiscal Q4 2024 partly allocated to OpenAI Stargate commitments.",
                ],
            },
            {
                "ticker": "AMD",
                "company_name": "Advanced Micro Devices Inc.",
                "layer": 2,
                "supply_chain_role": "MI300X AI accelerators as alternative GPU capacity on Azure for OpenAI inference workloads",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Microsoft Azure deployed AMD MI300X GPUs; OpenAI inference workloads run on both NVIDIA and AMD Azure capacity.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["inference_hardware", "ai_infrastructure"],
                "evidence": [
                    "Microsoft Azure deployed AMD MI300X accelerators for AI inference; OpenAI's ChatGPT inference traffic is handled partly on AMD hardware.",
                ],
            },
            {
                "ticker": "CRWV",
                "company_name": "CoreWeave Inc.",
                "layer": 1,
                "supply_chain_role": "GPU cloud provider and infrastructure partner for OpenAI under Stargate Project",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "CoreWeave is a named Stargate Project infrastructure partner; OpenAI has signed multi-year GPU compute contracts with CoreWeave.",
                "relationship_specificity": "confirmed_asset_partner",
                "themes": ["neocloud", "ai_infrastructure"],
                "evidence": [
                    "CoreWeave is a confirmed Stargate Project infrastructure provider; OpenAI signed a $11.9B multi-year cloud services contract with CoreWeave.",
                    "CoreWeave's NVIDIA H100/H200 clusters are a core component of OpenAI's non-Azure GPU compute supply.",
                ],
            },
            {
                "ticker": "AMAT",
                "company_name": "Applied Materials Inc.",
                "layer": 4,
                "supply_chain_role": "Deposition equipment for TSMC advanced node production of OpenAI compute chips",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "AMAT is the dominant semicap equipment supplier; structural upstream for all TSMC N4/N3 production of OpenAI's compute supply.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Applied Materials provides the majority of TSMC deposition and etch equipment used to manufacture NVIDIA GPUs and Microsoft Maia chips for OpenAI.",
                ],
            },
            {
                "ticker": "ASML",
                "company_name": "ASML Holding N.V.",
                "layer": 4,
                "supply_chain_role": "EUV lithography for TSMC advanced node production of all OpenAI AI chips",
                "bottleneck_score": 90,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASML monopoly on EUV; every chip in OpenAI's compute supply (NVIDIA H100, Maia 100, OpenAI custom ASIC) depends on ASML EUV.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASML EUV scanners are the sole lithography tool for TSMC N4/N3; every GPU and custom chip in OpenAI's compute supply chain requires ASML equipment.",
                ],
            },
            {
                "ticker": "GEV",
                "company_name": "GE Vernova Inc.",
                "layer": 3,
                "supply_chain_role": "Gas turbines and power generation equipment for Stargate data center power supply",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "GE Vernova gas turbines are being procured for Stargate data center backup and primary power generation; referenced in Stargate energy sourcing discussions.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["ai_power_energy", "grid_transformers"],
                "evidence": [
                    "GE Vernova gas turbines were cited in Stargate Project power generation plans for emergency and primary power supply to OpenAI data centers.",
                    "GEV's HA gas turbines can provide 400MW+ per installation, matching the scale of Stargate's projected power demand.",
                ],
            },
            {
                "ticker": "VRT",
                "company_name": "Vertiv Holdings Co.",
                "layer": 2,
                "supply_chain_role": "Liquid cooling and power distribution for OpenAI GPU clusters on Azure and CoreWeave",
                "bottleneck_score": 82,
                "evidence_grade": "A",
                "evidence_grade_reason": "Vertiv is the primary data center thermal management supplier; cited in Azure and CoreWeave DGX deployment guides for OpenAI GPU clusters.",
                "relationship_specificity": "critical_upstream",
                "themes": ["cooling_thermal", "ai_power_energy"],
                "evidence": [
                    "Vertiv liquid cooling systems are required for the NVIDIA H100/H200 GPU density in OpenAI Azure and CoreWeave clusters.",
                    "Vertiv's Liebert liquid cooling and Geist power distribution are standard in 400kW+ per rack GPU deployments.",
                ],
            },
            {
                "ticker": "MU",
                "company_name": "Micron Technology Inc.",
                "layer": 3,
                "supply_chain_role": "HBM3E and GDDR7 memory for NVIDIA GPUs running OpenAI model training and inference",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "Micron is a qualified HBM3E supplier for NVIDIA H200 and a major DRAM supplier to the AI data center market.",
                "relationship_specificity": "critical_upstream",
                "themes": ["memory_hbm"],
                "evidence": [
                    "Micron HBM3E memory is integrated into NVIDIA H200 GPUs used in OpenAI training clusters; Micron disclosed OpenAI-adjacent customer qualifications.",
                ],
            },
            {
                "ticker": "LRCX",
                "company_name": "Lam Research Corporation",
                "layer": 4,
                "supply_chain_role": "Etch systems for TSMC advanced node production of OpenAI AI chips",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "Lam dominant etch share; structural upstream for TSMC N4/N3 production serving OpenAI compute supply.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Lam Research etch systems are used in every TSMC advanced node; OpenAI's GPU supply depends on Lam equipment uptime at TSMC.",
                ],
            },
            {
                "ticker": "KLAC",
                "company_name": "KLA Corporation",
                "layer": 4,
                "supply_chain_role": "Process control and inspection for TSMC advanced node production of OpenAI chips",
                "bottleneck_score": 70,
                "evidence_grade": "B",
                "evidence_grade_reason": "KLA ~50% process control share; yield management for all TSMC advanced AI chip production serving OpenAI.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "KLA inspection tools are deployed at TSMC N4/N3 to manage GPU yield; OpenAI compute supply is directly dependent on KLA-enabled TSMC yields.",
                ],
            },
            {
                "ticker": "ETN",
                "company_name": "Eaton Corporation plc",
                "layer": 3,
                "supply_chain_role": "Power distribution infrastructure for OpenAI Stargate and Azure AI data centers",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Eaton is a primary power distribution supplier to hyperscale AI data centers; Stargate Project scale requires Eaton-class electrical infrastructure.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_power_energy", "data_center_infrastructure"],
                "evidence": [
                    "Eaton power distribution and UPS systems are deployed in Microsoft Azure and CoreWeave data centers running OpenAI workloads.",
                ],
            },
            {
                "ticker": "PWR",
                "company_name": "Quanta Services Inc.",
                "layer": 2,
                "supply_chain_role": "Electrical infrastructure construction and data center power connectivity for Stargate Project",
                "bottleneck_score": 70,
                "evidence_grade": "B",
                "evidence_grade_reason": "Quanta Services is a leading EPC contractor for large-scale electrical infrastructure; Stargate's grid connectivity requirements favor Quanta.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["ai_power_energy", "grid_transformers"],
                "evidence": [
                    "Quanta Services is contracted for power transmission and substation construction projects that support Stargate and Azure AI data center grid interconnections.",
                ],
            },
            {
                "ticker": "ANET",
                "company_name": "Arista Networks Inc.",
                "layer": 2,
                "supply_chain_role": "Ethernet switching infrastructure for OpenAI GPU cluster scale-out networking on Azure",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Arista is the primary spine-leaf Ethernet switch supplier to Microsoft Azure AI data centers running OpenAI workloads.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["data_center_infrastructure", "ai_infrastructure"],
                "evidence": [
                    "Arista 7500R and 7800R switches form the spine-leaf fabric in Microsoft Azure data centers running OpenAI GPU clusters.",
                    "Azure's AI networking announcement confirmed Arista as the primary 400G Ethernet switching vendor for AI interconnect.",
                ],
            },
            {
                "ticker": "MRVL",
                "company_name": "Marvell Technology Inc.",
                "layer": 2,
                "supply_chain_role": "Custom AI ASIC and networking silicon for Microsoft Azure infrastructure serving OpenAI",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Marvell co-designs Microsoft Azure networking ASICs; Azure's AI infrastructure for OpenAI relies on Marvell electro-optic silicon.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["custom_silicon", "ai_infrastructure"],
                "evidence": [
                    "Marvell designs custom networking ASICs for Microsoft Azure's Sonic switching infrastructure; OpenAI's inference traffic flows through Marvell-powered Azure networking.",
                ],
            },
            {
                "ticker": "ARM",
                "company_name": "Arm Holdings plc",
                "tradingview_symbol": "ARM",
                "layer": 3,
                "supply_chain_role": "CPU architecture licensing for AI inference chips and cloud server processors in OpenAI infrastructure",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "Arm architecture licenses underpin Microsoft Azure Cobalt and NVIDIA Grace processors used in OpenAI deployments.",
                "relationship_specificity": "critical_upstream",
                "themes": ["custom_silicon", "inference_hardware"],
                "evidence": [
                    "Microsoft Azure Cobalt 100 CPU (Arm v9 architecture) is deployed in OpenAI inference clusters; NVIDIA Grace-Hopper also uses Arm cores.",
                    "Arm's architecture licenses are embedded in the CPU side of every custom AI chip serving OpenAI inference.",
                ],
            },
            {
                "ticker": "APH",
                "company_name": "Amphenol Corporation",
                "layer": 3,
                "supply_chain_role": "High-speed connectors and cable assemblies for GPU server backplanes in OpenAI data centers",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "Amphenol is the largest connector company; GPU server backplanes use Amphenol cable assemblies for PCIe and power connectivity.",
                "relationship_specificity": "critical_upstream",
                "themes": ["data_center_infrastructure", "ai_infrastructure"],
                "evidence": [
                    "Amphenol connectors are used in GPU server backplanes and cable assemblies for NVIDIA DGX H100 systems deployed in OpenAI data centers.",
                ],
            },
            {
                "ticker": "COHR",
                "company_name": "Coherent Corp.",
                "layer": 2,
                "supply_chain_role": "800G optical transceivers for OpenAI GPU cluster scale-out networking",
                "bottleneck_score": 73,
                "evidence_grade": "A",
                "evidence_grade_reason": "Coherent supplies 800G transceivers to Azure and CoreWeave AI networking for OpenAI GPU clusters.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo"],
                "evidence": [
                    "Coherent 800G DR8 optical transceivers are deployed in Microsoft Azure and CoreWeave AI data center interconnects serving OpenAI.",
                ],
            },
            {
                "ticker": "SMCI",
                "company_name": "Super Micro Computer Inc.",
                "layer": 2,
                "supply_chain_role": "GPU server systems and rack-scale systems for OpenAI GPU cluster deployments",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "SMCI is a primary GPU server ODM; CoreWeave and other OpenAI cloud providers source NVIDIA DGX-equivalent systems from SMCI.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["ai_infrastructure", "data_center_infrastructure"],
                "evidence": [
                    "Super Micro Computer builds NVIDIA HGX H100/H200 GPU server systems used by CoreWeave and other OpenAI infrastructure providers.",
                    "SMCI's liquid-cooled GPU server configurations are specified in OpenAI Stargate infrastructure plans.",
                ],
            },
            {
                "ticker": "DELL",
                "company_name": "Dell Technologies Inc.",
                "layer": 2,
                "supply_chain_role": "PowerEdge GPU server systems for OpenAI Azure-based GPU cluster deployments",
                "bottleneck_score": 62,
                "evidence_grade": "B",
                "evidence_grade_reason": "Dell is a primary server ODM for Microsoft Azure GPU deployments running OpenAI workloads.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["data_center_infrastructure"],
                "evidence": [
                    "Dell PowerEdge XE9680 GPU servers are deployed in Microsoft Azure for OpenAI training and inference workloads.",
                ],
            },
            {
                "ticker": "FN",
                "company_name": "Fabrinet",
                "layer": 2,
                "supply_chain_role": "Contract manufacturing of optical transceivers for OpenAI AI cluster networking",
                "bottleneck_score": 68,
                "evidence_grade": "A",
                "evidence_grade_reason": "Fabrinet is the primary contract manufacturer for Coherent optical transceivers used in OpenAI GPU cluster networking.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo", "ai_infrastructure"],
                "evidence": [
                    "Fabrinet manufactures optical transceivers for Coherent and other suppliers in the NVIDIA GPU cluster networking supply chain for OpenAI.",
                ],
            },
            {
                "ticker": "AMKR",
                "company_name": "Amkor Technology Inc.",
                "layer": 3,
                "supply_chain_role": "Advanced packaging for NVIDIA GPUs and Microsoft custom AI chips used in OpenAI compute",
                "bottleneck_score": 68,
                "evidence_grade": "A",
                "evidence_grade_reason": "Amkor packages NVIDIA GPUs using CoWoS and is a TSMC packaging partner for Microsoft Maia AI chips.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "Amkor Technology provides advanced packaging services for NVIDIA GPU and Microsoft Maia chip production in the OpenAI compute supply chain.",
                ],
            },
        ],
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # TSM — TSMC
    # ═══════════════════════════════════════════════════════════════════════════
    "TSM": {
        "anchor_name": "TSMC",
        "nodes": [
            {
                "ticker": "ASML",
                "company_name": "ASML Holding N.V.",
                "layer": 2,
                "supply_chain_role": "EUV and DUV lithography equipment — sole supplier of EUV for TSMC advanced nodes",
                "bottleneck_score": 99,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASML is the sole global supplier of EUV lithography systems; TSMC's N3/N2 nodes are physically impossible without ASML EUV.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASML holds a global monopoly on EUV scanners; TSMC is ASML's largest single customer, purchasing the majority of ASML's annual EUV output.",
                    "TSMC's N3/N2 technology nodes require multiple EUV exposures per layer; no alternative lithography technology can replicate EUV at production scale.",
                    "ASML's NXE:3800E is the primary EUV scanner for TSMC N3; ASML's next-gen High-NA EUV (EXE:5000) is required for TSMC N2 and beyond.",
                ],
                "source_urls": ["https://www.asml.com/en/investors/annual-report"],
            },
            {
                "ticker": "AMAT",
                "company_name": "Applied Materials Inc.",
                "layer": 2,
                "supply_chain_role": "Deposition, etch, and implant equipment — largest equipment supplier to TSMC globally",
                "bottleneck_score": 88,
                "evidence_grade": "A",
                "evidence_grade_reason": "AMAT is TSMC's largest semiconductor equipment supplier by revenue; TSMC accounts for ~25% of AMAT's total revenue.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Applied Materials is TSMC's largest semicap equipment vendor; AMAT CVD, PVD, and CMP tools are used in every TSMC logic node from N7 to N2.",
                    "AMAT's Centura Sculpta system enables EUV pattern shaping at TSMC, reducing mask layers and improving yield at N3/N2.",
                ],
            },
            {
                "ticker": "TOELY",
                "company_name": "Tokyo Electron Limited",
                "tradingview_symbol": "TOELY",
                "layer": 2,
                "supply_chain_role": "Coater/developer and thermal processing equipment — sole supplier for TSMC's EUV lithography process modules",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "Tokyo Electron is sole supplier of EUV-compatible coater/developer systems and is TSMC's #2 equipment vendor by revenue.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Tokyo Electron CLEAN TRACK ACT 12 EUV coater/developer is the industry-standard track system for TSMC EUV lithography; no qualified alternative exists.",
                    "TEL holds ~90% global coater/developer market share; TSMC's EUV patterning yield depends on TEL track uptime.",
                ],
            },
            {
                "ticker": "LRCX",
                "company_name": "Lam Research Corporation",
                "layer": 2,
                "supply_chain_role": "Etch and deposition equipment — critical for TSMC's multi-patterning and gate-all-around formation",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "Lam Research is TSMC's #3 equipment supplier; Lam's VECTOR ALD and etch systems are used at every critical TSMC advanced node.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Lam's Kiyo etch and VECTOR ALD systems are required for TSMC's gate-all-around nanosheet formation at N2.",
                    "Lam Research's selective etch is used for multi-patterning at TSMC N3; Lam is the dominant plasma etch supplier.",
                ],
            },
            {
                "ticker": "KLAC",
                "company_name": "KLA Corporation",
                "layer": 2,
                "supply_chain_role": "Inspection and metrology — process control for yield management across all TSMC nodes",
                "bottleneck_score": 85,
                "evidence_grade": "A",
                "evidence_grade_reason": "KLA holds ~50% global process control share; TSMC's yield ramp at every new node is gate-limited by KLA inspection capability.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "KLA's patterned wafer inspection and e-beam review tools are deployed at every TSMC logic fab; TSMC's N3 yield ramp was enabled by KLA Gen5 e-beam inspection.",
                    "KLA's Reticle inspection tools are used by TSMC's mask shop; mask defect control is critical to EUV lithography yield.",
                ],
            },
            {
                "ticker": "ASMI",
                "company_name": "ASM International N.V.",
                "tradingview_symbol": "ASMI",
                "layer": 2,
                "supply_chain_role": "ALD equipment for TSMC gate dielectric and barrier metal deposition",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASMI holds ~80% global ALD market share; TSMC's gate-all-around N2 nodes require ASMI ALD for conformal dielectric deposition.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASM International's Pulsar ALD system is used for high-k metal gate dielectric deposition at TSMC N3/N2.",
                    "ASMI's ALD is the only commercially available system capable of conformal gate-dielectric deposition in TSMC's nanosheet GAA architecture.",
                ],
            },
            {
                "ticker": "ENTG",
                "company_name": "Entegris Inc.",
                "layer": 3,
                "supply_chain_role": "Ultra-pure process chemicals and contamination control materials for TSMC fabs",
                "bottleneck_score": 78,
                "evidence_grade": "A",
                "evidence_grade_reason": "Entegris is a disclosed TSMC preferred supplier of process chemicals, filters, and materials; TSMC accounts for a significant portion of Entegris revenue.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain", "soi_substrates_materials"],
                "evidence": [
                    "Entegris supplies ultra-high-purity chemicals, advanced filters, and materials-handling systems to TSMC fabs; Entegris is on TSMC's preferred supplier list.",
                    "Entegris's Advanced Purity Solutions division produces photoresist ancillary chemicals and EUV pellicle materials used at TSMC N3/N2.",
                ],
            },
            {
                "ticker": "ONTO",
                "company_name": "Onto Innovation Inc.",
                "layer": 3,
                "supply_chain_role": "Advanced packaging metrology and inspection for TSMC CoWoS and SoIC 3D packaging",
                "bottleneck_score": 70,
                "evidence_grade": "B",
                "evidence_grade_reason": "Onto Innovation provides lithography and inspection tools for TSMC's advanced packaging lines; CoWoS inspection is a key deployment.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test", "semicap_supply_chain"],
                "evidence": [
                    "Onto Innovation's Dragonfly G3 inspection system is used at TSMC CoWoS advanced packaging lines for die-to-substrate alignment and bump inspection.",
                ],
            },
            {
                "ticker": "MKS",
                "company_name": "MKS Instruments Inc.",
                "layer": 3,
                "supply_chain_role": "Gas delivery, pressure control, and power systems for TSMC fab process equipment",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "MKS Instruments supplies critical subsystems (mass flow controllers, power supplies, gas analyzers) to all major TSMC equipment suppliers.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "MKS Instruments mass flow controllers and RF power generators are embedded in Applied Materials, Lam Research, and Tokyo Electron process tools at TSMC fabs.",
                ],
            },
            {
                "ticker": "AEIS",
                "company_name": "Advanced Energy Industries Inc.",
                "layer": 3,
                "supply_chain_role": "Precision power delivery and RF systems for TSMC etch and CVD process equipment",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Advanced Energy is the primary RF power supplier for Lam Research and Applied Materials plasma etch systems at TSMC.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Advanced Energy Industries RF generators and pulsed power systems are integral to plasma etch and CVD equipment used at TSMC N3/N2 nodes.",
                ],
            },
            {
                "ticker": "ATEYY",
                "company_name": "Advantest Corporation",
                "tradingview_symbol": "ATEYY",
                "layer": 3,
                "supply_chain_role": "High-speed semiconductor test equipment for TSMC-produced advanced logic and memory chips",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "Advantest is the dominant test equipment supplier for advanced SoC and AI chips; TSMC customers require Advantest ATE for chip acceptance testing.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "Advantest T2000 and V93000 ATE systems are used to test advanced SoC, AI GPU, and HBM chips produced at TSMC; Advantest is TSMC's primary ATE ecosystem partner.",
                ],
            },
            {
                "ticker": "FORM",
                "company_name": "FormFactor Inc.",
                "layer": 3,
                "supply_chain_role": "Probe cards and wafer-level test equipment for TSMC advanced node chip acceptance testing",
                "bottleneck_score": 60,
                "evidence_grade": "B",
                "evidence_grade_reason": "FormFactor is the leading probe card supplier; TSMC wafer-sort testing for all advanced nodes depends on FormFactor probe cards.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "FormFactor probe cards are used in TSMC's wafer-sort testing for advanced node products; FormFactor holds majority share of advanced probe card market.",
                ],
            },
            {
                "ticker": "APD",
                "company_name": "Air Products and Chemicals Inc.",
                "layer": 3,
                "supply_chain_role": "Specialty gases (NF3, H2, N2, ultra-pure argon) for TSMC fab process chemistry",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Air Products is a named TSMC preferred supplier of specialty gases; TSMC's advanced nodes require ultra-pure specialty gas supply under long-term contracts.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain", "soi_substrates_materials"],
                "evidence": [
                    "Air Products supplies ultra-high-purity NF3 (chamber cleaning), hydrogen, and nitrogen to TSMC fabs under long-term on-site supply agreements.",
                    "TSMC's N3/N2 fabs consume tens of thousands of tonnes of specialty gases annually; Air Products is among TSMC's largest disclosed gas suppliers.",
                ],
            },
            {
                "ticker": "LIN",
                "company_name": "Linde plc",
                "layer": 3,
                "supply_chain_role": "Industrial and specialty gases for TSMC fab operations (ultra-pure nitrogen, helium, argon)",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "Linde is a confirmed on-site gas supplier to TSMC Taiwan fabs; Linde operates dedicated gas production plants at TSMC's Hsinchu and Tainan campuses.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Linde operates dedicated on-site air separation units and gas supply infrastructure at TSMC's Hsinchu Science Park and GIGAFAB facilities.",
                    "TSMC's annual gas consumption requires dedicated Linde plant-of-record installations on or adjacent to TSMC campuses.",
                ],
            },
            {
                "ticker": "CDNS",
                "company_name": "Cadence Design Systems Inc.",
                "layer": 2,
                "supply_chain_role": "EDA software for chip design at TSMC advanced nodes; TSMC-certified design flows",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Cadence is TSMC's primary EDA partner; Cadence tools are part of the certified TSMC Design-Rules Check (DRC) flow at N3/N2.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Cadence is a TSMC Open Innovation Platform (OIP) ecosystem partner; Virtuoso and Innovus tools provide the certified EDA flow for TSMC N3/N2 chip design.",
                    "TSMC's advanced node PDKs are co-developed with Cadence; Cadence Spectre circuit simulation is the standard for TSMC advanced node signoff.",
                ],
            },
            {
                "ticker": "SNPS",
                "company_name": "Synopsys Inc.",
                "layer": 2,
                "supply_chain_role": "EDA and silicon IP for TSMC chip design; largest EDA company by revenue",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Synopsys is TSMC's largest EDA partner; Synopsys tools are certified for TSMC N3/N2 design and verification.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Synopsys IC Compiler II and Fusion Compiler are the primary P&R tools certified in TSMC N3/N2 design flows.",
                    "Synopsys DesignWare IP (PCIe, USB, LPDDR) is the primary interface IP used by fabless customers designing chips at TSMC advanced nodes.",
                ],
            },
            {
                "ticker": "BESI",
                "company_name": "BE Semiconductor Industries N.V.",
                "tradingview_symbol": "BESI",
                "layer": 3,
                "supply_chain_role": "Hybrid bonding and die attach equipment for TSMC SoIC and CoWoS 3D IC packaging",
                "bottleneck_score": 78,
                "evidence_grade": "A",
                "evidence_grade_reason": "BESI dominates hybrid bonding tools used in TSMC's SoIC 3D IC platform; sole qualified tool for sub-2μm Cu-Cu bonding.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "BE Semiconductor is the sole qualified supplier of Cu-Cu hybrid bonding tools for TSMC SoIC; BESI's Datacon tools are used on TSMC's SoIC-X and SoIC-T lines.",
                ],
            },
            {
                "ticker": "GLW",
                "company_name": "Corning Incorporated",
                "layer": 3,
                "supply_chain_role": "Glass substrates and optical fiber for TSMC fab facilities and data center connectivity",
                "bottleneck_score": 55,
                "evidence_grade": "B",
                "evidence_grade_reason": "Corning glass substrates are used in TSMC display-grade test environments; optical fiber connectivity is used in TSMC campus networking.",
                "relationship_specificity": "critical_upstream",
                "themes": ["soi_substrates_materials"],
                "evidence": [
                    "Corning supplies specialty glass substrates and optical fiber to TSMC fab facilities and campus networking infrastructure.",
                ],
            },
            {
                "ticker": "AMKR",
                "company_name": "Amkor Technology Inc.",
                "layer": 2,
                "supply_chain_role": "Advanced OSAT and CoWoS substrate assembly for TSMC packaging ecosystem",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Amkor is a key TSMC packaging ecosystem partner; Amkor provides CoWoS substrate and fan-out packaging for TSMC fabless customers.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "Amkor Technology is TSMC's primary external OSAT partner for CoWoS and InFO advanced packaging; TSMC customer chips are packaged at Amkor facilities.",
                ],
            },
        ],
    },

    # ═══════════════════════════════════════════════════════════════════════════
    # GOOG — Google / Alphabet
    # ═══════════════════════════════════════════════════════════════════════════
    "GOOG": {
        "anchor_name": "Google / Alphabet",
        "nodes": [
            {
                "ticker": "TSM",
                "company_name": "Taiwan Semiconductor Manufacturing Company",
                "layer": 3,
                "supply_chain_role": "Advanced node foundry for Google TPU v5/v6 and Tensor G-series custom AI chips",
                "bottleneck_score": 96,
                "evidence_grade": "A",
                "evidence_grade_reason": "TSMC manufactures Google's TPU v5p at N5 and Tensor G4 at N4; confirmed in Google Cloud and TSMC investor materials.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["semicap_supply_chain", "custom_silicon"],
                "evidence": [
                    "Google's TPU v5p is manufactured at TSMC N5; Google Cloud discloses TPU generation and process node in technical documentation.",
                    "Google's Tensor G4 (Pixel 9) is manufactured at TSMC N4; Google relies entirely on TSMC for all custom silicon production.",
                ],
            },
            {
                "ticker": "AVGO",
                "company_name": "Broadcom Inc.",
                "layer": 2,
                "supply_chain_role": "Co-designer and manufacturer of Google TPU ASIC and GCP networking switch ASICs",
                "bottleneck_score": 92,
                "evidence_grade": "A",
                "evidence_grade_reason": "Broadcom is confirmed co-designer and manufacturer of Google's TPU custom AI chips; relationship publicly disclosed.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["custom_silicon", "ai_infrastructure"],
                "evidence": [
                    "Broadcom co-designs Google's TPU ASICs and provides the Tomahawk Ethernet switching ASICs used in Google's Jupiter data center fabric.",
                    "Google Cloud and Broadcom jointly developed the TPU v4/v5 series; Broadcom's ASIC team designs the tensor core and HBM controller for each TPU generation.",
                ],
            },
            {
                "ticker": "NVDA",
                "company_name": "NVIDIA Corporation",
                "layer": 2,
                "supply_chain_role": "H100/H200 GPUs for Google Cloud AI compute alongside TPUs",
                "bottleneck_score": 80,
                "evidence_grade": "A",
                "evidence_grade_reason": "Google Cloud Platform deploys NVIDIA A100 and H100 GPUs on GCP; confirmed in Google Cloud product announcements.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["ai_infrastructure"],
                "evidence": [
                    "Google Cloud Platform offers NVIDIA H100 GPU instances; Google deploys NVIDIA GPUs alongside its own TPUs for GCP AI workloads.",
                ],
            },
            {
                "ticker": "MU",
                "company_name": "Micron Technology Inc.",
                "layer": 3,
                "supply_chain_role": "HBM3E memory for Google TPU v5p AI accelerators",
                "bottleneck_score": 82,
                "evidence_grade": "A",
                "evidence_grade_reason": "Micron supplies HBM3E for Google TPU v5p; Micron disclosed Google/GCP as a key HBM customer.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["memory_hbm"],
                "evidence": [
                    "Google's TPU v5p uses HBM3 memory; Micron is a disclosed HBM supplier to Google and GCP's custom AI chip program.",
                ],
            },
            {
                "ticker": "ASML",
                "company_name": "ASML Holding N.V.",
                "layer": 4,
                "supply_chain_role": "EUV lithography enabling TSMC production of Google TPU and Tensor custom chips",
                "bottleneck_score": 88,
                "evidence_grade": "A",
                "evidence_grade_reason": "ASML EUV monopoly; every Google custom chip (TPU, Tensor) manufactured at TSMC depends on ASML scanners.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "ASML EUV scanners are required for TSMC N5/N4 production of Google TPU v5 and Tensor G-series; no EUV alternative exists.",
                ],
            },
            {
                "ticker": "ETN",
                "company_name": "Eaton Corporation plc",
                "layer": 3,
                "supply_chain_role": "Power distribution and UPS for Google data centers and GCP AI infrastructure",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Eaton is a primary power management supplier to Google data centers; GCP AI cluster power density requires Eaton-class UPS and distribution.",
                "relationship_specificity": "critical_upstream",
                "themes": ["ai_power_energy", "data_center_infrastructure"],
                "evidence": [
                    "Eaton power distribution and UPS systems are deployed in Google's hyperscale data centers; GCP TPU pods require dedicated high-density power infrastructure.",
                ],
            },
            {
                "ticker": "VRT",
                "company_name": "Vertiv Holdings Co.",
                "layer": 2,
                "supply_chain_role": "Liquid cooling infrastructure for Google GCP TPU and GPU data center facilities",
                "bottleneck_score": 78,
                "evidence_grade": "A",
                "evidence_grade_reason": "Vertiv is a Google data center partner; Vertiv's liquid cooling is deployed in GCP AI infrastructure facilities.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["cooling_thermal", "ai_power_energy"],
                "evidence": [
                    "Vertiv supplies thermal management and power distribution infrastructure to Google data centers hosting GCP TPU and GPU clusters.",
                ],
            },
            {
                "ticker": "AMAT",
                "company_name": "Applied Materials Inc.",
                "layer": 4,
                "supply_chain_role": "Deposition equipment for TSMC advanced node production of Google custom AI chips",
                "bottleneck_score": 72,
                "evidence_grade": "B",
                "evidence_grade_reason": "AMAT largest semicap equipment vendor; structural upstream dependency for TSMC N5/N4 production of Google TPU.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Applied Materials equipment is used across TSMC's N5/N4 production line for Google TPU and Tensor chips.",
                ],
            },
            {
                "ticker": "LRCX",
                "company_name": "Lam Research Corporation",
                "layer": 4,
                "supply_chain_role": "Etch systems for TSMC N5/N4 production of Google custom AI chips",
                "bottleneck_score": 70,
                "evidence_grade": "B",
                "evidence_grade_reason": "Lam dominant etch share; structural upstream for TSMC production of Google TPU and Tensor silicon.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Lam Research etch systems are used in TSMC N5/N4 production of Google's TPU v5 and Tensor G4 custom chips.",
                ],
            },
            {
                "ticker": "KLAC",
                "company_name": "KLA Corporation",
                "layer": 4,
                "supply_chain_role": "Process control and inspection for TSMC production of Google AI chips",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "KLA ~50% process control market share; yield management for all TSMC advanced node production serving Google.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "KLA inspection tools manage yield at TSMC N5/N4 fabs producing Google's custom AI silicon.",
                ],
            },
            {
                "ticker": "COHR",
                "company_name": "Coherent Corp.",
                "layer": 2,
                "supply_chain_role": "800G optical transceivers for Google's Jupiter AI data center network fabric",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Coherent supplies 400G/800G transceivers to Google's Jupiter data center fabric; Google has publicly referenced optical transceiver supply constraints.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo", "data_center_infrastructure"],
                "evidence": [
                    "Coherent 400G/800G optical transceivers are deployed in Google's Jupiter data center network fabric interconnecting TPU pods.",
                ],
            },
            {
                "ticker": "LITE",
                "company_name": "Lumentum Holdings Inc.",
                "layer": 2,
                "supply_chain_role": "EML laser chips for optical transceivers in Google AI data center networking",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Lumentum EML lasers are in transceiver components deployed across Google's high-speed data center interconnects.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo"],
                "evidence": [
                    "Lumentum EML and DFB laser chips are used in optical transceivers deployed in Google's data center networking fabric.",
                ],
            },
            {
                "ticker": "AMKR",
                "company_name": "Amkor Technology Inc.",
                "layer": 3,
                "supply_chain_role": "Advanced packaging for Google TPU custom chips in TSMC's packaging ecosystem",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Amkor packages Google TPU chips produced at TSMC; Amkor is TSMC's primary external packaging partner.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["advanced_packaging_test"],
                "evidence": [
                    "Amkor provides advanced packaging services for Google TPU chips manufactured at TSMC; Amkor is the TSMC-certified packaging partner for custom AI chip customers.",
                ],
            },
            {
                "ticker": "TOELY",
                "company_name": "Tokyo Electron Limited",
                "tradingview_symbol": "TOELY",
                "layer": 4,
                "supply_chain_role": "Coater/developer and CVD equipment for TSMC production of Google custom chips",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "Tokyo Electron is TSMC's #2 equipment vendor; TEL coater/developer is sole-source for EUV track at TSMC N5/N4.",
                "relationship_specificity": "critical_upstream",
                "themes": ["semicap_supply_chain"],
                "evidence": [
                    "Tokyo Electron CLEAN TRACK systems are used in TSMC N5/N4 EUV lithography tracks for Google TPU chip production.",
                ],
            },
            {
                "ticker": "GLW",
                "company_name": "Corning Incorporated",
                "layer": 3,
                "supply_chain_role": "Optical fiber for Google's data center and subsea cable networking infrastructure",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "Corning supplies fiber to Google's GCP data center interconnects and Google's global subsea cable infrastructure.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo", "data_center_infrastructure"],
                "evidence": [
                    "Corning optical fiber is used in Google's data center networking and in Google's subsea cable system (e.g., Equiano, Firmina) connecting GCP regions.",
                ],
            },
            {
                "ticker": "SNPS",
                "company_name": "Synopsys Inc.",
                "layer": 3,
                "supply_chain_role": "EDA software for Google's custom AI chip (TPU, Tensor) design and verification",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Synopsys EDA tools are used in Google's custom silicon design flow for TPU and Tensor chips at TSMC advanced nodes.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["semicap_supply_chain", "custom_silicon"],
                "evidence": [
                    "Google's custom AI chip design team uses Synopsys Fusion Compiler and Primetime for TPU and Tensor chip P&R and timing signoff at TSMC N5/N4.",
                ],
            },
            {
                "ticker": "CDNS",
                "company_name": "Cadence Design Systems Inc.",
                "layer": 3,
                "supply_chain_role": "EDA and verification tools for Google's TPU and Tensor chip design at TSMC advanced nodes",
                "bottleneck_score": 70,
                "evidence_grade": "A",
                "evidence_grade_reason": "Cadence tools are in the TSMC-certified design flow for N5/N4; Google's custom silicon team uses Cadence for verification.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["semicap_supply_chain", "custom_silicon"],
                "evidence": [
                    "Cadence Xcelium and JasperGold verification tools are used in Google's TPU design flow; Cadence is a TSMC OIP ecosystem partner for N5/N4.",
                ],
            },
            {
                "ticker": "MRVL",
                "company_name": "Marvell Technology Inc.",
                "layer": 2,
                "supply_chain_role": "Custom networking ASICs and electro-optic DSPs for Google GCP data center fabric",
                "bottleneck_score": 72,
                "evidence_grade": "A",
                "evidence_grade_reason": "Marvell supplies GCP data center networking ASICs and co-packaged optics DSPs for Google's Jupiter fabric upgrades.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["custom_silicon", "data_center_infrastructure"],
                "evidence": [
                    "Marvell's electro-optic DSP chips and custom Ethernet ASICs are deployed in Google's Jupiter data center switching fabric.",
                ],
            },
            {
                "ticker": "ANET",
                "company_name": "Arista Networks Inc.",
                "layer": 2,
                "supply_chain_role": "Ethernet switching for Google GCP spine-leaf data center networking",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Arista supplies Ethernet switches to GCP data centers; Google uses a mix of custom Jupiter switches and merchant-silicon Arista for edge/interconnect.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["data_center_infrastructure"],
                "evidence": [
                    "Arista Networks supplies 400G Ethernet switching to Google GCP data center edge and interconnect layers.",
                ],
            },
            {
                "ticker": "CRDO",
                "company_name": "Credo Technology Group Holding Ltd.",
                "tradingview_symbol": "CRDO",
                "layer": 2,
                "supply_chain_role": "High-speed SerDes and active electrical cables for GCP AI cluster interconnects",
                "bottleneck_score": 68,
                "evidence_grade": "B",
                "evidence_grade_reason": "Credo Technology is a confirmed supplier of active electrical cables and SerDes to Google GCP AI data centers.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["data_center_infrastructure", "ai_infrastructure"],
                "evidence": [
                    "Credo Technology's line card retimers and active electrical cable ASICs are deployed in Google GCP AI cluster interconnects for TPU pod networking.",
                ],
            },
            {
                "ticker": "FN",
                "company_name": "Fabrinet",
                "layer": 2,
                "supply_chain_role": "Contract manufacturing of optical transceivers for Google's data center networking",
                "bottleneck_score": 65,
                "evidence_grade": "B",
                "evidence_grade_reason": "Fabrinet manufactures Coherent and other transceivers deployed in Google's data center networking.",
                "relationship_specificity": "confirmed_supplier",
                "themes": ["photonics_cpo"],
                "evidence": [
                    "Fabrinet manufactures optical transceivers for Google's data center networking supply chain.",
                ],
            },
            {
                "ticker": "ARM",
                "company_name": "Arm Holdings plc",
                "tradingview_symbol": "ARM",
                "layer": 3,
                "supply_chain_role": "CPU architecture for Google Tensor chips and Axion cloud server processors",
                "bottleneck_score": 75,
                "evidence_grade": "A",
                "evidence_grade_reason": "Google's Tensor G-series uses Arm CPU cores; Google Axion (C4A) is Arm Neoverse V2-based — disclosed in Google Cloud announcements.",
                "relationship_specificity": "confirmed_partner",
                "themes": ["custom_silicon", "cloud_ai_infra"],
                "evidence": [
                    "Google's Axion Arm-based server processor (C4A instances) uses Arm Neoverse V2 architecture under license; Google Cloud announced Axion as Google's first custom Arm CPU.",
                    "Google's Tensor G4 mobile chip uses Arm Cortex X4 and A720 CPU cores; Google is one of Arm's largest licensees.",
                ],
            },
        ],
    },
}


# ── Public API ──────────────────────────────────────────────────────────────────

def get_curated_anchor_bottlenecks(anchor_key: str) -> list[dict]:
    """
    Return the fully-expanded curated node list for one anchor.
    Nodes are sorted descending by bottleneck_score.
    """
    key = anchor_key.upper()
    entry = _ANCHOR_RAW.get(key)
    if not entry:
        return []
    anchor_name = entry["anchor_name"]
    nodes = [_fill_node(key, anchor_name, n) for n in entry["nodes"]]
    nodes.sort(key=lambda r: r["bottleneck_score"], reverse=True)
    return nodes


def get_curated_anchor_list() -> list[dict]:
    """
    Return a summary row per anchor (anchor_key, anchor_name, row_count, status, last_curated_at).
    """
    result = []
    for key, entry in _ANCHOR_RAW.items():
        result.append({
            "anchor_key":      key,
            "anchor_name":     entry["anchor_name"],
            "row_count":       len(entry["nodes"]),
            "status":          "ready",
            "last_curated_at": LAST_CURATED_AT,
        })
    return result


def get_curated_anchor_overlap(include_manual: Optional[list[dict]] = None) -> list[dict]:
    """
    Return tickers that appear in more than one anchor map (including any manual rows).

    include_manual: optional list of manual-added rows (each must have ticker, anchor_key,
                    company_name, supply_chain_role, bottleneck_score).
    """
    from collections import defaultdict

    ticker_map: dict[str, dict] = defaultdict(lambda: {
        "anchors": [],
        "company_name": "",
        "max_bottleneck_score": 0,
        "roles_by_anchor": {},
    })

    # Curated rows
    for key, entry in _ANCHOR_RAW.items():
        anchor_name = entry["anchor_name"]
        for n in entry["nodes"]:
            t = n["ticker"].upper()
            d = ticker_map[t]
            if key not in d["anchors"]:
                d["anchors"].append(key)
            d["company_name"] = n["company_name"]
            d["max_bottleneck_score"] = max(d["max_bottleneck_score"], n["bottleneck_score"])
            d["roles_by_anchor"][key] = n["supply_chain_role"]

    # Manual rows (optional)
    if include_manual:
        for row in include_manual:
            t = (row.get("ticker") or "").upper()
            ak = (row.get("anchor_key") or "").upper()
            if not t or not ak:
                continue
            d = ticker_map[t]
            if ak not in d["anchors"]:
                d["anchors"].append(ak)
            d["company_name"] = row.get("company_name") or d["company_name"]
            d["max_bottleneck_score"] = max(
                d["max_bottleneck_score"],
                int(row.get("bottleneck_score") or 0),
            )
            d["roles_by_anchor"][ak] = row.get("supply_chain_role") or ""

    # Filter to multi-anchor tickers and sort by count desc, score desc
    items = []
    for ticker, d in ticker_map.items():
        if len(d["anchors"]) >= 2:
            items.append({
                "ticker":               ticker,
                "company_name":         d["company_name"],
                "anchors":              sorted(d["anchors"]),
                "count":                len(d["anchors"]),
                "max_bottleneck_score": d["max_bottleneck_score"],
                "roles_by_anchor":      d["roles_by_anchor"],
            })

    items.sort(key=lambda x: (-x["count"], -x["max_bottleneck_score"]))
    return items
