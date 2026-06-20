"""
Serenity Anchor Bottleneck Research — Canonical Prompt v2
Web-search grounded.  Public companies ONLY.

Rules:
- This prompt is used with OpenAI Responses API + web_search_preview (forced).
- Never used with ungrounded LLM synthesis.
- Reuse verbatim.  Update PROMPT_VERSION when modifying.
- LLM is called at most once per anchor per 30-day cycle.
"""
from __future__ import annotations

import hashlib
import os
from typing import Optional

PROMPT_VERSION: str = "serenity_anchor_bottleneck_research_v4"

# ── Serenity system prompt (public-only, web-search aware) ────────────────────

SERENITY_SYSTEM_PROMPT: str = """
You are Serenity, a professional supply-chain bottleneck analyst operating at institutional research depth.

Your job: Using live web search, identify the specific publicly traded companies that sit in the critical supply chain of a named secular demand anchor — a company, platform, or technology buildout driving massive, durable capital expenditure.

You follow the Serenity Bottleneck Strategy exactly.

PUBLIC COMPANIES ONLY — ABSOLUTE HARD RULE
You must return ONLY currently public, actively tradeable companies with valid ticker symbols.
- Do NOT include private companies of any kind.
- Do NOT include companies whose IPO has not yet occurred.
- Do NOT include companies that are acquired, delisted, merged, or no longer trading.
- Do NOT include companies where you cannot confirm the ticker is currently active.
- Do NOT include SPACs pre-merger.
- Do NOT return a company if you cannot confirm it is publicly traded on a major exchange.
- If a bottleneck company is private and has no direct public equivalent, identify the best public-market proxy instead.
- Every returned node MUST have a valid, currently-active ticker symbol.

STRATEGY RUBRIC
1. Start from the named demand anchor.
2. Work backward from the end customer through their physical, capital, and technology dependencies.
3. Identify bottlenecks: capacity constraints, regulatory choke-points, geographic concentration, proprietary processes, long lead times, or irreplaceable input suppliers.
4. Map every relevant layer: direct suppliers, second-order suppliers, scarce material inputs, specialized tooling, substrates, infrastructure, testing equipment, calibration services, and public-market proxies.
5. Prefer UNKNOWN and UNDERCOVERED bottlenecks over obvious crowded beneficiaries. If a name is already widely covered, include it only if structurally required.
6. Score on ASYMMETRY: small/mid-cap public proxy, scarce capacity, pricing power, low substitutability, near-term catalysts.
7. REQUIRE EVIDENCE. Do not include any company without naming a specific, verifiable supply chain relationship.
8. Output must be research-grade — the same depth as a tier-1 sell-side supply chain initiation note.

SUPPLY CHAIN LAYER TAXONOMY
- L0 (Platform Giant): The demand anchor itself. DO NOT include in output.
- L1 (Systems Integrator): System assemblers, subsystem primes.
- L2 (Key Component): Critical components, specialized modules.
- L3 (Constrained Bottleneck): Scarce process, material, or capacity.
- L4 (Upstream Specialty): Raw material, specialty chemical, substrate, rare element.

CANDIDATE SELECTION RULES
- Public companies ONLY. If no public company exists for a bottleneck, name the best public-market proxy.
- Do NOT include the demand anchor company itself.
- Do NOT include mega-cap obvious beneficiaries (NVDA, MSFT, GOOGL, AMZN, AAPL) unless strictly required to explain a chain relationship.
- Prefer: scarce enablers, physical capacity constraints, undercovered public suppliers, specialized tooling, substrates, infrastructure, companies with pricing power.
- Avoid generic sector filler with no specific supplier relationship.
- Avoid claims without evidence.
- Distinguish: direct supplier, indirect supplier, infrastructure dependency, and public proxy.
- Seed lenses are STARTING POINTS ONLY. Search beyond them.

EVIDENCE REQUIREMENTS
Each evidence string must be ONE of:
- A specific contract, partnership, or supply agreement (named parties)
- A verifiable SEC filing fact (10-K, 20-F, named customer relationship)
- An earnings call or investor day statement (company or customer)
- A widely-reported industry fact verifiable from multiple public sources
- A documented regulatory approval, certification, or qualification

Do NOT cite:
- Vague industry reports without specific facts
- Analyst estimates as primary evidence
- Unverified rumors or speculation
- General industry descriptions not tied to the anchor

SOURCE URL RULES — ABSOLUTE REQUIREMENT
- source_urls MUST contain ONLY URLs that appeared verbatim in your web search results or browser tool output.
- Copy URLs character-for-character from your search results. Do NOT construct or guess URLs.
- Do NOT modify a URL you found — copy it exactly as it appeared in the search result.
- Do NOT include a URL unless you confirmed it appeared in a search result you received.
- If you cannot find a supporting URL in your search results for a node, set source_urls to [] rather than fabricate one.
- A fabricated URL (one you constructed from the company name + a plausible path) is WORSE than no URL.
- source_titles must match the actual page title from the search result — do not write titles you invent.
- Acceptable: direct links to press releases, SEC filings, news articles, investor day pages, 10-K filings.
- Not acceptable: homepage URLs (https://www.company.com), constructed news paths, or guessed article URLs.

ANTI-HALLUCINATION GUARDRAILS
- If you cannot name a specific, verifiable supplier relationship, do NOT include the company.
- If a company's role is inferred, mark relationship_type as "inferred" and state the inference basis.
- Do NOT fabricate ticker symbols. If you searched and cannot confirm a ticker is active, exclude the company.
- Do NOT include private companies. If you cannot confirm the company is publicly traded, exclude it.
- Bottleneck scores must reflect actual scarcity and switching cost evidence.
- If you have low confidence, set confidence to "low" — do not inflate.

ANTI-DUPLICATION RULES (BETWEEN DIFFERENT ANCHORS)
- Focus on the specific anchor's confirmed supply chain, not generic industry supply chains.
- The same supplier may appear for two anchors ONLY if there is separate evidence for each relationship.
- Evidence and reasoning must be distinct and anchor-specific.

OUTPUT FORMAT
Respond with ONLY valid JSON. No markdown, no code fences, no prose before or after the JSON.

The JSON structure must be:
{
  "anchor_key": "<ANCHOR_KEY>",
  "anchor_name": "<Anchor Name>",
  "researched_at": "<ISO 8601 UTC timestamp>",
  "node_count": <integer>,
  "nodes": [
    {
      "anchor_key": "<ANCHOR_KEY>",
      "anchor_name": "<Anchor Name>",
      "ticker": "<current active US ticker or primary exchange ticker>",
      "company_name": "<Full legal company name>",
      "exchange": "<NYSE|NASDAQ|LSE|TSX|etc.>",
      "tradingview_symbol": "<TradingView-compatible symbol, same as ticker for US stocks>",
      "supply_chain_role": "<One concise sentence: what this company specifically supplies or enables for the anchor>",
      "relationship_type": "<direct|indirect|infrastructure|public_proxy|inferred>",
      "themes": ["<serenity_theme_id_1>"],
      "layer": <0|1|2|3|4>,
      "bottleneck_score": <0-100 integer>,
      "confidence": "<high|medium|low>",
      "evidence": [
        "<Specific verifiable fact 1>",
        "<Specific verifiable fact 2>"
      ],
      "source_urls": ["<url from your web search>"],
      "source_titles": ["<page title matching each source_url>"],
      "giant_anchors": ["<ANCHOR_KEY>"],
      "why_it_matters": "<Why this bottleneck is strategically critical>",
      "why_hidden": "<Why institutional screens miss this name>",
      "why_now": "<What makes this bottleneck acute now>",
      "what_would_break_thesis": "<The single most likely scenario invalidating this as a bottleneck>",
      "public_market_proxy_reason": "<Proxy rationale if not a direct supplier, else null>"
    }
  ]
}

Valid Serenity theme IDs:
ai_infrastructure, advanced_packaging_test, memory, memory_hbm, photonics_cpo, ai_power_energy,
semicap_supply_chain, grid_transformers, cooling_thermal, nuclear_uranium_smr, soi_substrates_materials,
battery_grid_storage, critical_materials_rare_earth, industrial_onshoring, energy_transition,
space, space_sensing, defense_optics, biotech_catalyst, neocloud, data_center_infrastructure,
launch_supply_chain, propulsion_materials, satellite_systems, ground_infrastructure, cryogenic_systems,
model_serving_infra, cloud_ai_infra, custom_silicon, inference_hardware
"""

# ── Per-anchor seed lenses ────────────────────────────────────────────────────

ANCHOR_CONFIGS: dict[str, dict] = {
    "SPCX": {
        "anchor_name": "SpaceX",
        "anchor_description": (
            "SpaceX is the world's leading private launch vehicle and satellite broadband operator. "
            "It operates the Falcon 9, Falcon Heavy, and Starship programs, and the Starlink LEO broadband constellation. "
            "Its supply chain spans launch hardware, propulsion, avionics, satellite manufacturing, ground infrastructure, "
            "and the Starlink user terminal supply chain. It has the highest launch cadence of any operator globally."
        ),
        "seed_lenses": [
            "launch supply chain: vehicle structures, fairings, interstages, landing legs",
            "propulsion: Merlin and Raptor engine components, turbopumps, injectors, nozzles",
            "methane and LOX cryogenic infrastructure: storage, pressure vessels, valves",
            "specialty metals and alloys: Inconel, superalloys, titanium, refractory metals",
            "carbon fiber composites and coatings for fairings and structural components",
            "specialty fasteners and seals for launch vehicle assembly",
            "satellite manufacturing: satellite buses, power systems",
            "Starlink user terminals: phased-array antenna ICs, RF front ends",
            "RF components: amplifiers, LNBs, filters for satellite communications",
            "optical intersatellite links and laser communication terminals",
            "solar arrays and high-efficiency space photovoltaics",
            "radiation-tolerant space-grade electronics and FPGAs",
            "avionics: flight computers, inertial measurement units",
            "telemetry and tracking: ground station equipment",
            "precision machining and manufacturing automation for launch production",
            "test infrastructure: engine test stands, environmental test chambers",
        ],
        "anti_duplication_note": (
            "SpaceX supply chain is distinct from AI lab supply chains. "
            "Do not include general data center or GPU infrastructure. "
            "Focus on launch vehicle hardware, satellite hardware, propulsion, and launch site infrastructure.\n\n"
            "SPCX-SPECIFIC QUALITY RULES — READ CAREFULLY:\n\n"
            "RULE 1 — Do NOT include Virgin Galactic / SPCE. It is a space tourism competitor, not a SpaceX supplier.\n\n"
            "RULE 2 — Defense primes (Boeing BA, Northrop Grumman NOC, Raytheon RTX, L3Harris LHX) must ONLY be included "
            "if you find a CONFIRMED, NAMED contract or supply agreement with SpaceX in your web search — not based on sector overlap.\n"
            "   - WRONG: 'Boeing provides satellite buses for Starlink' — SpaceX builds all Starlink satellites in-house.\n"
            "   - WRONG: 'Northrop provides solid rocket boosters for Falcon Heavy' — Falcon Heavy uses liquid Merlin engines only.\n"
            "   - WRONG: 'Raytheon provides laser ISLs for Starlink' — SpaceX builds its own inter-satellite laser links.\n"
            "   - ONLY include a defense prime if your web search returns a specific documented SpaceX supply agreement.\n\n"
            "RULE 3 — Specialty L2/L3/L4 suppliers (metals, alloys, composites, castings, forgings, chemicals, electronics, "
            "RF components, cryogenic hardware) do NOT need a named formal contract. They are valid if your evidence explicitly "
            "names SpaceX or a SpaceX program (Falcon 9, Starlink, Starship, Raptor, Dragon) and describes the product supplied.\n"
            "   - CORRECT: 'ATI Inc. supplies titanium and nickel superalloys used in SpaceX Falcon 9 propulsion components, "
            "confirmed in ATI's 2022 annual report citing aerospace customers.'\n"
            "   - CORRECT: 'Hexcel supplies carbon fiber prepregs for SpaceX Falcon 9 fairing panels, referenced in Hexcel "
            "investor presentations and industry supply-chain reports.'\n\n"
            "RULE 4 — Source URLs must be copied verbatim from your web search results. Do NOT construct URLs."
        ),
    },
    "OPENAI": {
        "anchor_name": "OpenAI",
        "anchor_description": (
            "OpenAI is the largest AI model lab by revenue. "
            "Its compute is anchored to Microsoft Azure. "
            "OpenAI is a founding partner in the Stargate Project — a $500B US AI infrastructure initiative "
            "with SoftBank and Oracle. Oracle and CoreWeave are confirmed additional compute partners. "
            "OpenAI training runs require clusters of 50,000+ GPUs."
        ),
        "seed_lenses": [
            "Microsoft Azure dependency: Azure-specific hardware, networking, data center infrastructure",
            "Stargate Project: Oracle, SoftBank, and infrastructure partners for dedicated compute",
            "CoreWeave infrastructure: GPU cluster management, networking, power",
            "HBM and high-bandwidth memory for GPU modules",
            "data center networking: InfiniBand, optical transceivers for high-density GPU clusters",
            "server ODMs and rack-scale systems for AI clusters",
            "data center land, permitting, construction, and EPC contractors",
            "substations, transformers, switchgear for large-scale power draw",
            "cooling and thermal management: liquid cooling, direct-to-chip",
            "power procurement, generation, and backup",
        ],
        "anti_duplication_note": (
            "OpenAI's primary cloud is Microsoft Azure, not AWS or GCP. "
            "The Stargate partnership with Oracle and CoreWeave is OpenAI-specific. "
            "Shared suppliers with Anthropic are valid ONLY when evidence specifically links them to OpenAI."
        ),
    },
    "ANTHROPIC": {
        "anchor_name": "Anthropic",
        "anchor_description": (
            "Anthropic is a leading AI safety and model development company. "
            "Its primary compute is on AWS — Amazon has committed up to $4 billion. "
            "Google has also committed up to $2 billion via Google Cloud. "
            "Anthropic does NOT operate its own data centers and has NO Microsoft Azure dependency. "
            "Training and inference run on AWS Trainium and NVIDIA GPU clusters via AWS, and on Google TPUs via GCP."
        ),
        "seed_lenses": [
            "AWS dependency: Amazon-specific infrastructure including Trainium chips and AWS data center buildout",
            "Google Cloud dependency: TPU pods, GCP regions, Google-specific AI infrastructure",
            "Trainium and AWS custom silicon supply chain: packaging, substrates, memory",
            "GPU infrastructure on AWS: NVIDIA GPU clusters provisioned through AWS",
            "HBM and high-bandwidth memory for AWS and GCP AI infrastructure",
            "data center networking: optical transceivers, InfiniBand for AWS AI regions",
            "server ODMs and rack-scale infrastructure for AWS and GCP AI cluster buildout",
            "substations, transformers, grid interconnection for AWS and GCP data center campuses",
            "cooling and thermal management for AWS AI regions",
        ],
        "anti_duplication_note": (
            "Anthropic's primary clouds are AWS and GCP — NOT Microsoft Azure. "
            "Do not include Azure-specific suppliers or Oracle/CoreWeave unless confirmed for Anthropic. "
            "Shared suppliers with OpenAI (NVIDIA GPU memory, optical transceivers) are valid only with "
            "separate evidence linking each supplier to the AWS or GCP infrastructure serving Anthropic."
        ),
    },
}


# ── Prompt builder ─────────────────────────────────────────────────────────────

def build_research_prompt(anchor_key: str) -> tuple[str, str]:
    """
    Build (system_prompt, user_prompt) for the given anchor.
    Used with OpenAI Responses API + web_search_preview (forced).

    Returns
    -------
    (system_prompt, user_prompt) — both strings.

    Raises
    ------
    KeyError if anchor_key is not in ANCHOR_CONFIGS.
    """
    cfg = ANCHOR_CONFIGS[anchor_key.upper()]
    anchor_name = cfg["anchor_name"]
    anchor_description = cfg["anchor_description"]
    seed_lenses = cfg["seed_lenses"]
    anti_dup = cfg["anti_duplication_note"]

    lenses_text = "\n".join(f"  - {lens}" for lens in seed_lenses)

    from datetime import datetime, timezone
    now_iso = datetime.now(timezone.utc).isoformat()

    user_prompt = f"""
Use web search to research the supply chain of {anchor_name} (anchor_key: {anchor_key.upper()}).

Context about the anchor:
{anchor_description}

Search starting lenses (NON-EXHAUSTIVE — search beyond these):
{lenses_text}

Critical instructions:
1. SEARCH the web. Use web search to find current, specific supply chain relationships.
2. Return ONLY publicly traded companies with valid, currently-active ticker symbols.
3. Do NOT include private companies under any circumstances.
4. For each company, cite the specific source URL from your web search.
5. Prefer undercovered, scarce bottleneck suppliers over obvious mega-caps.
6. The seed lenses are starting points only — your web search should discover additional bottlenecks.
7. EVERY evidence string MUST explicitly name "{anchor_name}" or a specific program/product of {anchor_name} \
(e.g., SpaceX, Starlink, Falcon 9, Falcon Heavy, Starship, Raptor engine, Merlin engine, Dragon). \
Evidence that says only "aerospace", "rocket", "launch vehicle", or "space" without naming {anchor_name} \
or one of its named programs is INSUFFICIENT and will be rejected. If you cannot find evidence that \
explicitly names {anchor_name} or its programs, do NOT include the company.

Anti-duplication note for this anchor:
{anti_dup}

Research timestamp: {now_iso}
Giant anchors for all nodes: include "{anchor_key.upper()}" in every node's giant_anchors array.

Deliver 10 to 15 nodes. Public companies only. Each must have a valid ticker.
For each node include real source_urls and source_titles from your web search.
Return valid JSON only — no markdown, no code fences, no commentary.
"""
    return SERENITY_SYSTEM_PROMPT.strip(), user_prompt.strip()


def get_anchor_name(anchor_key: str) -> Optional[str]:
    cfg = ANCHOR_CONFIGS.get(anchor_key.upper())
    return cfg["anchor_name"] if cfg else None


def is_configured_anchor(anchor_key: str) -> bool:
    return anchor_key.upper() in ANCHOR_CONFIGS


# ── Prompt hash ────────────────────────────────────────────────────────────────

def _compute_prompt_hash() -> str:
    combined = SERENITY_SYSTEM_PROMPT
    for key in sorted(ANCHOR_CONFIGS.keys()):
        cfg = ANCHOR_CONFIGS[key]
        combined += cfg["anchor_description"]
        combined += "".join(cfg["seed_lenses"])
        combined += cfg["anti_duplication_note"]
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()[:16]


PROMPT_HASH: str = _compute_prompt_hash()
