"""
Playbook Registry — defines all playbook configurations.

Add new playbooks by creating a PlaybookDefinition and calling register().
Enable/disable via environment variables — no code changes needed.
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

from services.playbook.playbook_types import (
    HardFilter,
    PenaltyRule,
    PlaybookDefinition,
)

# ── Feature flags ─────────────────────────────────────────────────────────────

_ENGINE_ON   = os.getenv("ENABLE_PLAYBOOK_ENGINE",   "true").lower() != "false"
_SERENITY_ON = os.getenv("ENABLE_PLAYBOOK_SERENITY",  "true").lower() != "false"
_SJCAPITAL_ON= os.getenv("ENABLE_PLAYBOOK_SJCAPITAL", "true").lower() != "false"


# ── Registry store ────────────────────────────────────────────────────────────

_REGISTRY: Dict[str, PlaybookDefinition] = {}


def register(pb: PlaybookDefinition) -> None:
    _REGISTRY[pb.id] = pb


def get(playbook_id: str) -> Optional[PlaybookDefinition]:
    return _REGISTRY.get(playbook_id)


def list_all() -> List[PlaybookDefinition]:
    return list(_REGISTRY.values())


def list_enabled() -> List[PlaybookDefinition]:
    return [pb for pb in _REGISTRY.values() if pb.enabled]


# ── Serenity Playbook ─────────────────────────────────────────────────────────
# Philosophy: Structural edge investing. Identify companies with physical
# bottleneck positions in critical supply chains. Prefer clean balance sheets,
# upcoming catalysts, small/mid-cap asymmetry. Sector momentum is secondary.
#
# v1.5 weight changes vs v1.0:
#   bottleneck_exposure:       0.20 → 0.22  (primary thesis driver, up)
#   theme_alignment:           0.10 → 0.14  (thematic precision matters more, up)
#   sector_strength:           0.05 → 0.08  (light tailwind signal, up)
#   catalyst_proximity:        0.15 → 0.12  (still important, slightly reduced)
#   small_cap_asymmetry:       0.15 → 0.12  (still important, slightly reduced)
#   supply_chain_confirmation: 0.10 → 0.07  (stub, reduced until real data)
#   balance_sheet_strength:    0.15 → 0.15  (unchanged)
#   technical_confirmation:    0.10 → 0.10  (unchanged)
#   Total: 1.00

_SERENITY = PlaybookDefinition(
    id="serenity",
    name="Serenity",
    short_label="Serenity",
    description=(
        "Bottleneck-exposure strategy focused on structurally advantaged companies "
        "in physical supply chains with clean balance sheets, upcoming catalysts, "
        "and small/mid-cap asymmetric upside. Favors direction over perfect timing."
    ),
    enabled=_ENGINE_ON and _SERENITY_ON,
    version="1.5.0",
    factor_weights={
        "bottleneck_exposure":       0.22,
        "balance_sheet_strength":    0.15,
        "theme_alignment":           0.14,
        "catalyst_proximity":        0.12,
        "small_cap_asymmetry":       0.12,
        "technical_confirmation":    0.10,
        "sector_strength":           0.08,
        "supply_chain_confirmation": 0.07,
    },
    hard_filters=[
        HardFilter(
            field="mkt_cap",
            op="gte",
            value=50_000_000,
            label="Market cap below $50M minimum",
        ),
        HardFilter(
            field="debt_to_equity",
            op="lte",
            value=5.0,
            label="Extreme leverage (D/E > 5.0)",
        ),
    ],
    penalty_rules=[
        PenaltyRule(
            factor="dilution_risk",
            threshold=70.0,
            deduction=8.0,
            label="Elevated dilution risk (-8pts)",
        ),
        PenaltyRule(
            factor="crowding_risk",
            threshold=70.0,
            deduction=6.0,
            label="Crowded positioning risk (-6pts)",
        ),
    ],
    # Internal theme IDs — must match keys in theme_map.ALL_THEMES
    preferred_themes=[
        "photonics_cpo",
        "advanced_packaging_test",
        "semicap_supply_chain",
        "defense_optics",
        "grid_transformers",
        "space",
        "memory",
        "ai_infrastructure",
    ],
    preferred_sectors=[
        "Technology",
        "Industrials",
        "Basic Materials",
        "Energy",
        "Healthcare",
    ],
    entry_style="Staged entry on confirmation; size up on strength.",
    exit_style="Partial profit at catalyst resolution; hold core through structural thesis.",
    positioning_style="Concentrated small/mid-cap positions, 3–8% per name.",
    ui_color="#6366f1",
    explanation_template_key="serenity_v1",
)

if _SERENITY_ON and _ENGINE_ON:
    register(_SERENITY)


# ── S&J Capital Playbook ─────────────────────────────────────────────────────
# Philosophy: Ride hot sectors, find undervalued names with accelerating revenue
# and approaching EBITDA/FCF inflection. Tighter sector requirements, avoid
# negative asymmetry. Chart confirmation required.
#
# v1.5 weight changes vs v1.0:
#   sector_strength:               0.20 → 0.22  (primary gate, up)
#   catalyst_proximity:            0.00 → 0.12  (NEW — timely setups matter)
#   technical_confirmation:        0.15 → 0.12  (slightly reduced)
#   valuation_discount_vs_peers:   0.15 → 0.12  (slightly reduced)
#   revenue_acceleration:          0.10 → 0.08  (still stub, slightly reduced)
#   ebitda_inflection_proximity:   0.10 → 0.08  (still stub, slightly reduced)
#   bottleneck_exposure:           0.10 → 0.08  (secondary concern for S&J)
#   small_cap_asymmetry:           0.05 → 0.04  (minor)
#   theme_alignment:               0.05 → 0.04  (minor)
#   revenue_growth:                0.10 → 0.10  (unchanged)
#   Total: 1.00

_SJCAPITAL = PlaybookDefinition(
    id="sjcapital",
    name="S&J Capital",
    short_label="S&J",
    description=(
        "Sector-momentum strategy targeting undervalued names in hot sectors "
        "with accelerating revenue and approaching EBITDA inflection. "
        "Requires chart confirmation and avoids negative asymmetry."
    ),
    enabled=_ENGINE_ON and _SJCAPITAL_ON,
    version="1.5.0",
    factor_weights={
        "sector_strength":              0.22,
        "technical_confirmation":       0.12,
        "valuation_discount_vs_peers":  0.12,
        "catalyst_proximity":           0.12,
        "revenue_growth":               0.10,
        "revenue_acceleration":         0.08,
        "ebitda_inflection_proximity":  0.08,
        "bottleneck_exposure":          0.08,
        "small_cap_asymmetry":          0.04,
        "theme_alignment":              0.04,
    },
    hard_filters=[
        HardFilter(
            field="mkt_cap",
            op="gte",
            value=200_000_000,
            label="Market cap below $200M minimum (S&J tighter size requirement)",
        ),
        HardFilter(
            field="debt_to_equity",
            op="lte",
            value=3.0,
            label="High leverage (D/E > 3.0) — negative asymmetry risk",
        ),
    ],
    penalty_rules=[
        PenaltyRule(
            factor="crowding_risk",
            threshold=70.0,
            deduction=8.0,
            label="Crowded positioning risk (-8pts)",
        ),
        PenaltyRule(
            factor="execution_risk",
            threshold=70.0,
            deduction=5.0,
            label="High execution risk (-5pts)",
        ),
    ],
    # Internal theme IDs — must match keys in theme_map.ALL_THEMES
    preferred_themes=[
        "ai_infrastructure",
        "neocloud",
        "ai_software",
        "ai_power_energy",
        "energy_transition",
        "biotech_catalyst",
    ],
    preferred_sectors=[
        "Technology",
        "Healthcare",
        "Consumer Cyclical",
        "Communication Services",
        "Energy",
    ],
    entry_style="Enter on sector strength confirmation; add on momentum.",
    exit_style="Cut quickly on sector rotation signals; book gains at EBITDA inflection.",
    positioning_style="5–10% per name, sector-concentrated, momentum-aware.",
    ui_color="#10b981",
    explanation_template_key="sjcapital_v1",
)

if _SJCAPITAL_ON and _ENGINE_ON:
    register(_SJCAPITAL)
