"""
Playbook Registry — defines all playbook configurations.

Add new playbooks by creating a PlaybookDefinition and calling register().
Enable/disable via environment variables — no code changes needed.

v2.0 — Phase 2 weight retune:
  Both playbooks bumped to v2.0.0.
  7 new Phase 2 factors integrated into weights.
  Revenue_acceleration remains stubbed (v2.1 roadmap).
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

_ENGINE_ON    = os.getenv("ENABLE_PLAYBOOK_ENGINE",   "true").lower() != "false"
_SERENITY_ON  = os.getenv("ENABLE_PLAYBOOK_SERENITY",  "true").lower() != "false"
_SJCAPITAL_ON = os.getenv("ENABLE_PLAYBOOK_SJCAPITAL", "true").lower() != "false"


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


# ── Serenity Playbook v2.0 ────────────────────────────────────────────────────
# Philosophy: Structural edge investing. Identify companies with physical
# bottleneck positions in critical supply chains. Prefer clean balance sheets,
# upcoming catalysts, small/mid-cap asymmetry. Sector momentum is secondary.
#
# v2.0 weight changes vs v1.5 (Phase 2 factors now live):
#   bottleneck_exposure:          0.22 → 0.20  (still primary, slight trim)
#   supply_chain_confirmation:    0.07 → 0.13  (NOW REAL — confirms bottleneck)
#   theme_alignment:              0.14 → 0.10  (precise, reduced as SC confirms)
#   balance_sheet_strength:       0.15 → 0.12  (still critical, slight trim)
#   evidence_freshness:           0.00 → 0.08  (NEW REAL — thesis currency)
#   catalyst_proximity:           0.12 → 0.10  (still timing signal)
#   small_cap_asymmetry:          0.12 → 0.10  (still asymmetric upside)
#   technical_confirmation:       0.10 → 0.08  (secondary)
#   sector_strength:              0.08 → 0.05  (light tailwind)
#   policy_tailwind:              0.00 → 0.04  (NEW REAL — macro tailwind)
#   Total: 1.00
#
# Penalties (v2.0 adds execution_risk):
#   dilution_risk > 70    → -8pts  (unchanged)
#   crowding_risk > 70    → -6pts  (unchanged)
#   execution_risk > 70   → -6pts  (NEW)

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
    version="2.0.0",
    factor_weights={
        "bottleneck_exposure":          0.20,
        "supply_chain_confirmation":    0.13,
        "theme_alignment":              0.10,
        "balance_sheet_strength":       0.12,
        "evidence_freshness":           0.08,
        "catalyst_proximity":           0.10,
        "small_cap_asymmetry":          0.10,
        "technical_confirmation":       0.08,
        "sector_strength":              0.05,
        "policy_tailwind":              0.04,
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
        PenaltyRule(
            factor="execution_risk",
            threshold=70.0,
            deduction=6.0,
            label="High execution risk (-6pts)",
        ),
    ],
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
    explanation_template_key="serenity_v2",
)

if _SERENITY_ON and _ENGINE_ON:
    register(_SERENITY)


# ── S&J Capital Playbook v2.0 ─────────────────────────────────────────────────
# Philosophy: Ride hot sectors, find undervalued names with accelerating revenue
# and approaching EBITDA/FCF inflection. Tighter sector requirements, avoid
# negative asymmetry. Chart confirmation required.
#
# v2.0 weight changes vs v1.5 (Phase 2 factors now live):
#   sector_strength:              0.22 → 0.20  (still primary gate)
#   ebitda_inflection_proximity:  0.08 → 0.12  (NOW REAL — key thesis driver)
#   revenue_growth:               0.10 → 0.10  (unchanged)
#   technical_confirmation:       0.12 → 0.10  (slightly reduced)
#   valuation_discount_vs_peers:  0.12 → 0.09  (slightly reduced)
#   catalyst_proximity:           0.12 → 0.09  (slightly reduced)
#   backlog_quality:              0.00 → 0.08  (NEW REAL — forward visibility)
#   policy_tailwind:              0.00 → 0.07  (NEW REAL — macro tailwind)
#   revenue_acceleration:         0.08 → 0.07  (still stub, slight trim)
#   bottleneck_exposure:          0.08 → 0.05  (secondary for S&J)
#   theme_alignment:              0.04 → 0.03  (minor)
#   Total: 1.00
#
# Penalties (v2.0 keeps same):
#   crowding_risk > 70    → -8pts
#   execution_risk > 70   → -5pts

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
    version="2.0.0",
    factor_weights={
        "sector_strength":              0.20,
        "ebitda_inflection_proximity":  0.12,
        "revenue_growth":               0.10,
        "technical_confirmation":       0.10,
        "valuation_discount_vs_peers":  0.09,
        "catalyst_proximity":           0.09,
        "backlog_quality":              0.08,
        "policy_tailwind":              0.07,
        "revenue_acceleration":         0.07,
        "bottleneck_exposure":          0.05,
        "theme_alignment":              0.03,
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
    explanation_template_key="sjcapital_v2",
)

if _SJCAPITAL_ON and _ENGINE_ON:
    register(_SJCAPITAL)
