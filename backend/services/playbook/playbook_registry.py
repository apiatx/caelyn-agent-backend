"""
Playbook Registry — single source of truth for all strategy definitions.

To add a new playbook: define a PlaybookDefinition and call register().
No other file needs to change.

Current playbooks:
  serenity   — bottleneck/supply-chain focus, clean balance sheets, small/mid-cap asymmetry
  sjcapital  — hot-sector momentum, undervaluation vs peers, revenue acceleration
"""
from __future__ import annotations

import os
from typing import Dict, List, Optional

from services.playbook.playbook_types import (
    HardFilter,
    PenaltyRule,
    PlaybookDefinition,
)

_registry: Dict[str, PlaybookDefinition] = {}


def register(pb: PlaybookDefinition) -> None:
    _registry[pb.id] = pb
    print(f"[PLAYBOOK] Registered: id={pb.id!r} enabled={pb.enabled}")


def get(playbook_id: str) -> Optional[PlaybookDefinition]:
    return _registry.get(playbook_id)


def list_enabled() -> List[PlaybookDefinition]:
    return [pb for pb in _registry.values() if pb.enabled]


def list_all() -> List[PlaybookDefinition]:
    return list(_registry.values())


# ── Feature-flag helpers ─────────────────────────────────────────────────────

def _flag(env_var: str, default: bool = True) -> bool:
    val = os.getenv(env_var, str(default)).lower()
    return val in ("true", "1", "yes")


_ENGINE_ON     = _flag("ENABLE_PLAYBOOK_ENGINE",    True)
_SERENITY_ON   = _flag("ENABLE_PLAYBOOK_SERENITY",  True)
_SJCAPITAL_ON  = _flag("ENABLE_PLAYBOOK_SJCAPITAL", True)


# ── Serenity Playbook ────────────────────────────────────────────────────────
# Philosophy: Find structurally advantaged companies sitting inside a physical
# bottleneck (supply chain, infra, manufacturing), with clean financials,
# upcoming catalysts, and small/mid-cap asymmetry. Direction > perfect timing.
#
# Factor weights (must sum ≤ 1.0):
#   bottleneck_exposure         0.20  ← primary thesis driver
#   balance_sheet_strength      0.15  ← dilution / distress protection
#   catalyst_proximity          0.15  ← event window open
#   small_cap_asymmetry         0.15  ← size-based upside potential
#   supply_chain_confirmation   0.10  ← physical confirmation
#   technical_confirmation      0.10  ← price structure not broken
#   theme_alignment             0.10  ← sector/theme fit
#   sector_strength             0.05  ← macro sector tailwind
#   Total                       1.00

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
    version="1.0.0",
    factor_weights={
        "bottleneck_exposure":       0.20,
        "balance_sheet_strength":    0.15,
        "catalyst_proximity":        0.15,
        "small_cap_asymmetry":       0.15,
        "supply_chain_confirmation": 0.10,
        "technical_confirmation":    0.10,
        "theme_alignment":           0.10,
        "sector_strength":           0.05,
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
    preferred_themes=[
        "AI infrastructure",
        "semiconductor supply chain",
        "defense & aerospace",
        "energy transition hardware",
        "critical minerals",
        "data center components",
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
# Factor weights (must sum ≤ 1.0):
#   sector_strength             0.20  ← hot sector is the primary gate
#   technical_confirmation      0.15  ← chart must confirm
#   valuation_discount_vs_peers 0.15  ← undervaluation vs sector peers
#   revenue_growth              0.10  ← must be growing
#   revenue_acceleration        0.10  ← QoQ acceleration (stub v1)
#   ebitda_inflection_proximity 0.10  ← approaching profitability inflection (stub v1)
#   bottleneck_exposure         0.10  ← bottleneck alignment
#   small_cap_asymmetry         0.05  ← some size preference
#   theme_alignment             0.05  ← thematic overlay
#   Total                       1.00

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
    version="1.0.0",
    factor_weights={
        "sector_strength":              0.20,
        "technical_confirmation":       0.15,
        "valuation_discount_vs_peers":  0.15,
        "revenue_growth":               0.10,
        "revenue_acceleration":         0.10,
        "ebitda_inflection_proximity":  0.10,
        "bottleneck_exposure":          0.10,
        "small_cap_asymmetry":          0.05,
        "theme_alignment":              0.05,
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
        "AI/machine learning",
        "cloud infrastructure",
        "biotech catalyst",
        "semiconductor",
        "energy transition",
        "fintech",
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
