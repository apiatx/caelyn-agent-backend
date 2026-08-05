"""
Tests for the theme hierarchy metadata added in the canonical registry:
  - parent_theme_id assignments
  - rollup_sector_ids assignments
  - display-name changes with backward-compat aliases
  - classification promotions (sub_theme → theme for parent nodes)
  - validate_theme_hierarchy() structural checks

Run with:
    cd backend && python -m pytest tests/test_theme_hierarchy.py -v
"""
from __future__ import annotations

import pytest

from services.theme_rs_universe import (
    THEME_RS_UNIVERSE as REG,
    validate_theme_hierarchy,
)


# ── helpers ───────────────────────────────────────────────────────────────────

def node(tid: str) -> dict:
    assert tid in REG, f"registry node {tid!r} not found"
    return REG[tid]


# ── 1. validate_theme_hierarchy passes on the live registry ──────────────────

def test_validate_hierarchy_clean():
    """The canonical registry must pass all structural checks with zero errors."""
    errors = validate_theme_hierarchy()
    assert errors == [], "Hierarchy validation errors:\n" + "\n".join(errors)


# ── 2. Sector nodes exist and have correct classification ─────────────────────

@pytest.mark.parametrize("sector_id", [
    "technology", "materials", "energy", "industrials", "utilities",
    "financials", "healthcare", "real_estate", "communication_services",
    "consumer_discretionary", "consumer_staples",
])
def test_sector_nodes_exist(sector_id):
    n = node(sector_id)
    assert n["classification"] == "sector"
    assert n.get("parent_sector") is None


# ── 3. Parent theme promotions ────────────────────────────────────────────────

@pytest.mark.parametrize("tid", [
    "metals_mining", "semiconductors", "oil_gas", "software", "defense",
])
def test_parent_theme_classification(tid):
    """Promoted parent nodes must have classification='theme'."""
    n = node(tid)
    assert n["classification"] == "theme", (
        f"{tid} expected classification='theme', got {n['classification']!r}"
    )


@pytest.mark.parametrize("tid", [
    "metals_mining", "semiconductors", "oil_gas", "software", "defense",
])
def test_parent_theme_has_no_parent_theme_id(tid):
    """Parent theme nodes must not themselves have a parent_theme_id."""
    n = node(tid)
    assert n.get("parent_theme_id") is None, (
        f"{tid} should not have parent_theme_id, got {n.get('parent_theme_id')!r}"
    )


# ── 4. Materials → Metals & Mining hierarchy ─────────────────────────────────

def test_metals_mining_rollup():
    assert node("metals_mining")["rollup_sector_ids"] == ["materials"]

def test_gold_parent():
    assert node("gold")["parent_theme_id"] == "metals_mining"

def test_silver_parent():
    assert node("silver")["parent_theme_id"] == "metals_mining"

def test_copper_miners_parent():
    assert node("copper_miners")["parent_theme_id"] == "metals_mining"

def test_rare_earth_parent():
    assert node("rare_earth")["parent_theme_id"] == "metals_mining"


# ── 5. Display-label changes with backward-compat aliases ────────────────────

def test_copper_display_name():
    """copper_miners node must now display as 'Copper'."""
    assert node("copper_miners")["display_name"] == "Copper"

def test_copper_alias_preserved():
    """Prior display label 'copper_miners' must be preserved as an alias."""
    assert "copper_miners" in (node("copper_miners").get("aliases") or [])

def test_rare_earth_display_name():
    """rare_earth node must now display as 'Rare Earth Elements'."""
    assert node("rare_earth")["display_name"] == "Rare Earth Elements"

def test_rare_earth_alias_preserved():
    """Prior display label alias 'rare_earth_metals' must be present."""
    assert "rare_earth_metals" in (node("rare_earth").get("aliases") or [])


# ── 6. Technology → Semiconductors hierarchy ─────────────────────────────────

def test_semiconductors_rollup():
    assert node("semiconductors")["rollup_sector_ids"] == ["technology"]

def test_memory_storage_parent():
    assert node("memory_storage")["parent_theme_id"] == "semiconductors"

def test_semicap_equipment_parent():
    assert node("semicap_equipment")["parent_theme_id"] == "semiconductors"

def test_substrates_packaging_parent():
    assert node("substrates_packaging")["parent_theme_id"] == "semiconductors"

def test_semicap_equipment_id_frozen():
    """theme_id 'semicap_equipment' must be unchanged — downstream refs depend on it."""
    assert "semicap_equipment" in REG

def test_semicap_equipment_aliases_preserved():
    """All historical aliases for semicap_equipment must still be present."""
    expected = {"semicap", "semiconductor_equipment", "semi_equipment",
                "semi_materials", "semiconductor_materials",
                "semi_equipment_and_materials", "semicap_equipment"}
    actual = set(node("semicap_equipment").get("aliases") or [])
    missing = expected - actual
    assert not missing, f"Missing semicap_equipment aliases: {missing}"


# ── 7. Energy → Oil & Gas hierarchy ──────────────────────────────────────────

def test_oil_gas_rollup():
    assert node("oil_gas")["rollup_sector_ids"] == ["energy"]

def test_lng_gas_parent():
    assert node("lng_gas")["parent_theme_id"] == "oil_gas"

def test_oil_services_parent():
    assert node("oil_services")["parent_theme_id"] == "oil_gas"


# ── 8. Technology → Software hierarchy ───────────────────────────────────────

def test_software_rollup():
    assert node("software")["rollup_sector_ids"] == ["technology"]

def test_cloud_software_parent():
    assert node("cloud_software")["parent_theme_id"] == "software"

def test_cybersecurity_parent():
    assert node("cybersecurity")["parent_theme_id"] == "software"


# ── 9. Industrials → Defense hierarchy ───────────────────────────────────────

def test_defense_rollup():
    assert node("defense")["rollup_sector_ids"] == ["industrials"]

def test_drones_parent():
    assert node("drones")["parent_theme_id"] == "defense"

def test_space_is_independent():
    """Space Economy must remain independent (no parent_theme_id)."""
    assert node("space").get("parent_theme_id") is None, (
        "space was forced under defense without repository evidence — leave independent"
    )


# ── 10. Cross-sector rollup_sector_ids ───────────────────────────────────────

def test_clean_energy_rollup():
    rollup = set(node("clean_energy")["rollup_sector_ids"])
    assert rollup == {"utilities", "industrials", "energy"}

def test_datacenter_infra_rollup():
    rollup = set(node("datacenter_infra")["rollup_sector_ids"])
    assert rollup == {"technology", "utilities", "real_estate"}


# ── 11. rollup_sector_ids values must all be canonical sector IDs ─────────────

def test_all_rollup_ids_are_sectors():
    bad = []
    for tid, meta in REG.items():
        for sid in (meta.get("rollup_sector_ids") or []):
            if REG.get(sid, {}).get("classification") != "sector":
                bad.append(f"{tid}.rollup_sector_ids contains {sid!r} (not a sector)")
    assert bad == [], "\n".join(bad)


# ── 12. parent_theme_id referential integrity ─────────────────────────────────

def test_all_parent_theme_ids_exist():
    bad = []
    for tid, meta in REG.items():
        ptid = meta.get("parent_theme_id")
        if ptid is not None and ptid not in REG:
            bad.append(f"{tid}: parent_theme_id={ptid!r} not in registry")
    assert bad == [], "\n".join(bad)


def test_all_parent_theme_ids_are_theme_class():
    bad = []
    for tid, meta in REG.items():
        ptid = meta.get("parent_theme_id")
        if ptid is not None:
            ref_cls = REG.get(ptid, {}).get("classification")
            if ref_cls != "theme":
                bad.append(
                    f"{tid}: parent_theme_id={ptid!r} has classification={ref_cls!r}"
                )
    assert bad == [], "\n".join(bad)


# ── 13. Existing IDs unchanged ────────────────────────────────────────────────

@pytest.mark.parametrize("tid", [
    "gold", "silver", "copper_miners", "rare_earth",
    "semiconductors", "memory_storage", "semicap_equipment", "substrates_packaging",
    "oil_gas", "lng_gas", "oil_services",
    "software", "cloud_software", "cybersecurity",
    "defense", "drones", "space",
    "clean_energy", "datacenter_infra", "metals_mining",
])
def test_canonical_id_unchanged(tid):
    """Every canonical ID involved in hierarchy work must still exist unchanged."""
    assert tid in REG, f"canonical ID {tid!r} was removed or renamed"


# ── 14. Existing endpoint fields still present ────────────────────────────────

def test_list_endpoint_fields_present():
    """
    All pre-existing fields that GET /api/themes/list returns must still be
    present on every registry node (backward compat).
    """
    required_keys = {
        "classification", "parent_sector", "proxy_type", "proxy_symbols",
        "candidate_symbols", "sector_tags", "keywords", "macro_sensitivities",
        "display_name",
    }
    missing = []
    for tid, meta in REG.items():
        for k in required_keys:
            if k not in meta:
                missing.append(f"{tid}: missing key {k!r}")
    assert missing == [], "\n".join(missing)


# ── 15. Malformed / cyclic fixtures rejected by validator ─────────────────────

def test_validator_rejects_unknown_parent_theme_id():
    bad_reg = dict(REG)
    bad_reg["_test_bad"] = {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "display_name": "Bad Node",
        "parent_theme_id": "nonexistent_parent",
        "proxy_type": "basket",
        "proxy_symbols": [],
        "candidate_symbols": [],
        "sector_tags": [],
        "keywords": [],
        "macro_sensitivities": [],
    }
    errors = validate_theme_hierarchy(bad_reg)
    assert any("nonexistent_parent" in e for e in errors), errors


def test_validator_rejects_self_parent():
    bad_reg = dict(REG)
    bad_reg["_test_self"] = {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Self-Parent",
        "parent_theme_id": "_test_self",
        "proxy_type": "basket",
        "proxy_symbols": [],
        "candidate_symbols": [],
        "sector_tags": [],
        "keywords": [],
        "macro_sensitivities": [],
    }
    errors = validate_theme_hierarchy(bad_reg)
    assert any("itself" in e or "_test_self" in e for e in errors), errors


def test_validator_rejects_parent_with_sector_classification():
    """A node must not point to a sector node as parent_theme_id."""
    bad_reg = dict(REG)
    bad_reg["_test_sector_parent"] = {
        "classification": "sub_theme",
        "parent_sector": "technology",
        "display_name": "Bad Parent Ref",
        "parent_theme_id": "technology",   # technology is a sector, not a theme
        "proxy_type": "basket",
        "proxy_symbols": [],
        "candidate_symbols": [],
        "sector_tags": [],
        "keywords": [],
        "macro_sensitivities": [],
    }
    errors = validate_theme_hierarchy(bad_reg)
    assert any("technology" in e and "classification" in e for e in errors), errors


def test_validator_rejects_cycle():
    bad_reg = {
        "node_a": {
            "classification": "theme",
            "parent_sector": "technology",
            "display_name": "Node A",
            "parent_theme_id": "node_b",
            "proxy_type": "basket",
            "proxy_symbols": [],
            "candidate_symbols": [],
            "sector_tags": [],
            "keywords": [],
            "macro_sensitivities": [],
        },
        "node_b": {
            "classification": "theme",
            "parent_sector": "technology",
            "display_name": "Node B",
            "parent_theme_id": "node_a",
            "proxy_type": "basket",
            "proxy_symbols": [],
            "candidate_symbols": [],
            "sector_tags": [],
            "keywords": [],
            "macro_sensitivities": [],
        },
        "technology": {
            "classification": "sector",
            "parent_sector": None,
            "display_name": "Technology",
            "proxy_type": "etf",
            "proxy_symbols": ["XLK"],
            "candidate_symbols": [],
            "sector_tags": [],
            "keywords": [],
            "macro_sensitivities": [],
        },
    }
    errors = validate_theme_hierarchy(bad_reg)
    assert any("cycle" in e for e in errors), errors


def test_validator_rejects_non_sector_rollup():
    """rollup_sector_ids must only reference sector-classified nodes."""
    bad_reg = dict(REG)
    bad_reg["_test_bad_rollup"] = {
        "classification": "theme",
        "parent_sector": "technology",
        "display_name": "Bad Rollup",
        "rollup_sector_ids": ["gold"],    # gold is a theme, not a sector
        "proxy_type": "basket",
        "proxy_symbols": [],
        "candidate_symbols": [],
        "sector_tags": [],
        "keywords": [],
        "macro_sensitivities": [],
    }
    errors = validate_theme_hierarchy(bad_reg)
    assert any("gold" in e for e in errors), errors


# ── 16. Direct parent-theme member with no subtheme ──────────────────────────

def test_direct_parent_member_no_subtheme():
    """
    clean_energy is a direct member of the top-level hierarchy with no
    parent_theme_id — a parent-theme member with no subtheme above it.
    """
    n = node("clean_energy")
    assert n.get("parent_theme_id") is None
    assert n["classification"] == "theme"
    assert "rollup_sector_ids" in n


# ── 17. Ticker with primary + additional membership (simulate) ────────────────

def test_theme_ids_union_logic():
    """
    Demonstrate that theme_ids = primary + additional memberships.
    e.g. a ticker in both 'semiconductors' (primary) and 'memory_storage' (additional)
    should have theme_ids = ['semiconductors', 'memory_storage'].
    """
    primary = "semiconductors"
    additional = ["memory_storage"]
    theme_ids = [primary] + [t for t in additional if t != primary]
    assert "semiconductors" in theme_ids
    assert "memory_storage" in theme_ids

    # subtheme_ids = those that have a parent_theme_id in registry
    subtheme_ids = [t for t in theme_ids if REG.get(t, {}).get("parent_theme_id")]
    assert "memory_storage" in subtheme_ids  # memory_storage → semiconductors
    assert "semiconductors" not in subtheme_ids  # semiconductors has no parent_theme_id
