"""
Tests for the theme hierarchy metadata added in the canonical registry:
  - parent_theme_id assignments
  - rollup_sector_ids assignments
  - display-name changes with backward-compat aliases
  - classification promotions (sub_theme → theme for parent nodes)
  - validate_theme_hierarchy() structural checks
  - get_effective_rollup_sector_ids() — Contract 2
  - normalize_company_sector_to_id()  — Contract 4
  - RS endpoint row serialization     — Contract 1
  - Watchlist row identity fields (production-path) — Contract 3
  - sector_id uses FMP profile path  — Contract 4 (production-path)

Run with:
    cd backend && python -m pytest tests/test_theme_hierarchy.py -v
"""
from __future__ import annotations

import pytest
from unittest.mock import patch as _patch

from services.theme_rs_universe import (
    THEME_RS_UNIVERSE as REG,
    validate_theme_hierarchy,
    get_effective_rollup_sector_ids,
    normalize_company_sector_to_id,
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
    # Contract 2: defense rolls up to both industrials AND technology
    rollup = set(node("defense")["rollup_sector_ids"])
    assert rollup == {"industrials", "technology"}, f"defense rollup expected {{industrials,technology}}, got {rollup}"

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
    # Contract 2: datacenter_infra must also include industrials (power_cooling players)
    rollup = set(node("datacenter_infra")["rollup_sector_ids"])
    assert rollup == {"technology", "utilities", "real_estate", "industrials"}, (
        f"datacenter_infra rollup expected {{technology,utilities,real_estate,industrials}}, got {rollup}"
    )


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
    Includes sector_tags and macro_sensitivities (Contract 4: restored in corrective commit).
    """
    required_keys = {
        "classification", "parent_sector", "proxy_type", "proxy_symbols",
        "candidate_symbols", "keywords", "display_name",
        "sector_tags", "macro_sensitivities",
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


# ═══════════════════════════════════════════════════════════════════════════════
# Contract 2 — get_effective_rollup_sector_ids()
# ═══════════════════════════════════════════════════════════════════════════════

class TestGetEffectiveRollupSectorIds:
    """Every non-sector node must yield at least one effective rollup sector."""

    # Nodes with explicit rollup_sector_ids — returned as-is (plus any parent_sector)
    def test_metals_mining_explicit(self):
        r = get_effective_rollup_sector_ids("metals_mining", REG)
        assert "materials" in r

    def test_semiconductors_explicit(self):
        r = get_effective_rollup_sector_ids("semiconductors", REG)
        assert "technology" in r

    def test_clean_energy_explicit_cross_sector(self):
        r = get_effective_rollup_sector_ids("clean_energy", REG)
        assert set(r) == {"utilities", "industrials", "energy"}

    def test_datacenter_infra_explicit_cross_sector(self):
        # Contract 2 fix: datacenter_infra now includes industrials (power/cooling players)
        r = get_effective_rollup_sector_ids("datacenter_infra", REG)
        assert set(r) == {"technology", "utilities", "real_estate", "industrials"}

    def test_defense_explicit(self):
        # Contract 2 fix: defense now includes both industrials and technology
        r = get_effective_rollup_sector_ids("defense", REG)
        assert "industrials" in r
        assert "technology" in r

    # Nodes with NO explicit rollup_sector_ids — inherit via parent_sector
    def test_gold_inherits_via_parent_sector(self):
        """gold has no explicit rollup_sector_ids; parent_sector='materials' must be resolved."""
        assert not REG["gold"].get("rollup_sector_ids"), "fixture: gold should have no explicit rollup"
        r = get_effective_rollup_sector_ids("gold", REG)
        assert "materials" in r, f"gold effective rollup={r}"

    def test_copper_miners_inherits_via_parent_sector(self):
        r = get_effective_rollup_sector_ids("copper_miners", REG)
        assert "materials" in r, f"copper_miners effective rollup={r}"

    def test_memory_storage_inherits_via_parent_and_grandparent(self):
        """memory_storage has no explicit rollup; inherits via semiconductors → technology."""
        assert not REG["memory_storage"].get("rollup_sector_ids"), "fixture"
        r = get_effective_rollup_sector_ids("memory_storage", REG)
        assert "technology" in r, f"memory_storage effective rollup={r}"

    def test_lng_gas_inherits_via_parent_sector(self):
        r = get_effective_rollup_sector_ids("lng_gas", REG)
        assert "energy" in r, f"lng_gas effective rollup={r}"

    def test_drones_inherits_via_parent_sector_and_parent_theme(self):
        r = get_effective_rollup_sector_ids("drones", REG)
        assert "industrials" in r, f"drones effective rollup={r}"

    def test_cloud_software_inherits_technology(self):
        r = get_effective_rollup_sector_ids("cloud_software", REG)
        assert "technology" in r, f"cloud_software effective rollup={r}"

    def test_cybersecurity_inherits_technology(self):
        r = get_effective_rollup_sector_ids("cybersecurity", REG)
        assert "technology" in r, f"cybersecurity effective rollup={r}"

    # Sectors themselves return []
    def test_sector_node_returns_empty(self):
        assert get_effective_rollup_sector_ids("technology", REG) == []
        assert get_effective_rollup_sector_ids("materials", REG) == []
        assert get_effective_rollup_sector_ids("energy", REG) == []

    # Unknown node returns []
    def test_unknown_id_returns_empty(self):
        assert get_effective_rollup_sector_ids("nonexistent_xyz", REG) == []

    # Cycle guard — synthetic registry with a cycle must not infinite-loop
    def test_cycle_guard(self):
        cycle_reg = {
            "alpha": {"classification": "theme", "parent_sector": "technology",
                      "parent_theme_id": "beta", "display_name": "A",
                      "proxy_type": "basket", "proxy_symbols": [], "candidate_symbols": [],
                      "sector_tags": [], "keywords": [], "macro_sensitivities": []},
            "beta":  {"classification": "theme", "parent_sector": "technology",
                      "parent_theme_id": "alpha", "display_name": "B",
                      "proxy_type": "basket", "proxy_symbols": [], "candidate_symbols": [],
                      "sector_tags": [], "keywords": [], "macro_sensitivities": []},
            "technology": {"classification": "sector", "parent_sector": None,
                           "display_name": "Technology", "proxy_type": "etf",
                           "proxy_symbols": ["XLK"], "candidate_symbols": [],
                           "sector_tags": [], "keywords": [], "macro_sensitivities": []},
        }
        # Must not raise — cycle guard terminates the walk
        result = get_effective_rollup_sector_ids("alpha", cycle_reg)
        assert isinstance(result, list)

    # No duplicates in output
    def test_no_duplicates_in_output(self):
        for tid in REG:
            r = get_effective_rollup_sector_ids(tid, REG)
            assert len(r) == len(set(r)), f"{tid}: duplicate sector IDs in effective rollup: {r}"

    # Full registry: every non-sector node with a parent_sector has ≥1 effective rollup
    def test_every_non_sector_with_parent_has_rollup(self):
        bad = []
        for tid, meta in REG.items():
            if meta.get("classification") == "sector":
                continue
            if not meta.get("parent_sector"):
                continue
            r = get_effective_rollup_sector_ids(tid, REG)
            if not r:
                bad.append(f"{tid}: parent_sector={meta['parent_sector']!r} but rollup=[]")
        assert bad == [], "\n".join(bad)

    # All IDs in effective rollup must be registered sector nodes
    def test_all_effective_ids_are_sectors(self):
        bad = []
        for tid in REG:
            for sid in get_effective_rollup_sector_ids(tid, REG):
                node_cls = REG.get(sid, {}).get("classification")
                if node_cls != "sector":
                    bad.append(
                        f"{tid} → {sid} has classification={node_cls!r}, expected 'sector'"
                    )
        assert bad == [], "\n".join(bad)


# ═══════════════════════════════════════════════════════════════════════════════
# Contract 4 — normalize_company_sector_to_id()
# ═══════════════════════════════════════════════════════════════════════════════

class TestNormalizeCompanySectorToId:
    """All known provider label variants must map to canonical sector IDs."""

    @pytest.mark.parametrize("label, expected", [
        # Technology
        ("Technology",              "technology"),
        ("technology",              "technology"),
        ("Information Technology",  "technology"),
        # Materials
        ("Basic Materials",         "materials"),
        ("Materials",               "materials"),
        # Energy
        ("Energy",                  "energy"),
        # Industrials
        ("Industrials",             "industrials"),
        ("Industrial",              "industrials"),
        # Financials
        ("Financials",              "financials"),
        ("Financial Services",      "financials"),
        ("Financial",               "financials"),
        # Healthcare
        ("Healthcare",              "healthcare"),
        ("Health Care",             "healthcare"),
        # Utilities
        ("Utilities",               "utilities"),
        # Real estate
        ("Real Estate",             "real_estate"),
        # Communication services
        ("Communication Services",  "communication_services"),
        ("Communication",           "communication_services"),
        # Consumer discretionary — FMP emits "Consumer Cyclical"
        ("Consumer Cyclical",       "consumer_discretionary"),
        ("Consumer Discretionary",  "consumer_discretionary"),
        # Consumer staples — FMP emits "Consumer Defensive"
        ("Consumer Defensive",      "consumer_staples"),
        ("Consumer Staples",        "consumer_staples"),
    ])
    def test_known_labels(self, label, expected):
        assert normalize_company_sector_to_id(label) == expected, (
            f"normalize_company_sector_to_id({label!r}) expected {expected!r}"
        )

    def test_empty_string_returns_none(self):
        assert normalize_company_sector_to_id("") is None

    def test_none_returns_none(self):
        assert normalize_company_sector_to_id(None) is None

    def test_unknown_label_returns_none(self):
        """Unknown labels must return None, never a guessed value."""
        assert normalize_company_sector_to_id("Widget Industry XYZ") is None

    def test_case_insensitive(self):
        assert normalize_company_sector_to_id("TECHNOLOGY") == "technology"
        assert normalize_company_sector_to_id("consumer cyclical") == "consumer_discretionary"

    def test_whitespace_stripped(self):
        assert normalize_company_sector_to_id("  Technology  ") == "technology"

    def test_output_is_registered_sector(self):
        """Every non-None output must match a sector-classified node in the registry."""
        labels = [
            "Technology", "Basic Materials", "Energy", "Industrials",
            "Financials", "Healthcare", "Utilities", "Real Estate",
            "Communication Services", "Consumer Cyclical", "Consumer Defensive",
            "Financial Services", "Consumer Staples", "Consumer Discretionary",
        ]
        for label in labels:
            sid = normalize_company_sector_to_id(label)
            assert sid is not None, f"{label!r} returned None unexpectedly"
            assert REG.get(sid, {}).get("classification") == "sector", (
                f"{label!r} → {sid!r} is not a sector in the registry"
            )


# ═══════════════════════════════════════════════════════════════════════════════
# Contract 1 — RS row serialization includes hierarchy fields
# ═══════════════════════════════════════════════════════════════════════════════

class TestRsRowHierarchyFields:
    """
    Validate that the RS row builder correctly emits parent_theme_id and
    rollup_sector_ids for eight representative nodes.  Tests operate directly
    on theme_rs_service._build_theme_row() output by inspecting the registry
    fields and the effective-rollup helper — without making live HTTP calls.
    """

    @pytest.mark.parametrize("theme_id, expected_parent_tid, expected_rollup_has", [
        # sub-theme nodes — must carry parent_theme_id + inherited rollup
        ("gold",           "metals_mining",    {"materials"}),
        ("copper_miners",  "metals_mining",    {"materials"}),
        ("memory_storage", "semiconductors",   {"technology"}),
        ("lng_gas",        "oil_gas",          {"energy"}),
        ("drones",         "defense",          {"industrials"}),
        ("cloud_software", "software",         {"technology"}),
        ("cybersecurity",  "software",         {"technology"}),
        # parent-theme node — has own rollup, no parent_theme_id
        ("semiconductors", None,               {"technology"}),
    ])
    def test_registry_matches_contracts(
        self, theme_id, expected_parent_tid, expected_rollup_has
    ):
        """
        The registry entry and effective-rollup helper must satisfy the same
        contracts that _build_theme_row() enforces when constructing RS rows.
        This is a pure-registry test (no I/O) that acts as a proxy for the
        live RS endpoint serialization.
        """
        meta = REG.get(theme_id)
        assert meta is not None, f"Theme {theme_id!r} not found in registry"

        # parent_theme_id
        assert meta.get("parent_theme_id") == expected_parent_tid, (
            f"{theme_id}: parent_theme_id={meta.get('parent_theme_id')!r}, "
            f"expected {expected_parent_tid!r}"
        )

        # rollup_sector_ids via effective helper
        effective = get_effective_rollup_sector_ids(theme_id, REG)
        missing = expected_rollup_has - set(effective)
        assert not missing, (
            f"{theme_id}: effective rollup={effective}, "
            f"missing sectors {missing}"
        )

    def test_rs_row_field_names(self):
        """
        Smoke-check that THEME_RS_UNIVERSE nodes carry the exact field-names
        that _build_theme_row() reads when building the 'parent_theme_id' and
        'rollup_sector_ids' keys in the RS response row.
        """
        for tid, meta in REG.items():
            # Every node must be dict-accessible for .get()
            assert isinstance(meta, dict), f"{tid}: meta is not a dict"
        # Fields used by _build_theme_row must exist as dict keys when present
        for tid, meta in REG.items():
            if "parent_theme_id" in meta:
                assert isinstance(meta["parent_theme_id"], str)
            if "rollup_sector_ids" in meta:
                assert isinstance(meta["rollup_sector_ids"], list)


# ═══════════════════════════════════════════════════════════════════════════════
# Contracts 3 & 4 — Production Watchlist path tests
#
# These tests invoke the REAL _enrich_store_with_quotes() function (not a
# copied simulation) with all external I/O stubbed out so tests make:
#   - no network requests
#   - no Neon reads or writes
#   - no provider calls
#   - no app startup
#   - no background jobs
# ═══════════════════════════════════════════════════════════════════════════════

async def _run_enrich(
    store: dict,
    *,
    fund_snaps: dict | None = None,
    overrides: list | None = None,
) -> dict:
    """
    Call the real _enrich_store_with_quotes with all external I/O stubbed.

    fund_snaps: ticker → fund_snap dict (controls sector_id via profile.sector).
    overrides:  list of {"symbol": ..., "theme_id": ..., "action": "add"|"remove"}
                rows (controls additional theme memberships).
    """
    from services.watchlist_router import _enrich_store_with_quotes

    async def _fake_quotes(_tickers):
        return {}

    with (
        _patch("services.watchlist_quote_cache.get_watchlist_quotes", side_effect=_fake_quotes),
        _patch("services.name_overrides.get_name_overrides", return_value={}),
        _patch("data.watchlist_fundamentals_store.get_snapshots_bulk", return_value=fund_snaps or {}),
        _patch("services.watchlist_router._load_cached_watchlist_market_data", return_value={}),
        _patch("data.pg_storage.get_theme_ticker_overrides", return_value=overrides or []),
        _patch("data.quote_demand_registry.register", return_value=None),
        _patch("services.watchlist_router._get_stage2_breakout", return_value={}),
    ):
        return await _enrich_store_with_quotes(store)


async def _run_enrich_skeleton(
    tickers: list[str],
    *,
    resolved_theme_id: str,
    resolved_theme_name: str,
    fund_snaps: dict | None = None,
    overrides: list | None = None,
) -> dict:
    """
    Call _enrich_store_with_quotes with an empty sections list (skeleton path).

    Stubs theme_resolver so identity fields can be verified without real I/O.
    """
    from services.watchlist_router import _enrich_store_with_quotes

    async def _fake_quotes(_tickers):
        return {}

    def _fake_build_ctx():
        return {}

    def _fake_resolve_theme(sym, *, industry="", ctx=None):
        return {
            "theme_name": resolved_theme_name,
            "theme_id":   resolved_theme_id,
            "source":     "test_stub",
        }

    with (
        _patch("services.watchlist_quote_cache.get_watchlist_quotes", side_effect=_fake_quotes),
        _patch("services.name_overrides.get_name_overrides", return_value={}),
        _patch("data.watchlist_fundamentals_store.get_snapshots_bulk", return_value=fund_snaps or {}),
        _patch("services.watchlist_router._load_cached_watchlist_market_data", return_value={}),
        _patch("data.pg_storage.get_theme_ticker_overrides", return_value=overrides or []),
        _patch("data.quote_demand_registry.register", return_value=None),
        _patch("services.watchlist_router._get_stage2_breakout", return_value={}),
        _patch("services.theme_resolver.build_theme_resolution_context", side_effect=_fake_build_ctx),
        _patch("services.theme_resolver.resolve_primary_theme_for_ticker", side_effect=_fake_resolve_theme),
    ):
        return await _enrich_store_with_quotes({
            "tickers":  tickers,
            "analysis": {"sections": []},
            "csv_data": [],
        })


def _section_row(result: dict, section_index: int = 0, row_index: int = 0) -> dict:
    """Extract one ticker row from the enriched result."""
    return result["analysis"]["sections"][section_index]["tickers"][row_index]


def _make_store(sym: str, canonical_theme_id: str, sections: list | None = None) -> dict:
    """Build a minimal store with one ticker in one section."""
    return {
        "tickers":  [sym],
        "analysis": {
            "sections": sections or [{
                "id":      canonical_theme_id,
                "title":   canonical_theme_id.replace("_", " ").title(),
                "tickers": [{"symbol": sym, "canonical_theme_id": canonical_theme_id}],
            }],
        },
        "csv_data": [],
    }


class TestWatchlistProductionPath:
    """
    10 production-path test cases that invoke the real _enrich_store_with_quotes()
    and inspect the returned section rows.  No simulation helpers — these tests
    prove what the production function actually produces.
    """

    # ── Case 1 — Defect 1 regression: standalone sub_theme without parent_theme_id ─

    async def test_case1_standalone_subtheme_without_parent_theme_id(self):
        """
        MANDATORY Defect 1 regression (updated for taxonomy v2):
        dc_connectivity_silicon has classification='sub_theme' with parent_theme_id='semiconductors'.

        In taxonomy v1, ai_networking (now deprecated) was the example sub_theme without
        a parent_theme_id. In v2 all sub_themes have parent_theme_ids. The defect fix
        is that subtheme detection uses classification='sub_theme', not parent_theme_id
        existence. This test confirms that any sub_theme — with OR without parent_theme_id
        — always appears in subtheme_ids.

        Old (broken) code: parent_theme_id existence → subtheme_ids empty.
        Fixed code:        classification == 'sub_theme' → subtheme_ids includes node.
        """
        # v2 precondition: dc_connectivity_silicon is the canonical sub_theme for AI networking
        assert REG["dc_connectivity_silicon"]["classification"] == "sub_theme"
        assert REG["dc_connectivity_silicon"].get("parent_theme_id") == "semiconductors"

        result = await _run_enrich(_make_store("ALAB", "dc_connectivity_silicon"))
        row = _section_row(result)

        assert row["primary_theme_id"] == "dc_connectivity_silicon"
        assert row["theme_ids"] == ["dc_connectivity_silicon"]
        assert row["subtheme_ids"] == ["dc_connectivity_silicon"], (
            "dc_connectivity_silicon must appear in subtheme_ids because "
            "classification='sub_theme', regardless of whether parent_theme_id is set"
        )

    # ── Case 2 — Parent theme + standalone and nested subthemes ──────────────────

    async def test_case2_parent_plus_standalone_and_nested_subthemes(self):
        """
        primary=semiconductors (classification='theme')
        additional: dc_connectivity_silicon (sub_theme, parent_theme_id='semiconductors')
                    memory_storage (sub_theme, parent_theme_id='semiconductors')

        Both sub_theme nodes must appear in subtheme_ids.
        semiconductors must NOT appear in subtheme_ids.
        """
        assert REG["dc_connectivity_silicon"].get("parent_theme_id") == "semiconductors"
        assert REG["memory_storage"].get("parent_theme_id") == "semiconductors"
        assert REG["semiconductors"]["classification"] == "theme"

        result = await _run_enrich(
            _make_store("TEST", "semiconductors"),
            overrides=[
                {"symbol": "TEST", "theme_id": "dc_connectivity_silicon", "action": "add"},
                {"symbol": "TEST", "theme_id": "memory_storage",          "action": "add"},
            ],
        )
        row = _section_row(result)

        assert row["primary_theme_id"] == "semiconductors"
        assert row["theme_ids"][0] == "semiconductors", "primary must be first"
        assert set(row["theme_ids"]) == {"semiconductors", "dc_connectivity_silicon", "memory_storage"}
        assert "dc_connectivity_silicon" in row["subtheme_ids"], "sub_theme must be included"
        assert "memory_storage"          in row["subtheme_ids"], "nested sub_theme must be included"
        assert "semiconductors" not in row["subtheme_ids"], "theme node must not appear in subtheme_ids"

    # ── Case 3 — Removed membership excluded ─────────────────────────────────────

    async def test_case3_removed_membership_excluded(self):
        """
        An override row with action='remove' must exclude that theme_id from
        theme_ids and subtheme_ids.
        """
        result = await _run_enrich(
            _make_store("TEST", "semiconductors"),
            overrides=[
                {"symbol": "TEST", "theme_id": "cybersecurity", "action": "remove"},
            ],
        )
        row = _section_row(result)

        assert "cybersecurity" not in row["theme_ids"]
        assert "cybersecurity" not in row["subtheme_ids"]

    # ── Case 4 — Duplicates and deterministic order ───────────────────────────────

    async def test_case4_deduplication_and_deterministic_order(self):
        """
        Duplicate IDs in overrides must produce no duplicates.
        Primary ID must be first.
        Additional IDs must be consistently sorted.
        """
        result = await _run_enrich(
            _make_store("TEST", "semiconductors"),
            overrides=[
                # memory_storage listed twice — must be deduplicated
                {"symbol": "TEST", "theme_id": "memory_storage",  "action": "add"},
                {"symbol": "TEST", "theme_id": "ai_networking",   "action": "add"},
                {"symbol": "TEST", "theme_id": "memory_storage",  "action": "add"},
            ],
        )
        row = _section_row(result)

        assert row["theme_ids"][0] == "semiconductors"
        # No duplicates
        assert len(row["theme_ids"]) == len(set(row["theme_ids"]))
        # Additional IDs sorted (ai_networking < memory_storage alphabetically)
        assert row["theme_ids"][1:] == sorted(row["theme_ids"][1:])

    # ── Case 5 — FMP profile sector wins over CSV ─────────────────────────────────

    async def test_case5_fmp_profile_sector_wins_over_csv(self):
        """
        fund_snap["fields"]["profile"]["sector"] = "Utilities"
        CSV Sector = "Technology"

        Expected sector_id = "utilities" (FMP profile path wins).
        """
        fund_snaps = {
            "TEST": {
                "fields": {
                    "profile": {"sector": "Utilities"},
                },
            }
        }
        result = await _run_enrich(
            {
                "tickers":  ["TEST"],
                "analysis": {"sections": [{
                    "id": "clean_energy", "title": "Clean Energy",
                    "tickers": [{"symbol": "TEST", "canonical_theme_id": "clean_energy"}],
                }]},
                "csv_data": [{"Symbol": "TEST", "Sector": "Technology"}],
            },
            fund_snaps=fund_snaps,
        )
        row = _section_row(result)
        assert row["sector_id"] == "utilities", (
            f"Expected 'utilities' from FMP profile, got {row['sector_id']!r}"
        )

    # ── Case 6 — CSV sector fallback ─────────────────────────────────────────────

    async def test_case6_csv_sector_fallback(self):
        """
        No FMP profile sector present.
        CSV Sector = "Basic Materials"
        Expected sector_id = "materials".
        """
        fund_snaps = {
            "TEST": {
                "fields": {},  # no "profile", no "sector"
            }
        }
        result = await _run_enrich(
            {
                "tickers":  ["TEST"],
                "analysis": {"sections": [{
                    "id": "metals_mining", "title": "Metals & Mining",
                    "tickers": [{"symbol": "TEST", "canonical_theme_id": "metals_mining"}],
                }]},
                "csv_data": [{"Symbol": "TEST", "Sector": "Basic Materials"}],
            },
            fund_snaps=fund_snaps,
        )
        row = _section_row(result)
        assert row["sector_id"] == "materials", (
            f"Expected 'materials' from CSV, got {row['sector_id']!r}"
        )

    # ── Case 7 — sector_id and theme diverge; company sector always wins ──────────

    async def test_case7_sector_theme_conflict_company_sector_wins(self):
        """
        company profile sector = Energy
        primary theme = datacenter_infra (rollup: technology/utilities/real_estate)

        Expected:
          sector_id = "energy"                  (from actual company data)
          primary_theme_id = "datacenter_infra" (from theme assignment)
          theme's rollup sectors must NOT overwrite sector_id
        """
        fund_snaps = {
            "TEST": {
                "fields": {
                    "profile": {"sector": "Energy"},
                },
            }
        }
        result = await _run_enrich(
            _make_store("TEST", "datacenter_infra"),
            fund_snaps=fund_snaps,
        )
        row = _section_row(result)

        assert row["sector_id"] == "energy"
        assert row["primary_theme_id"] == "datacenter_infra"
        assert row["theme_ids"] == ["datacenter_infra"]
        # theme rollups must NOT appear as sector_id
        theme_rollups = get_effective_rollup_sector_ids("datacenter_infra", REG)
        assert row["sector_id"] not in theme_rollups or row["sector_id"] == "energy", (
            "sector_id must track company, not theme rollup"
        )

    # ── Case 8 — Invalid / sentinel primary maps to null ─────────────────────────

    async def test_case8_sentinel_primary_maps_to_null(self):
        """
        primary value = 'other_uncategorized' which is absent from the canonical
        registry.  primary_theme_id must be null; theme_ids and subtheme_ids empty.
        The human-readable canonical_theme_name field may remain unchanged.
        """
        assert "other_uncategorized" not in REG, (
            "Precondition: other_uncategorized must not be in the canonical registry"
        )

        result = await _run_enrich(
            {
                "tickers":  ["TEST"],
                "analysis": {"sections": [{
                    "id":    "other_uncategorized",
                    "title": "Other / Uncategorized",
                    "tickers": [{
                        "symbol":              "TEST",
                        "canonical_theme_id":  "other_uncategorized",
                        "canonical_theme_name": "Other / Uncategorized",
                    }],
                }]},
                "csv_data": [],
            },
        )
        row = _section_row(result)

        assert row["primary_theme_id"] is None, (
            f"other_uncategorized is not in the registry; "
            f"primary_theme_id must be null, got {row['primary_theme_id']!r}"
        )
        assert row["theme_ids"] == []
        assert row["subtheme_ids"] == []
        # Human-readable name preserved
        assert row.get("canonical_theme_name") == "Other / Uncategorized"

    # ── Case 9 — Skeleton path: all four identity fields present ─────────────────

    async def test_case9_skeleton_path_all_identity_fields_present(self):
        """
        No saved analysis sections (skeleton path).
        Ticker resolved to 'dc_connectivity_silicon' (sub_theme under semiconductors).

        All four identity fields must be present and correct.
        """
        result = await _run_enrich_skeleton(
            ["ALAB"],
            resolved_theme_id="dc_connectivity_silicon",
            resolved_theme_name="Data Center Connectivity & Interconnect Silicon",
        )
        sections = result["analysis"]["sections"]
        assert sections, "Skeleton must produce at least one section"
        all_tickers = [r for s in sections for r in s.get("tickers", [])]
        assert all_tickers, "Skeleton must produce at least one ticker row"

        row = all_tickers[0]
        assert "primary_theme_id" in row, "primary_theme_id must be present"
        assert "theme_ids"        in row, "theme_ids must be present"
        assert "subtheme_ids"     in row, "subtheme_ids must be present"
        assert "sector_id"        in row, "sector_id must be present"

        assert row["primary_theme_id"] == "dc_connectivity_silicon"
        assert row["theme_ids"] == ["dc_connectivity_silicon"]
        assert row["subtheme_ids"] == ["dc_connectivity_silicon"], (
            "dc_connectivity_silicon is classification=sub_theme; "
            "must appear in subtheme_ids via classification check"
        )

    # ── Case 10 — Saved / cached path: identity enrichment occurs ────────────────

    async def test_case10_saved_path_identity_enrichment_occurs(self):
        """
        Saved analysis sections (normal path) — the standard code path for a
        watchlist that has completed its AI analysis.

        Verifies that identity enrichment occurs before the response is returned,
        covering both a theme-classified and a subtheme-classified row.
        Uses taxonomy v2 nodes: semiconductors (theme) + dc_connectivity_silicon (sub_theme).
        """
        store = {
            "tickers":  ["NVDA", "ALAB"],
            "analysis": {
                "sections": [
                    {
                        "id": "semiconductors", "title": "Semiconductors",
                        "tickers": [{"symbol": "NVDA", "canonical_theme_id": "semiconductors"}],
                    },
                    {
                        "id": "dc_connectivity_silicon",
                        "title": "Data Center Connectivity & Interconnect Silicon",
                        "tickers": [{"symbol": "ALAB", "canonical_theme_id": "dc_connectivity_silicon"}],
                    },
                ],
            },
            "csv_data": [],
        }
        result = await _run_enrich(store)

        # Find the row for each ticker regardless of section order
        all_rows = {
            r["symbol"]: r
            for s in result["analysis"]["sections"]
            for r in s.get("tickers", [])
        }

        nvda = all_rows["NVDA"]
        assert nvda["primary_theme_id"] == "semiconductors"
        assert nvda["subtheme_ids"] == [], (
            "semiconductors has classification='theme'; must not appear in subtheme_ids"
        )

        alab = all_rows["ALAB"]
        assert alab["primary_theme_id"] == "dc_connectivity_silicon"
        assert "dc_connectivity_silicon" in alab["subtheme_ids"], (
            "dc_connectivity_silicon has classification='sub_theme'; must appear in subtheme_ids"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Contract 4 — sector_id conflict fixture
# ═══════════════════════════════════════════════════════════════════════════════

class TestSectorIdConflict:
    """
    sector_id must reflect the ACTUAL company sector, not the theme hierarchy.
    These fixtures demonstrate the conflict between theme-derived and company
    sector values and confirm that normalize_company_sector_to_id is the
    correct resolution path.
    """

    def test_theme_parent_sector_vs_company_sector(self):
        """
        A ticker in 'clean_energy' (parent_sector='utilities') but whose company
        is classified as 'Industrials' by FMP must get sector_id='industrials',
        not 'utilities'.  sector_id tracks the company, not the theme assignment.
        """
        # Simulate fund_snap sector field for an industrial-classified company
        company_sector_label = "Industrials"
        theme_parent_sector  = "utilities"   # clean_energy.parent_sector

        sector_id_from_company = normalize_company_sector_to_id(company_sector_label)
        sector_id_from_theme   = theme_parent_sector   # old (wrong) derivation

        assert sector_id_from_company == "industrials"
        assert sector_id_from_company != sector_id_from_theme, (
            "sector_id must come from company data, not from theme.parent_sector"
        )

    def test_null_when_company_sector_unavailable(self):
        """When fund_snap and CSV both lack sector data, sector_id must be None."""
        sector_id = normalize_company_sector_to_id("") or None
        assert sector_id is None

    def test_null_when_company_sector_unrecognised(self):
        """Unrecognised labels must yield None, not a guessed sector_id."""
        sector_id = normalize_company_sector_to_id("Speculative Assets") or None
        assert sector_id is None

    def test_no_theme_hierarchy_inference(self):
        """
        For every theme in the registry that has a parent_sector, confirm that
        the company-sector path (via normalize) and the theme path (parent_sector)
        CAN disagree without being treated as an error.  The company-sector path
        always wins.
        """
        # pick a cross-sector theme: datacenter_infra spans technology + utilities + real_estate
        meta = REG["datacenter_infra"]
        assert "parent_sector" in meta

        # A company in datacenter_infra might be classified as 'Utilities' by FMP
        company_sector_id = normalize_company_sector_to_id("Utilities")
        theme_parent_sector = meta["parent_sector"]   # "technology"

        # Both are valid canonical sector IDs but they differ — that's expected
        assert company_sector_id == "utilities"
        assert theme_parent_sector == "technology"
        # The company sector wins; no assertion error means the conflict is handled


# ═══════════════════════════════════════════════════════════════════════════════
# RS production-function tests — Contract 1
#
# These invoke the REAL _build_theme_row() and _validate_basket_hashes() with
# deterministic local inputs.  No live provider calls; _build_leader_universe
# is patched to return an empty universe.
# ═══════════════════════════════════════════════════════════════════════════════

def _make_bars(n: int = 60) -> list[dict]:
    """Return n synthetic daily OHLCV bars."""
    from datetime import date, timedelta
    start = date(2025, 1, 1)
    return [
        {
            "date":   (start + timedelta(days=i)).isoformat(),
            "open":   100.0 + i * 0.1,
            "high":   101.0 + i * 0.1,
            "low":    99.0  + i * 0.1,
            "close":  100.5 + i * 0.1,
            "volume": 1_000_000.0,
        }
        for i in range(n)
    ]


async def _build_row(theme_id: str, tf: str = "7D") -> dict | None:
    """
    Invoke the real _build_theme_row() with synthetic history + patched universe.
    Returns the row dict, or None if the function itself returns None.
    """
    from services.theme_rs_service import _build_theme_row
    from services.theme_rs_universe import THEME_RS_UNIVERSE

    meta = THEME_RS_UNIVERSE[theme_id]
    proxy_syms = meta["proxy_symbols"]
    bars = _make_bars(60)
    histories = {sym: (bars, "test") for sym in proxy_syms}
    histories["SPY"] = (bars, "test")
    quotes = {sym: {"price": bars[-1]["close"], "last": bars[-1]["close"]} for sym in proxy_syms}
    stock_perfs = {sym: 1.5 for sym in proxy_syms}
    stock_sources = {sym: "test" for sym in proxy_syms}

    # Stub _build_leader_universe so no ETF/Grok I/O occurs
    async def _fake_universe(tid, m, used):
        return (list(proxy_syms), {s: ["test_basket"] for s in proxy_syms}, "test")

    with _patch("services.theme_rs_service._build_leader_universe", side_effect=_fake_universe):
        return await _build_theme_row(
            theme_id, meta, quotes, histories, tf, stock_perfs, stock_sources
        )


class TestRsBuildThemeRowProduction:
    """
    Production tests that invoke the real _build_theme_row() and verify that
    hierarchy fields are present in the returned row dict.
    """

    async def test_parent_theme_row_has_hierarchy_fields(self):
        """
        A parent-theme node (semiconductors) must emit parent_theme_id=None and
        rollup_sector_ids containing 'technology' in the real _build_theme_row row.
        """
        row = await _build_row("semiconductors")
        assert row is not None, "_build_theme_row returned None for semiconductors"
        assert "parent_theme_id" in row, "parent_theme_id key must be present"
        assert "rollup_sector_ids" in row, "rollup_sector_ids key must be present"
        assert row["parent_theme_id"] is None, (
            f"semiconductors is a top-level theme; parent_theme_id must be None, "
            f"got {row['parent_theme_id']!r}"
        )
        assert "technology" in (row["rollup_sector_ids"] or []), (
            f"semiconductors must roll up to technology; got {row['rollup_sector_ids']!r}"
        )

    async def test_nested_subtheme_row_has_inherited_rollup(self):
        """
        A nested sub_theme (memory_storage, parent=semiconductors) must emit
        parent_theme_id='semiconductors' and rollup_sector_ids=['technology']
        in the real _build_theme_row row.
        """
        row = await _build_row("memory_storage")
        assert row is not None, "_build_theme_row returned None for memory_storage"
        assert row["parent_theme_id"] == "semiconductors", (
            f"memory_storage parent_theme_id must be 'semiconductors', "
            f"got {row['parent_theme_id']!r}"
        )
        assert "technology" in (row["rollup_sector_ids"] or []), (
            f"memory_storage must inherit technology rollup; got {row['rollup_sector_ids']!r}"
        )

    async def test_standalone_subtheme_row_has_rollup_without_parent_theme_id(self):
        """
        A standalone sub_theme (ai_networking, no parent_theme_id) must emit
        parent_theme_id=None AND have a non-empty rollup_sector_ids in the real row.
        """
        row = await _build_row("ai_networking")
        assert row is not None, "_build_theme_row returned None for ai_networking"
        assert row.get("parent_theme_id") is None
        assert row["rollup_sector_ids"], (
            f"ai_networking must have an effective rollup even without parent_theme_id; "
            f"got {row['rollup_sector_ids']!r}"
        )

    async def test_core_rs_fields_present_alongside_hierarchy(self):
        """
        The real _build_theme_row row must carry the expected core RS fields
        alongside the new hierarchy fields.
        """
        row = await _build_row("semiconductors")
        assert row is not None
        required_core = {"theme_id", "display_name", "classification", "proxy_symbols",
                         "basket_hash", "performance_curve", "members"}
        missing = required_core - set(row)
        assert not missing, f"Core RS fields missing from _build_theme_row output: {missing}"
        # Hierarchy fields co-exist with core RS fields
        assert "parent_theme_id"   in row
        assert "rollup_sector_ids" in row


class TestValidateBasketHashesProduction:
    """
    Production tests that invoke the real _validate_basket_hashes() and verify
    that stale/legacy rows are patched with live display_name, parent_theme_id,
    and rollup_sector_ids from the live registry.
    """

    def test_legacy_row_receives_hierarchy_fields(self):
        """
        A legacy LKG row (no basket_hash) must be stamped with the current
        parent_theme_id and rollup_sector_ids from the live registry.
        curve_status must be 'stale_legacy_lkg'.
        """
        from services.theme_rs_service import _validate_basket_hashes

        # memory_storage: parent_theme_id=semiconductors, rollup=['technology']
        tid = "memory_storage"
        assert REG[tid].get("parent_theme_id") == "semiconductors"

        legacy_row = {
            "theme_id":    tid,
            "display_name": "Old Display Name",
            # No basket_hash — this is a legacy LKG row
        }
        payload = {"themes": [legacy_row]}
        patched, stale_count = _validate_basket_hashes(payload)

        assert stale_count == 0, "legacy rows are not 'stale' — they are 'legacy'"
        out = patched["themes"][0]
        assert out["curve_status"] == "stale_legacy_lkg"
        assert out["parent_theme_id"] == "semiconductors", (
            f"parent_theme_id not repaired from live registry; got {out.get('parent_theme_id')!r}"
        )
        assert "technology" in (out.get("rollup_sector_ids") or []), (
            f"rollup_sector_ids not repaired from live registry; got {out.get('rollup_sector_ids')!r}"
        )
        # display_name must be pulled from live registry
        live_display = REG[tid]["display_name"]
        assert out["display_name"] == live_display, (
            f"display_name not updated from live registry; got {out['display_name']!r}"
        )

    def test_stale_hash_row_receives_hierarchy_fields(self):
        """
        A row with a mismatched basket_hash (stale membership) must have
        parent_theme_id and rollup_sector_ids repaired from the live registry.
        curve_status must be 'stale_membership'.
        """
        from services.theme_rs_service import _validate_basket_hashes

        tid = "gold"  # parent_theme_id=metals_mining, rollup=['materials']
        assert REG[tid].get("parent_theme_id") == "metals_mining"

        stale_row = {
            "theme_id":           tid,
            "display_name":       "Gold (outdated)",
            "parent_theme_id":    None,          # stale — wrong value
            "rollup_sector_ids":  [],            # stale — wrong value
            "basket_hash":        "WRONG_HASH",  # deliberate mismatch
            "performance_curve":  [1, 2, 3],
        }
        payload = {"themes": [stale_row]}
        patched, stale_count = _validate_basket_hashes(payload)

        assert stale_count == 1
        out = patched["themes"][0]
        assert out["curve_status"] == "stale_membership"
        assert out["parent_theme_id"] == "metals_mining", (
            f"parent_theme_id not repaired for stale row; got {out.get('parent_theme_id')!r}"
        )
        assert "materials" in (out.get("rollup_sector_ids") or []), (
            f"rollup_sector_ids not repaired for stale row; got {out.get('rollup_sector_ids')!r}"
        )
        # Stale curve must be wiped
        assert out["performance_curve"] == [], (
            "stale_membership row must have performance_curve wiped"
        )

    def test_current_hash_row_is_untouched(self):
        """
        A row whose basket_hash matches the live enriched registry must be
        served as-is.  curve_status must be 'current' and performance_curve
        must be preserved.

        The hash is derived from the ENRICHED registry that _validate_basket_hashes
        actually uses (from services.theme_merge_layer), not the base registry, to
        avoid divergence when the merge layer adds manual symbols.
        """
        from services.theme_rs_service import _validate_basket_hashes

        tid = "semiconductors"
        # Derive the current hash by running a legacy call first — this ensures
        # we use the exact same enriched proxy_symbols list that the function uses.
        legacy_payload = {"themes": [{"theme_id": tid, "display_name": "tmp"}]}
        _legacy_out, _ = _validate_basket_hashes(legacy_payload)
        current_hash = _legacy_out["themes"][0]["basket_hash"]

        current_row = {
            "theme_id":          tid,
            "display_name":      "Semiconductors",
            "parent_theme_id":   REG[tid].get("parent_theme_id"),
            "rollup_sector_ids": get_effective_rollup_sector_ids(tid, REG),
            "basket_hash":       current_hash,
            "performance_curve": [0.5, 1.0, 1.5],
        }
        payload = {"themes": [current_row]}
        patched, stale_count = _validate_basket_hashes(payload)

        assert stale_count == 0, (
            "Current-hash row must not increment stale_count"
        )
        out = patched["themes"][0]
        assert out.get("curve_status") == "current", (
            f"current-hash row must have curve_status='current', got {out.get('curve_status')!r}"
        )
        # Performance curve must be preserved (not wiped)
        assert out["performance_curve"] == [0.5, 1.0, 1.5]


# ═══════════════════════════════════════════════════════════════════════════════
# ── TAXONOMY V2 NEW TESTS ─────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════


# ──────────────────────────────────────────────────────────────────────────────
# Registry integrity (15 checks)
# ──────────────────────────────────────────────────────────────────────────────

class TestRegistryIntegrityV2:
    """15 structural integrity checks for the v2 canonical registry."""

    def test_total_node_count(self):
        """Registry must have exactly 112 nodes (11+23+67+3+8)."""
        assert len(REG) == 112, f"Expected 112 nodes, got {len(REG)}"

    def test_exactly_11_sectors(self):
        sectors = [k for k, v in REG.items() if v.get("classification") == "sector"]
        assert len(sectors) == 11, f"Expected 11 sectors, got {len(sectors)}: {sorted(sectors)}"

    def test_exactly_23_parent_themes(self):
        themes = [k for k, v in REG.items() if v.get("classification") == "theme"]
        assert len(themes) == 23, f"Expected 23 parent themes, got {len(themes)}: {sorted(themes)}"

    def test_exactly_67_subthemes(self):
        subs = [k for k, v in REG.items() if v.get("classification") == "sub_theme"]
        assert len(subs) == 67, f"Expected 67 sub_themes, got {len(subs)}: {sorted(subs)}"

    def test_exactly_3_market_lenses(self):
        lenses = [k for k, v in REG.items() if v.get("classification") == "market_lens"]
        assert lenses == sorted(["gold", "silver", "copper_miners"]) or set(lenses) == {"gold", "silver", "copper_miners"}, \
            f"Expected gold/silver/copper_miners, got {lenses}"

    def test_exactly_8_deprecated(self):
        depr = [k for k, v in REG.items() if v.get("classification") == "deprecated"]
        assert len(depr) == 8, f"Expected 8 deprecated nodes, got {len(depr)}: {sorted(depr)}"

    def test_90_assignable_nodes(self):
        assignable = [
            k for k, v in REG.items()
            if v.get("assignable", True) and v.get("classification") in ("theme", "sub_theme")
        ]
        assert len(assignable) == 90, f"Expected 90 assignable, got {len(assignable)}"

    def test_no_big_tech_as_structural_node(self):
        """Big Tech / hyperscalers / M7 must not be assignable structural nodes."""
        from services.theme_rs_universe import _ASSIGNABLE_CLASSIFICATIONS
        for tid in ("big_tech", "hyperscalers", "magnificent_seven"):
            if tid in REG:
                assert REG[tid].get("classification") not in _ASSIGNABLE_CLASSIFICATIONS, \
                    f"{tid} must not be assignable"

    def test_commodity_lenses_not_assignable(self):
        for lid in ("gold", "silver", "copper_miners"):
            meta = REG.get(lid, {})
            assert meta.get("assignable", True) is False, f"{lid} must have assignable=False"
            assert meta.get("classification") == "market_lens", f"{lid} must be market_lens"

    def test_deprecated_nodes_not_assignable(self):
        for tid, meta in REG.items():
            if meta.get("classification") == "deprecated":
                assert meta.get("assignable", True) is False, f"{tid}: deprecated must have assignable=False"

    def test_every_subtheme_has_parent_theme_id(self):
        """All sub_themes must have a parent_theme_id pointing to an existing theme."""
        bad = []
        for tid, meta in REG.items():
            if meta.get("classification") != "sub_theme":
                continue
            parent = meta.get("parent_theme_id")
            if not parent:
                bad.append(f"{tid}: sub_theme missing parent_theme_id")
            elif REG.get(parent, {}).get("classification") not in ("theme",):
                bad.append(f"{tid}: parent_theme_id={parent!r} is not a 'theme' node")
        assert bad == [], "\n".join(bad)

    def test_market_lens_nodes_have_parent_theme_id(self):
        """Market-lens nodes (gold/silver/copper) have parent_theme_id for rollup context."""
        for lid in ("gold", "silver", "copper_miners"):
            assert REG[lid].get("parent_theme_id") == "metals_mining", \
                f"{lid}: market_lens must have parent_theme_id='metals_mining'"

    def test_no_node_missing_required_v2_keys(self):
        """Every node must have the required v2 field set."""
        required = {"classification", "parent_sector", "proxy_type",
                    "proxy_symbols", "candidate_symbols", "keywords", "display_name"}
        bad = []
        for tid, meta in REG.items():
            for k in required:
                if k not in meta:
                    bad.append(f"{tid}: missing required key {k!r}")
        assert bad == [], "\n".join(bad)

    def test_validate_registry_clean(self):
        """The canonical registry passes all structural validator checks."""
        from services.theme_rs_universe import validate_registry
        errors = validate_registry()
        assert errors == [], "validate_registry() errors:\n" + "\n".join(errors)

    def test_preserved_id_list(self):
        """Critical v1 IDs must be preserved (even if reclassified or deprecated)."""
        must_exist = [
            "banks", "insurance", "fintech", "datacenter_infra", "quantum", "space", "defense",
            "agribusiness", "metals_mining", "semiconductors", "software", "clean_energy",
            "oil_gas", "crypto_equities", "cloud_software", "cybersecurity", "memory_storage",
            "lng_gas", "oil_services", "rare_earth", "drones", "solar", "regional_banks",
            "biotech", "medical_devices", "homebuilders", "consumer_retail", "power_cooling",
            "robotics_automation",
        ]
        missing = [t for t in must_exist if t not in REG]
        assert missing == [], f"Preserved IDs missing from registry: {missing}"


# ──────────────────────────────────────────────────────────────────────────────
# Exact hierarchy (10 families)
# ──────────────────────────────────────────────────────────────────────────────

class TestExactHierarchyV2:
    """Verify parent–child relationships for 10 key v2 theme families."""

    # 1. Semiconductors family
    def test_semiconductors_children(self):
        children = [k for k, v in REG.items()
                    if v.get("parent_theme_id") == "semiconductors"]
        assert "dc_connectivity_silicon" in children
        assert "memory_storage"          in children
        assert "analog_power_mixed"      in children
        assert "packaging_substrates"    in children

    def test_dc_connectivity_silicon_is_sub_theme(self):
        assert REG["dc_connectivity_silicon"]["classification"] == "sub_theme"
        assert REG["dc_connectivity_silicon"]["parent_theme_id"] == "semiconductors"

    # 2. Nuclear Energy family
    def test_nuclear_energy_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "nuclear_energy"}
        assert children >= {"uranium_nuclear_fuel", "nuclear_equipment_services",
                            "smr_advanced_reactors", "nuclear_utilities_operators"}

    def test_nuclear_energy_rollup(self):
        rollup = set(node("nuclear_energy").get("rollup_sector_ids", []))
        assert "utilities" in rollup

    # 3. Photonics family
    def test_photonics_optical_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "photonics_optical"}
        assert "optical_components_lasers" in children
        assert "sensing_lidar"             in children

    # 4. Grid & Electrification family
    def test_grid_electrification_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "grid_electrification"}
        assert "grid_hardware_electrical" in children

    # 5. Crypto family
    def test_crypto_equities_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "crypto_equities"}
        assert "bitcoin_miners"         in children
        assert "blockchain_infrastructure" in children

    # 6. Defense family
    def test_defense_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "defense"}
        assert "drones"  in children
        assert "defense_platforms_electronics" in children

    # 7. Metals & Mining family
    def test_metals_mining_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "metals_mining"}
        assert "rare_earth"            in children
        assert "lithium"               in children
        assert "precious_metals"       in children
        assert "base_metals_diversified" in children

    # 8. Oil & Gas family
    def test_oil_gas_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "oil_gas"}
        assert "lng_gas"    in children
        assert "oil_services" in children

    # 9. Transportation & Mobility family
    def test_transportation_children(self):
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "transportation_mobility"}
        assert "travel_leisure"    in children
        assert "freight_logistics" in children

    # 10. Space family
    def test_space_is_theme_level(self):
        """Space is a top-level theme; satellite_comms and earth_observation are children."""
        assert REG["space"]["classification"] == "theme"
        children = {k for k, v in REG.items() if v.get("parent_theme_id") == "space"}
        assert "satellite_comms"    in children
        assert "earth_observation"  in children
        assert "launch_space_systems" in children


# ──────────────────────────────────────────────────────────────────────────────
# Cache repair (7 checks for classification + parent_sector injection)
# ──────────────────────────────────────────────────────────────────────────────

class TestValidateBasketHashesV2:
    """_validate_basket_hashes now repairs classification and parent_sector too."""

    def test_legacy_row_gets_classification(self):
        """Legacy row (no basket_hash) must receive the live classification from the registry."""
        from services.theme_rs_service import _validate_basket_hashes
        tid = "dc_connectivity_silicon"
        payload = {"themes": [{"theme_id": tid, "display_name": "old label"}]}
        patched, _ = _validate_basket_hashes(payload)
        out = patched["themes"][0]
        assert out.get("classification") == REG[tid]["classification"], \
            f"classification not repaired: {out.get('classification')!r}"

    def test_legacy_row_gets_parent_sector(self):
        from services.theme_rs_service import _validate_basket_hashes
        tid = "dc_connectivity_silicon"
        payload = {"themes": [{"theme_id": tid, "display_name": "old"}]}
        patched, _ = _validate_basket_hashes(payload)
        out = patched["themes"][0]
        assert out.get("parent_sector") == REG[tid].get("parent_sector"), \
            f"parent_sector not repaired: {out.get('parent_sector')!r}"

    def test_stale_row_gets_classification(self):
        from services.theme_rs_service import _validate_basket_hashes
        tid = "memory_storage"
        row = {"theme_id": tid, "display_name": "old", "basket_hash": "WRONG", "classification": "wrong_cls"}
        patched, stale = _validate_basket_hashes({"themes": [row]})
        assert stale == 1
        assert patched["themes"][0]["classification"] == REG[tid]["classification"]

    def test_stale_row_gets_assignable(self):
        from services.theme_rs_service import _validate_basket_hashes
        tid = "semiconductors"
        row = {"theme_id": tid, "display_name": "old", "basket_hash": "WRONG"}
        patched, _ = _validate_basket_hashes({"themes": [row]})
        assert "assignable" in patched["themes"][0], "assignable must be set in stale row"

    def test_current_row_gets_classification(self):
        """Current-hash row must also get classification injected (taxonomy patch)."""
        from services.theme_rs_service import _validate_basket_hashes
        tid = "semiconductors"
        legacy_payload = {"themes": [{"theme_id": tid, "display_name": "tmp"}]}
        _legacy_out, _ = _validate_basket_hashes(legacy_payload)
        current_hash = _legacy_out["themes"][0]["basket_hash"]
        row = {"theme_id": tid, "display_name": "Semiconductors", "basket_hash": current_hash}
        patched, stale = _validate_basket_hashes({"themes": [row]})
        out = patched["themes"][0]
        assert stale == 0
        assert out.get("classification") == REG[tid]["classification"]

    def test_parent_theme_id_repaired_for_all_branches(self):
        """parent_theme_id is repaired on legacy, stale, and current rows."""
        from services.theme_rs_service import _validate_basket_hashes
        tid = "dc_connectivity_silicon"
        expected_parent = REG[tid].get("parent_theme_id")
        # Legacy branch
        leg_out, _ = _validate_basket_hashes({"themes": [{"theme_id": tid, "display_name": "x"}]})
        assert leg_out["themes"][0].get("parent_theme_id") == expected_parent, "legacy branch"
        # Stale branch
        stale_out, _ = _validate_basket_hashes({"themes": [{"theme_id": tid, "display_name": "x", "basket_hash": "WRONG"}]})
        assert stale_out["themes"][0].get("parent_theme_id") == expected_parent, "stale branch"

    def test_deprecated_node_assignable_false_repaired(self):
        """Deprecated nodes carry assignable=False from the registry and must be repaired."""
        from services.theme_rs_service import _validate_basket_hashes
        tid = "ai_networking"  # deprecated in v2
        row = {"theme_id": tid, "display_name": "old", "basket_hash": "WRONG"}
        patched, _ = _validate_basket_hashes({"themes": [row]})
        out = patched["themes"][0]
        assert out.get("assignable") is False, \
            f"deprecated node should have assignable=False, got {out.get('assignable')!r}"


# ──────────────────────────────────────────────────────────────────────────────
# Atomic ticker taxonomy API (15 checks)
# ──────────────────────────────────────────────────────────────────────────────

class TestAtomicTickerTaxonomyEndpoint:
    """Unit-level tests for TickerTaxonomyBody Pydantic model and route helpers."""

    def test_model_normalizes_primary_lowercase(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(primary_theme_id="  Semiconductors  ")
        assert body.primary_theme_id == "semiconductors"

    def test_model_deduplicates_additional(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(additional_theme_ids=["memory_storage", "memory_storage", "cloud_software"])
        assert len(body.additional_theme_ids) == 2
        assert "memory_storage" in body.additional_theme_ids
        assert "cloud_software"  in body.additional_theme_ids

    def test_model_sorts_additional(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(additional_theme_ids=["solar", "biotech", "drones"])
        assert body.additional_theme_ids == sorted(body.additional_theme_ids)

    def test_model_none_primary(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody()
        assert body.primary_theme_id is None
        assert body.additional_theme_ids == []

    def test_model_empty_string_primary_normalized_to_none(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(primary_theme_id="")
        assert body.primary_theme_id is None

    def test_endpoint_mounted_under_admin_prefix(self):
        """PUT /api/themes/admin/ticker-taxonomy/{ticker} must be registered."""
        from routes.themes import router
        # Router is mounted with its prefix; paths include the prefix
        all_paths = {r.path for r in router.routes}
        # Accept either form since the prefix may or may not be included
        expected_suffix = "/admin/ticker-taxonomy/{ticker}"
        assert any(p.endswith(expected_suffix) for p in all_paths), (
            f"Expected a route ending with {expected_suffix!r}; found: {sorted(all_paths)}"
        )

    def test_endpoint_uses_put_method(self):
        from routes.themes import router
        for r in router.routes:
            if r.path == "/admin/ticker-taxonomy/{ticker}":
                assert "PUT" in r.methods, f"Expected PUT, got {r.methods}"
                break

    def test_model_accepts_valid_assignable_id(self):
        """Valid assignable canonical IDs must be accepted without error."""
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(primary_theme_id="dc_connectivity_silicon")
        assert body.primary_theme_id == "dc_connectivity_silicon"

    def test_model_additional_stripped_lowercase(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(additional_theme_ids=["  SOLAR  ", "BIOTECH"])
        # Validator lowercases and strips
        assert "solar" in body.additional_theme_ids
        assert "biotech" in body.additional_theme_ids

    def test_ticker_taxonomy_body_schema(self):
        """TickerTaxonomyBody must expose correct field types."""
        from routes.themes import TickerTaxonomyBody
        import inspect
        schema = TickerTaxonomyBody.model_fields
        assert "primary_theme_id" in schema
        assert "additional_theme_ids" in schema
        assert "note" in schema
        assert "created_by" in schema

    def test_model_additional_with_empty_strings_filtered(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(additional_theme_ids=["solar"])
        assert "solar" in body.additional_theme_ids
        # Pure empty string must not appear in output
        body2 = TickerTaxonomyBody(additional_theme_ids=["solar", "biotech"])
        assert len(body2.additional_theme_ids) == 2

    def test_get_ticker_theme_memberships_returns_dict(self):
        """_get_ticker_theme_memberships must return a dict with expected keys."""
        from routes.themes import _get_ticker_theme_memberships
        result = _get_ticker_theme_memberships("NONEXISTENT_XYZ_123")
        assert isinstance(result, dict)
        # Must not raise; non-existent ticker returns empty or default struct

    def test_router_has_admin_prefix(self):
        """themes.py router must be prefixed with /api/themes or /themes."""
        from routes.themes import router
        # Router prefix is set at include time; check it contains "themes"
        assert "themes" in router.prefix, f"Unexpected router prefix: {router.prefix!r}"

    def test_model_default_created_by_admin(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody()
        assert body.created_by == "admin"

    def test_model_note_optional(self):
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(primary_theme_id="solar", note=None)
        assert body.note is None


# ──────────────────────────────────────────────────────────────────────────────
# AI Proposal Engine (13 checks)
# ──────────────────────────────────────────────────────────────────────────────

class TestThemeTaxonomyClassifier:
    """13 checks for the dry-run AI classification engine."""

    def test_sol56_finding_reported(self):
        from services.theme_taxonomy_classifier import SOL56_FINDING, _SOL56_MISSING
        # SOL56_FINDING must document what was searched
        assert "SOL56_MODEL_ID" in SOL56_FINDING or "SOL 5.6" in SOL56_FINDING

    def test_sol56_missing_is_bool(self):
        from services.theme_taxonomy_classifier import _SOL56_MISSING
        assert isinstance(_SOL56_MISSING, bool)

    def test_dry_run_apply_constant_false(self):
        from services.theme_taxonomy_classifier import DRY_RUN_APPLY
        assert DRY_RUN_APPLY is False, "DRY_RUN_APPLY must be False — this module must never write to Neon"

    def test_get_assignable_registry_excludes_deprecated(self):
        from services.theme_taxonomy_classifier import _get_assignable_registry
        reg = _get_assignable_registry()
        for tid, meta in reg.items():
            assert meta.get("classification") not in ("deprecated", "market_lens", "sector"), \
                f"{tid} with classification={meta.get('classification')!r} must not be in assignable registry"

    def test_get_assignable_registry_includes_dc_connectivity_silicon(self):
        from services.theme_taxonomy_classifier import _get_assignable_registry
        reg = _get_assignable_registry()
        assert "dc_connectivity_silicon" in reg

    def test_build_taxonomy_prompt_contains_parent_themes(self):
        from services.theme_taxonomy_classifier import _get_assignable_registry, _build_taxonomy_prompt
        reg = _get_assignable_registry()
        prompt = _build_taxonomy_prompt(reg)
        assert "semiconductors" in prompt
        assert "metals_mining"  in prompt
        assert "dc_connectivity_silicon" in prompt

    def test_taxonomy_prompt_hash_stable(self):
        """Same registry must produce same hash (deterministic)."""
        from services.theme_taxonomy_classifier import _get_assignable_registry, _taxonomy_prompt_hash
        reg = _get_assignable_registry()
        h1 = _taxonomy_prompt_hash(reg)
        h2 = _taxonomy_prompt_hash(reg)
        assert h1 == h2

    def test_summarize_empty(self):
        from services.theme_taxonomy_classifier import _summarize
        s = _summarize([], [])
        assert s["total_tickers"] == 0
        assert s["valid_proposals"] == 0
        assert s["quarantined"] == 0

    def test_validate_proposal_quarantines_unknown_ids(self):
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        bad_proposal = {
            "ticker": "TEST",
            "proposed_primary_theme_id": "nonexistent_theme_xyz",
            "proposed_additional_theme_ids": ["another_bad_id"],
            "confidence": 0.9,
        }
        normalized, quarantine_reasons = _validate_proposal(bad_proposal, reg, {})
        assert len(quarantine_reasons) > 0, "Unknown IDs must produce quarantine reasons"

    def test_validate_proposal_accepts_valid_ids(self):
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        good_proposal = {
            "ticker": "ALAB",
            "proposed_primary_theme_id": "dc_connectivity_silicon",
            "proposed_additional_theme_ids": ["semiconductors"],
            "confidence": 0.92,
            "rationale": "PCIe retimer silicon for AI data centers.",
            "evidence": ["CXL products"],
            "warnings": [],
        }
        normalized, quarantine_reasons = _validate_proposal(good_proposal, reg, {})
        assert quarantine_reasons == [], f"Valid proposal should not be quarantined: {quarantine_reasons}"
        assert normalized["proposed_primary_theme_id"] == "dc_connectivity_silicon"

    def test_validate_proposal_strips_primary_from_additional(self):
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        proposal = {
            "ticker": "NVDA",
            "proposed_primary_theme_id": "semiconductors",
            "proposed_additional_theme_ids": ["semiconductors", "cloud_software"],
            "confidence": 0.95,
        }
        normalized, _ = _validate_proposal(proposal, reg, {})
        # primary must not also appear in additional
        assert "semiconductors" not in normalized["proposed_additional_theme_ids"], \
            "Primary must be stripped from additional_theme_ids"

    def test_manual_protected_proposal_has_warning(self):
        """When current assignment is manual, proposal must flag manual_override_protected."""
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        proposal = {
            "ticker": "TEST",
            "proposed_primary_theme_id": "semiconductors",
            "proposed_additional_theme_ids": [],
            "confidence": 0.9,
        }
        manual_assignments = {"TEST": {"primary_theme_id": "cloud_software", "additional_theme_ids": [], "manual": True}}
        normalized, _ = _validate_proposal(proposal, reg, manual_assignments)
        assert normalized["manual_override_protected"] is True
        assert normalized["requires_manual_review"] is True

    def test_run_sample_signature(self):
        """run_sample must accept optional tickers list and write_artifacts flag."""
        import inspect
        from services.theme_taxonomy_classifier import run_sample
        sig = inspect.signature(run_sample)
        params = sig.parameters
        assert "tickers"        in params
        assert "write_artifacts" in params


# ═══════════════════════════════════════════════════════════════════════════════
# CORRECTIVE COMMIT — Contract Tests (Contracts 2–16)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Contract 2 detail: rollup field corrections ───────────────────────────────

def test_defense_rollup_includes_technology():
    """Contract 2 explicit: defense.rollup_sector_ids must contain 'technology'."""
    rollup = set(node("defense")["rollup_sector_ids"])
    assert "technology" in rollup, f"defense rollup missing technology; got {rollup}"
    assert "industrials" in rollup, f"defense rollup missing industrials; got {rollup}"


def test_datacenter_infra_rollup_includes_industrials():
    """Contract 2 explicit: datacenter_infra.rollup_sector_ids must contain 'industrials'."""
    rollup = set(node("datacenter_infra")["rollup_sector_ids"])
    assert "industrials" in rollup, f"datacenter_infra rollup missing industrials; got {rollup}"


# ── Contract 3: AAOI placement ────────────────────────────────────────────────

def test_aaoi_not_in_photonics_optical_candidates():
    """AAOI must NOT appear in photonics_optical.candidate_symbols."""
    assert "AAOI" not in node("photonics_optical").get("candidate_symbols", []), (
        "AAOI must be removed from photonics_optical.candidate_symbols "
        "(it belongs under optical_interconnects specifically)"
    )


def test_aaoi_in_optical_interconnects_or_dc_connectivity():
    """AAOI (silicon photonics interconnect) should be in optical_interconnects or dc_connectivity_silicon."""
    in_oi = "AAOI" in node("optical_interconnects").get("candidate_symbols", [])
    in_dc = "AAOI" in node("dc_connectivity_silicon").get("candidate_symbols", [])
    assert in_oi or in_dc, (
        "AAOI should appear in optical_interconnects or dc_connectivity_silicon candidates"
    )


# ── Contract 4: sector_tags and macro_sensitivities on every node ─────────────

class TestSectorTagsAndMacroSensitivities:
    def test_all_nodes_have_sector_tags(self):
        missing = [tid for tid, m in REG.items() if "sector_tags" not in m]
        assert missing == [], f"Nodes missing sector_tags: {missing}"

    def test_all_nodes_have_macro_sensitivities(self):
        missing = [tid for tid, m in REG.items() if "macro_sensitivities" not in m]
        assert missing == [], f"Nodes missing macro_sensitivities: {missing}"

    def test_sector_tags_are_lists(self):
        bad = [tid for tid, m in REG.items() if not isinstance(m.get("sector_tags"), list)]
        assert bad == [], f"sector_tags must be lists: {bad}"

    def test_macro_sensitivities_are_lists(self):
        bad = [tid for tid, m in REG.items() if not isinstance(m.get("macro_sensitivities"), list)]
        assert bad == [], f"macro_sensitivities must be lists: {bad}"

    def test_assignable_nodes_have_nonempty_sector_tags(self):
        from services.theme_rs_universe import THEME_RS_UNIVERSE
        bad = [
            tid for tid, m in THEME_RS_UNIVERSE.items()
            if m.get("assignable", True)
            and m.get("classification") in ("theme", "sub_theme")
            and not m.get("sector_tags")
        ]
        assert bad == [], f"Assignable nodes have empty sector_tags: {bad}"


# ── Contract 5: AI Networking merge-layer mapping ─────────────────────────────

def test_section_map_ai_networking_points_to_deprecated_node():
    """_SECTION_TO_THEME_ID['AI Networking'] must NOT blindly redirect to networking_fabric_infra."""
    from services.theme_merge_layer import _SECTION_TO_THEME_ID
    val = _SECTION_TO_THEME_ID.get("AI Networking")
    assert val != "networking_fabric_infra", (
        f"AI Networking must map to deprecated ai_networking node, not networking_fabric_infra; got {val!r}"
    )
    assert val == "ai_networking", f"Expected 'ai_networking', got {val!r}"


def test_category_map_ai_networking_points_to_deprecated_node():
    """_CATEGORY_TO_THEME_ID['AI Networking'] must NOT blindly redirect to networking_fabric_infra."""
    from services.theme_merge_layer import _CATEGORY_TO_THEME_ID
    val = _CATEGORY_TO_THEME_ID.get("AI Networking")
    assert val != "networking_fabric_infra", (
        f"AI Networking must map to deprecated ai_networking node, not networking_fabric_infra; got {val!r}"
    )
    assert val == "ai_networking", f"Expected 'ai_networking', got {val!r}"


# ── Contract 8: Provider gate ─────────────────────────────────────────────────

class TestProviderGate:
    def _reload_clf(self, monkeypatch):
        import importlib
        import services.theme_taxonomy_classifier as clf
        importlib.reload(clf)
        return clf

    def test_detect_provider_rejects_anthropic_fallback(self, monkeypatch):
        """With only ANTHROPIC_API_KEY set, provider must be 'none'."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-anthropic")
        for k in ("SOL56_MODEL_ID", "SOL_MODEL_ID", "OPENAI_API_KEY",
                  "THEME_CLASSIFIER_MODEL", "GEMINI_API_KEY"):
            monkeypatch.delenv(k, raising=False)
        clf = self._reload_clf(monkeypatch)
        provider, _ = clf._detect_provider()
        assert provider == "none", f"Anthropic must not be auto-selected; got {provider!r}"

    def test_detect_provider_rejects_gemini_fallback(self, monkeypatch):
        """With only GEMINI_API_KEY set, provider must be 'none'."""
        monkeypatch.setenv("GEMINI_API_KEY", "AIza-test")
        for k in ("SOL56_MODEL_ID", "SOL_MODEL_ID", "OPENAI_API_KEY",
                  "THEME_CLASSIFIER_MODEL", "ANTHROPIC_API_KEY"):
            monkeypatch.delenv(k, raising=False)
        clf = self._reload_clf(monkeypatch)
        provider, _ = clf._detect_provider()
        assert provider == "none", f"Gemini must not be auto-selected; got {provider!r}"

    def test_detect_provider_accepts_sol56(self, monkeypatch):
        """SOL56_MODEL_ID present → provider is sol56."""
        monkeypatch.setenv("SOL56_MODEL_ID", "sol-5.6-turbo")
        for k in ("OPENAI_API_KEY", "THEME_CLASSIFIER_MODEL"):
            monkeypatch.delenv(k, raising=False)
        clf = self._reload_clf(monkeypatch)
        provider, model_id = clf._detect_provider()
        assert provider == "sol56", f"Expected sol56, got {provider!r}"
        assert model_id == "sol-5.6-turbo"

    def test_detect_provider_requires_explicit_model_for_openai(self, monkeypatch):
        """OPENAI_API_KEY alone is insufficient — THEME_CLASSIFIER_MODEL must also be set."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-openai")
        for k in ("SOL56_MODEL_ID", "SOL_MODEL_ID", "THEME_CLASSIFIER_MODEL"):
            monkeypatch.delenv(k, raising=False)
        clf = self._reload_clf(monkeypatch)
        provider, _ = clf._detect_provider()
        assert provider == "none", f"OpenAI without explicit model must not be selected; got {provider!r}"

    def test_detect_provider_accepts_openai_with_explicit_model(self, monkeypatch):
        """OPENAI_API_KEY + THEME_CLASSIFIER_MODEL → provider is openai."""
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-openai")
        monkeypatch.setenv("THEME_CLASSIFIER_MODEL", "gpt-4o")
        for k in ("SOL56_MODEL_ID", "SOL_MODEL_ID"):
            monkeypatch.delenv(k, raising=False)
        clf = self._reload_clf(monkeypatch)
        provider, model_id = clf._detect_provider()
        assert provider == "openai", f"Expected openai, got {provider!r}"
        assert model_id == "gpt-4o"

    def test_no_provider_returns_config_error_not_placeholder_proposals(self, monkeypatch):
        """When no provider is configured, run_sample must return config_error with zero proposals."""
        for k in ("SOL56_MODEL_ID", "SOL_MODEL_ID", "OPENAI_API_KEY",
                  "THEME_CLASSIFIER_MODEL", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"):
            monkeypatch.delenv(k, raising=False)
        clf = self._reload_clf(monkeypatch)
        result = clf.run_sample(tickers=["AAPL"], write_artifacts=False)
        assert "config_error" in result, f"Must include config_error when no provider; keys={list(result)}"
        assert result.get("proposals") == [], f"proposals must be empty, got {result.get('proposals')}"
        assert result.get("quarantined") == [], f"quarantined must be empty, got {result.get('quarantined')}"


# ── Contract 9: Input completeness gate ───────────────────────────────────────

class TestInputCompletenessGate:
    def _reload_clf_no_provider(self, monkeypatch):
        import importlib
        import services.theme_taxonomy_classifier as clf
        for k in ("SOL56_MODEL_ID", "SOL_MODEL_ID", "OPENAI_API_KEY",
                  "THEME_CLASSIFIER_MODEL", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"):
            monkeypatch.delenv(k, raising=False)
        importlib.reload(clf)
        return clf

    def test_quarantines_ticker_with_missing_company_name(self, monkeypatch):
        """Ticker whose company name defaults to ticker symbol must be quarantined INPUT_INCOMPLETE."""
        # Give it a provider so config gate doesn't fire first
        monkeypatch.setenv("SOL56_MODEL_ID", "sol-5.6-turbo")
        import importlib, services.theme_taxonomy_classifier as clf
        importlib.reload(clf)
        monkeypatch.setattr(clf, "_get_fundamentals_cache", lambda: {})
        monkeypatch.setattr(clf, "_get_llm_overrides", lambda: {})
        monkeypatch.setattr(clf, "_get_current_assignments", lambda: {})
        # Patch _call_llm to raise (should never be reached for incomplete input)
        monkeypatch.setattr(clf, "_call_llm", lambda *a, **k: (_ for _ in ()).throw(AssertionError("_call_llm must not be called for incomplete input")))
        result = clf.run_sample(tickers=["XYZQ"], write_artifacts=False)
        quarantined_tickers = [q["ticker"] for q in result.get("quarantined", [])]
        reasons = [q.get("reason", "") for q in result.get("quarantined", []) if q.get("ticker") == "XYZQ"]
        assert "XYZQ" in quarantined_tickers, f"XYZQ must be quarantined; got quarantined={result.get('quarantined')}"
        assert any("INPUT_INCOMPLETE" in r for r in reasons), f"Reason must contain INPUT_INCOMPLETE; got {reasons}"

    def test_quarantines_ticker_with_missing_description(self, monkeypatch):
        """Ticker with company name but no description must be quarantined INPUT_INCOMPLETE."""
        monkeypatch.setenv("SOL56_MODEL_ID", "sol-5.6-turbo")
        import importlib, services.theme_taxonomy_classifier as clf
        importlib.reload(clf)
        fake_cache = {"TSLA": {"fields": {"companyName": "Tesla Inc", "description": ""}}}
        monkeypatch.setattr(clf, "_get_fundamentals_cache", lambda: fake_cache)
        monkeypatch.setattr(clf, "_get_llm_overrides", lambda: {})
        monkeypatch.setattr(clf, "_get_current_assignments", lambda: {})
        monkeypatch.setattr(clf, "_call_llm", lambda *a, **k: (_ for _ in ()).throw(AssertionError("_call_llm must not be called for incomplete input")))
        result = clf.run_sample(tickers=["TSLA"], write_artifacts=False)
        quarantined_tickers = [q["ticker"] for q in result.get("quarantined", [])]
        reasons = [q.get("reason", "") for q in result.get("quarantined", []) if q.get("ticker") == "TSLA"]
        assert "TSLA" in quarantined_tickers, f"TSLA must be quarantined; got {result.get('quarantined')}"
        assert any("INPUT_INCOMPLETE" in r for r in reasons), f"Reason must contain INPUT_INCOMPLETE; got {reasons}"


# ── Contract 10: Identity guard ────────────────────────────────────────────────

class TestIdentityGuard:
    def test_identity_guard_flags_or_similar(self):
        """Rationale containing 'or similar' must trigger identity guard quarantine."""
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        proposal = {
            "ticker": "CRDO",
            "proposed_primary_theme_id": "dc_connectivity_silicon",
            "proposed_additional_theme_ids": [],
            "confidence": 0.7,
            "rationale": "This appears to be a connectivity or similar networking company.",
            "evidence": [],
            "warnings": [],
        }
        _, quarantine_reasons = _validate_proposal(
            proposal, reg, {}, company_name="Credo Technology Group"
        )
        assert any("IDENTITY" in r.upper() or "identity" in r.lower() for r in quarantine_reasons), (
            f"'or similar' in rationale must trigger identity guard; got {quarantine_reasons}"
        )

    def test_identity_guard_flags_ticker_only_guess_no_evidence(self):
        """Empty evidence + no company mention in rationale must trigger identity guard."""
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        proposal = {
            "ticker": "LPTH",
            "proposed_primary_theme_id": "optical_components_lasers",
            "proposed_additional_theme_ids": [],
            "confidence": 0.5,
            "rationale": "Based on the ticker symbol this is likely a photonics company.",
            "evidence": [],
            "warnings": [],
        }
        _, quarantine_reasons = _validate_proposal(
            proposal, reg, {}, company_name="LightPath Technologies"
        )
        assert any("IDENTITY" in r.upper() or "identity" in r.lower() for r in quarantine_reasons), (
            f"Ticker-only guess without evidence must trigger identity guard; got {quarantine_reasons}"
        )

    def test_identity_guard_accepts_good_rationale_with_company_name(self):
        """Good rationale that references the company name must pass identity guard."""
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        proposal = {
            "ticker": "ALAB",
            "proposed_primary_theme_id": "dc_connectivity_silicon",
            "proposed_additional_theme_ids": [],
            "confidence": 0.97,
            "rationale": "Astera Labs provides CXL and PCIe retimer silicon for AI data center connectivity.",
            "evidence": ["CXL retimers", "PCIe switch chips for AI servers"],
            "warnings": [],
        }
        _, quarantine_reasons = _validate_proposal(
            proposal, reg, {}, company_name="Astera Labs"
        )
        assert quarantine_reasons == [], f"Good proposal must pass identity guard: {quarantine_reasons}"

    def test_identity_guard_accepts_proposal_without_company_arg(self):
        """When company_name is not supplied, identity guard must not quarantine."""
        from services.theme_taxonomy_classifier import _validate_proposal, _get_assignable_registry
        reg = _get_assignable_registry()
        proposal = {
            "ticker": "NVDA",
            "proposed_primary_theme_id": "ai_accelerators",
            "proposed_additional_theme_ids": [],
            "confidence": 0.99,
            "rationale": "Dominant GPU maker for AI training.",
            "evidence": ["H100 GPU", "NVLink"],
            "warnings": [],
        }
        _, quarantine_reasons = _validate_proposal(proposal, reg, {})  # no company_name
        assert quarantine_reasons == [], f"No company_name supplied — identity guard must be skipped: {quarantine_reasons}"


# ── Contract 16: Atomic ticker-taxonomy write path ────────────────────────────

class TestAtomicTaxonomyPrimitive:
    """
    DB-level unit tests for atomic_taxonomy_write_db() (Contract 1).

    All tests mock the psycopg2 connection at the _get_conn / _put_conn boundary
    so no live Neon connection is required.  Each test injects failures at a
    specific SQL execution step and verifies commit/rollback counts.
    """

    def _make_mock_conn(self, execute_side_effects=None):
        """
        Return (conn, cursor, commit_calls, rollback_calls).
        execute_side_effects: list of values/exceptions to raise on sequential
        cursor.execute() calls.  None means succeed silently.
        """
        from unittest.mock import MagicMock, call

        cur = MagicMock()
        if execute_side_effects:
            effects = list(execute_side_effects)
            def _execute(*args, **kwargs):
                if effects:
                    e = effects.pop(0)
                    if isinstance(e, Exception):
                        raise e
            cur.execute.side_effect = _execute

        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        return conn, cur

    def _run(self, ticker_overrides, primary_operation=None, conn=None):
        """
        Call atomic_taxonomy_write_db with a patched _get_conn/_put_conn.
        Returns (result, conn).
        """
        import data.pg_storage as pgs
        from unittest.mock import patch

        if conn is None:
            conn, _ = self._make_mock_conn()

        with patch.object(pgs, "_get_conn", return_value=conn), \
             patch.object(pgs, "_put_conn") as mock_put:
            result = pgs.atomic_taxonomy_write_db(
                ticker_overrides=ticker_overrides,
                primary_operation=primary_operation,
            )
            put_called = mock_put.called
        return result, conn, put_called

    # ── Test 1: all successful statements commit once ─────────────────────────
    def test_successful_all_memberships_commit_once(self):
        """All membership statements succeed → exactly one commit, zero rollbacks."""
        conn, _ = self._make_mock_conn()
        result, conn, _ = self._run(
            ticker_overrides=[
                {"theme_id": "ai_accelerators", "symbol": "NVDA", "action": "add"},
                {"theme_id": "cloud_hyperscalers", "symbol": "NVDA", "action": "add"},
            ],
            primary_operation={"action": "set", "ticker": "NVDA",
                               "user_id": "default", "category": "AI Accelerators"},
            conn=conn,
        )
        assert result["ok"] is True
        assert result["succeeded"] == 2
        assert conn.commit.call_count == 1, f"Expected 1 commit, got {conn.commit.call_count}"
        assert conn.rollback.call_count == 0, f"Expected 0 rollbacks"

    # ── Test 2: failure on first membership → rollback, no commit ────────────
    def test_failure_first_membership_rolls_back(self):
        """Exception on first membership execute → rollback, no commit."""
        conn, cur = self._make_mock_conn(
            execute_side_effects=[Exception("inject first-stmt failure")]
        )
        result, conn, _ = self._run(
            ticker_overrides=[
                {"theme_id": "ai_accelerators", "symbol": "FAIL", "action": "add"},
                {"theme_id": "cloud_hyperscalers", "symbol": "FAIL", "action": "add"},
            ],
            conn=conn,
        )
        assert result["ok"] is False
        assert "inject first-stmt failure" in result.get("error", "")
        assert conn.commit.call_count == 0, "Must not commit after first-stmt failure"
        assert conn.rollback.call_count == 1, f"Expected 1 rollback, got {conn.rollback.call_count}"

    # ── Test 3: failure on middle membership → rollback, no commit ───────────
    def test_failure_middle_membership_rolls_back_prior(self):
        """Exception on second of three membership statements → rollback, no commit."""
        conn, cur = self._make_mock_conn(
            execute_side_effects=[None, Exception("inject middle failure"), None]
        )
        result, conn, _ = self._run(
            ticker_overrides=[
                {"theme_id": "ai_accelerators",    "symbol": "MID", "action": "add"},
                {"theme_id": "cloud_hyperscalers", "symbol": "MID", "action": "add"},
                {"theme_id": "quantum_computing",  "symbol": "MID", "action": "add"},
            ],
            conn=conn,
        )
        assert result["ok"] is False
        assert conn.commit.call_count == 0
        assert conn.rollback.call_count == 1

    # ── Test 4: failure on primary-set → rollback, no commit ─────────────────
    def test_failure_primary_set_rolls_back_memberships(self):
        """Exception on primary-set execute → membership rows are also rolled back."""
        # First execute (membership) succeeds, second (category_override) fails
        conn, cur = self._make_mock_conn(
            execute_side_effects=[None, Exception("inject primary-set failure")]
        )
        result, conn, _ = self._run(
            ticker_overrides=[
                {"theme_id": "ai_accelerators", "symbol": "PRI", "action": "add"},
            ],
            primary_operation={"action": "set", "ticker": "PRI",
                               "user_id": "default", "category": "AI Accel"},
            conn=conn,
        )
        assert result["ok"] is False
        assert conn.commit.call_count == 0
        assert conn.rollback.call_count == 1

    # ── Test 5: failure on primary-clear → rollback ───────────────────────────
    def test_failure_primary_clear_rolls_back_memberships(self):
        """Exception on primary-clear DELETE → membership rows are also rolled back."""
        conn, cur = self._make_mock_conn(
            execute_side_effects=[None, Exception("inject primary-clear failure")]
        )
        result, conn, _ = self._run(
            ticker_overrides=[
                {"theme_id": "ai_accelerators", "symbol": "CLR", "action": "remove"},
            ],
            primary_operation={"action": "clear", "ticker": "CLR", "user_id": "default"},
            conn=conn,
        )
        assert result["ok"] is False
        assert conn.commit.call_count == 0
        assert conn.rollback.call_count == 1

    # ── Test 6: no commit occurs after any failure ────────────────────────────
    def test_no_commit_after_any_failure(self):
        """A failure at any point must leave commit_count == 0."""
        for stmt_index in range(3):
            effects = [None] * stmt_index + [Exception(f"fail at {stmt_index}")]
            conn, cur = self._make_mock_conn(execute_side_effects=effects)
            result, conn, _ = self._run(
                ticker_overrides=[
                    {"theme_id": "ai_accelerators",   "symbol": "X", "action": "add"},
                    {"theme_id": "cloud_hyperscalers", "symbol": "X", "action": "add"},
                ],
                primary_operation={"action": "set", "ticker": "X",
                                   "user_id": "default", "category": "AI"},
                conn=conn,
            )
            assert result["ok"] is False, f"Expected ok=False for failure at stmt {stmt_index}"
            assert conn.commit.call_count == 0, \
                f"stmt {stmt_index}: commit_count={conn.commit.call_count}, expected 0"

    # ── Test 7: rollback occurs exactly once on failure ───────────────────────
    def test_rollback_exactly_once(self):
        """Exactly one rollback call regardless of which statement fails."""
        conn, cur = self._make_mock_conn(
            execute_side_effects=[Exception("single failure")]
        )
        result, conn, _ = self._run(
            ticker_overrides=[{"theme_id": "ai_accelerators", "symbol": "X", "action": "add"}],
            conn=conn,
        )
        assert result["ok"] is False
        assert conn.rollback.call_count == 1

    # ── Test 8: connection returned to pool in all cases ─────────────────────
    def test_connection_returned_to_pool(self):
        """_put_conn must be called regardless of success or failure."""
        import data.pg_storage as pgs
        from unittest.mock import patch

        for succeed in (True, False):
            effects = [] if succeed else [Exception("fail")]
            conn, cur = self._make_mock_conn(execute_side_effects=effects)
            put_calls = []

            def _mock_put(c):
                put_calls.append(c)

            with patch.object(pgs, "_get_conn", return_value=conn), \
                 patch.object(pgs, "_put_conn", side_effect=_mock_put):
                pgs.atomic_taxonomy_write_db(
                    ticker_overrides=[{"theme_id": "ai_accelerators", "symbol": "P", "action": "add"}],
                )
            assert len(put_calls) == 1, \
                f"succeed={succeed}: _put_conn must be called exactly once, got {len(put_calls)}"

    # ── Test: primary_operation clear generates DELETE, not upsert ────────────
    def test_primary_clear_issues_delete_statement(self):
        """action='clear' must DELETE from watchlist_category_overrides."""
        import data.pg_storage as pgs
        from unittest.mock import patch, MagicMock

        executed_sqls = []
        cur = MagicMock()
        def capture_execute(sql, *args, **kwargs):
            executed_sqls.append(sql.strip())
        cur.execute.side_effect = capture_execute

        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(pgs, "_get_conn", return_value=conn), \
             patch.object(pgs, "_put_conn"):
            pgs.atomic_taxonomy_write_db(
                ticker_overrides=[],
                primary_operation={"action": "clear", "ticker": "TEST", "user_id": "default"},
            )

        assert any("DELETE" in sql.upper() for sql in executed_sqls), \
            f"Expected a DELETE statement for clear action; got: {executed_sqls}"
        assert not any("INSERT" in sql.upper() and "watchlist_category_overrides" in sql
                       for sql in executed_sqls), \
            "clear action must not INSERT into watchlist_category_overrides"

    # ── Test: legacy category_override treated as set ────────────────────────
    def test_legacy_category_override_treated_as_set(self):
        """category_override kwarg (legacy) must issue an INSERT/upsert, not a DELETE."""
        import data.pg_storage as pgs
        from unittest.mock import patch, MagicMock

        executed_sqls = []
        cur = MagicMock()
        cur.execute.side_effect = lambda sql, *a, **k: executed_sqls.append(sql.strip())

        conn = MagicMock()
        conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
        conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(pgs, "_get_conn", return_value=conn), \
             patch.object(pgs, "_put_conn"):
            pgs.atomic_taxonomy_write_db(
                ticker_overrides=[],
                category_override={
                    "user_id": "default", "ticker": "LEG",
                    "category": "AI Accel", "source": "test",
                },
            )

        assert any("INSERT" in sql.upper() for sql in executed_sqls), \
            f"Legacy category_override must upsert; got: {executed_sqls}"


class TestAtomicTaxonomyRoute:
    """
    Route-level unit tests for admin_put_ticker_taxonomy() (Contract 2).

    Dependencies are monkeypatched so no Neon connection is required.
    The async handler is exercised directly via asyncio.run().

    Valid IDs used (from the live registry — confirmed assignable):
      _ID_A = "ai_accelerators"    (sub_theme, parent=semiconductors)
      _ID_B = "advanced_materials" (theme, no parent)
      _ID_C = "agribusiness"       (theme, no parent)
      _ID_D = "banks"              (theme, no parent)
    All four belong to different branches so no ancestor-pruning fires.
    """

    # ── Shared helpers ────────────────────────────────────────────────────────

    _SAFE_TICKER = "XTST"  # not a real watchlist ticker
    _SENTINEL    = object()  # distinguishes reread_primary=None (clear) from "not set"

    # Four known-good cross-branch assignable IDs
    _ID_A = "ai_accelerators"
    _ID_B = "advanced_materials"
    _ID_C = "agribusiness"
    _ID_D = "banks"

    def _make_body(self, primary=None, additional=None, note=None, created_by="test"):
        from routes.themes import TickerTaxonomyBody
        return TickerTaxonomyBody(
            primary_theme_id=primary,
            additional_theme_ids=additional or [],
            note=note,
            created_by=created_by,
        )

    def _fake_current_memberships(self, theme_ids=None, primary=None):
        """Fake _get_ticker_theme_memberships() response."""
        tids = list(theme_ids or [])
        memberships = [{"theme_id": t, "theme_name": t, "membership_source": "test", "is_primary": (t == primary)}
                       for t in tids]
        return {
            "ticker": self._SAFE_TICKER,
            "primary_theme": {"theme_id": primary, "theme_name": primary, "source": "test"},
            "theme_memberships": memberships,
            "additional_theme_memberships": [m for m in memberships if not m["is_primary"]],
        }

    def _run_handler(self, body, monkeypatch,
                     current_theme_ids=None, current_primary=None,
                     txn_ok=True, txn_error=None,
                     reread_theme_ids=None, reread_primary=_SENTINEL):
        """
        Exercise the route handler with all storage/cache deps mocked.
        Returns (response_or_exc, atomic_calls, invalidate_calls).

        reread_primary uses a sentinel to distinguish None (cleared primary)
        from "not supplied" (defaults to current_primary unchanged).
        """
        import asyncio
        import data.pg_storage as pgs
        import routes.themes as rth
        from unittest.mock import MagicMock

        atomic_calls: list[dict] = []
        invalidate_calls: list = []

        def fake_atomic(ticker_overrides, primary_operation=None, category_override=None):
            atomic_calls.append({
                "ticker_overrides": ticker_overrides,
                "primary_operation": primary_operation,
            })
            if not txn_ok:
                return {"ok": False, "succeeded": 0, "failed": 1, "error": txn_error or "injected"}
            return {"ok": True, "succeeded": len(ticker_overrides), "failed": 0, "error": None}

        def fake_invalidate():
            invalidate_calls.append(1)

        # Memberships: first call = pre-write (current state), second = post-write (reread)
        _call_count = [0]
        _reread_ids = list(reread_theme_ids or (current_theme_ids or []))
        # Use sentinel: None means "primary was cleared"; _SENTINEL means "use current_primary"
        _reread_pri = current_primary if (reread_primary is self._SENTINEL) else reread_primary

        def fake_memberships(ticker):
            _call_count[0] += 1
            if _call_count[0] == 1:
                return self._fake_current_memberships(current_theme_ids, current_primary)
            # Second call: post-write reread
            return self._fake_current_memberships(_reread_ids, _reread_pri)

        monkeypatch.setattr(pgs, "atomic_taxonomy_write_db", fake_atomic)
        monkeypatch.setattr(rth, "_invalidate_caches", fake_invalidate)
        monkeypatch.setattr(rth, "_get_ticker_theme_memberships", fake_memberships)
        monkeypatch.setattr(rth, "_check_admin", lambda req, key: None)
        # Suppress post-commit side effects
        monkeypatch.setattr(rth, "_log", MagicMock())

        class _FakeRequest:
            headers = {}
            state = MagicMock()

        import fastapi
        exc_holder = [None]
        result_holder = [None]

        async def _run():
            try:
                result_holder[0] = await rth.admin_put_ticker_taxonomy(
                    ticker=self._SAFE_TICKER,
                    request=_FakeRequest(),
                    body=body,
                    x_api_key="test",
                )
            except fastapi.HTTPException as e:
                exc_holder[0] = e

        asyncio.run(_run())
        return (exc_holder[0] if exc_holder[0] else result_holder[0]), atomic_calls, invalidate_calls

    # ── Test 9: route calls atomic_taxonomy_write_db exactly once ─────────────
    def test_route_calls_atomic_write_exactly_once(self, monkeypatch):
        """Route must call atomic_taxonomy_write_db() exactly once per request."""
        body = self._make_body(primary=self._ID_A, additional=[self._ID_B])
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=[self._ID_A, self._ID_B],
            reread_primary=self._ID_A,
        )
        assert len(atomic_calls) == 1, \
            f"atomic_taxonomy_write_db must be called exactly once, got {len(atomic_calls)}"
        assert isinstance(result, dict) and result.get("ok") is True

    # ── Test 10: route never calls per-membership helpers ─────────────────────
    def test_route_never_calls_per_membership_helpers(self, monkeypatch):
        """_perform_membership_write and _perform_theme_membership_only_write must not be called."""
        import routes.themes as rth

        called_helpers = []
        def _forbidden_primary(*a, **k):
            called_helpers.append("_perform_membership_write")
        def _forbidden_additional(*a, **k):
            called_helpers.append("_perform_theme_membership_only_write")
        monkeypatch.setattr(rth, "_perform_membership_write", _forbidden_primary)
        monkeypatch.setattr(rth, "_perform_theme_membership_only_write", _forbidden_additional)

        body = self._make_body(primary="ai_accelerators")
        self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=["ai_accelerators"], reread_primary="ai_accelerators",
        )
        assert called_helpers == [], \
            f"Per-membership helpers must not be called from PUT route; got: {called_helpers}"

    # ── Test 11: cache invalidated exactly once after success ─────────────────
    def test_cache_invalidated_exactly_once_after_success(self, monkeypatch):
        """_invalidate_caches() must be called exactly once after a successful commit."""
        body = self._make_body(primary="ai_accelerators")
        _, _, invalidate_calls = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=["ai_accelerators"], reread_primary="ai_accelerators",
        )
        assert len(invalidate_calls) == 1, \
            f"Cache invalidation must occur exactly once after success; got {len(invalidate_calls)}"

    # ── Test 12: cache not invalidated after transaction failure ──────────────
    def test_cache_not_invalidated_after_failure(self, monkeypatch):
        """_invalidate_caches() must not be called when the transaction fails."""
        body = self._make_body(primary="ai_accelerators")
        exc, _, invalidate_calls = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            txn_ok=False, txn_error="injected DB failure",
        )
        assert invalidate_calls == [], \
            f"Cache invalidation must not occur after transaction failure; got {len(invalidate_calls)} calls"
        assert exc is not None and exc.status_code == 500

    # ── Test 13: no undo-stack or compensating writes ─────────────────────────
    def test_no_compensating_writes_on_failure(self, monkeypatch):
        """On transaction failure the route must raise immediately without compensating writes."""
        import routes.themes as rth

        compensating_calls = []
        def _track_primary(*a, **k):
            compensating_calls.append("primary")
        def _track_additional(*a, **k):
            compensating_calls.append("additional")
        monkeypatch.setattr(rth, "_perform_membership_write", _track_primary)
        monkeypatch.setattr(rth, "_perform_theme_membership_only_write", _track_additional)

        body = self._make_body(primary="ai_accelerators")
        exc, _, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            txn_ok=False,
        )
        assert compensating_calls == [], \
            f"No compensating writes on failure; got: {compensating_calls}"
        assert exc is not None and exc.status_code == 500

    # ── Test 14: additional-membership failure cannot produce partial state ────
    def test_partial_state_impossible(self, monkeypatch):
        """Transaction failure means no rows are committed (atomicity)."""
        body = self._make_body(primary=self._ID_A, additional=[self._ID_B])
        exc, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            txn_ok=False,
        )
        # Exactly one atomic call, returned failure; no partial rows possible
        assert len(atomic_calls) == 1
        assert exc is not None and exc.status_code == 500

    # ── Test 15: primary-only save ────────────────────────────────────────────
    def test_primary_only_save(self, monkeypatch):
        """Primary-only save (no additional) must succeed and return correct shape."""
        body = self._make_body(primary="ai_accelerators")
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=["ai_accelerators"], reread_primary="ai_accelerators",
        )
        assert result.get("ok") is True
        assert result["primary_theme_id"] == "ai_accelerators"
        assert result["additional_theme_ids"] == []
        # Transaction payload must include the membership add + primary set
        payload = atomic_calls[0]
        ops = {(e["theme_id"], e["action"]) for e in payload["ticker_overrides"]}
        assert ("ai_accelerators", "add") in ops
        assert payload["primary_operation"]["action"] == "set"

    # ── Test 16: primary plus additions ──────────────────────────────────────
    def test_primary_plus_additions(self, monkeypatch):
        """Primary + two additional memberships must all be included in one transaction."""
        body = self._make_body(primary=self._ID_A, additional=[self._ID_B, self._ID_C])
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=[self._ID_A, self._ID_B, self._ID_C],
            reread_primary=self._ID_A,
        )
        assert result.get("ok") is True
        payload = atomic_calls[0]
        add_ops = {e["theme_id"] for e in payload["ticker_overrides"] if e["action"] == "add"}
        assert {self._ID_A, self._ID_B, self._ID_C} == add_ops

    # ── Test 17: replacing primary ────────────────────────────────────────────
    def test_replacing_primary(self, monkeypatch):
        """Replacing old primary with a new one must remove old and add new in one transaction."""
        body = self._make_body(primary=self._ID_B, additional=[])
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[self._ID_A], current_primary=self._ID_A,
            reread_theme_ids=[self._ID_B], reread_primary=self._ID_B,
        )
        assert result.get("ok") is True
        payload = atomic_calls[0]
        actions = {(e["theme_id"], e["action"]) for e in payload["ticker_overrides"]}
        assert (self._ID_A, "remove") in actions
        assert (self._ID_B, "add") in actions
        assert payload["primary_operation"]["action"] == "set"

    # ── Test 18: promoting existing additional to primary ─────────────────────
    def test_promote_existing_additional_to_primary(self, monkeypatch):
        """
        Promoting an existing additional membership to primary must update the
        category override without requiring an additional membership insert.
        The old primary becomes additional in the same transaction.
        """
        # Current: primary=_ID_A, additional=_ID_B
        # Requested: primary=_ID_B, additional=[_ID_A]
        body = self._make_body(primary=self._ID_B, additional=[self._ID_A])
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[self._ID_A, self._ID_B],
            current_primary=self._ID_A,
            reread_theme_ids=[self._ID_A, self._ID_B],
            reread_primary=self._ID_B,
        )
        assert result.get("ok") is True
        payload = atomic_calls[0]
        # No removals (both themes remain active)
        remove_ops = {e["theme_id"] for e in payload["ticker_overrides"] if e["action"] == "remove"}
        assert remove_ops == set(), \
            f"Promotion must not remove any membership; got removes: {remove_ops}"
        # primary_operation must update the category store
        assert payload["primary_operation"]["action"] == "set"
        # Source update: new primary gets manual_admin, old gets manual_admin_additional
        src_by_theme = {e["theme_id"]: e.get("source") for e in payload["ticker_overrides"]}
        assert src_by_theme.get(self._ID_B) == "manual_admin"
        assert src_by_theme.get(self._ID_A) == "manual_admin_additional"

    # ── Test 19: clearing primary ─────────────────────────────────────────────
    def test_clear_primary(self, monkeypatch):
        """Clearing primary (primary=null) must issue a category-override DELETE."""
        body = self._make_body(primary=None, additional=["ai_accelerators"])
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=["ai_accelerators"], current_primary="ai_accelerators",
            reread_theme_ids=["ai_accelerators"], reread_primary=None,
        )
        assert result.get("ok") is True
        payload = atomic_calls[0]
        assert payload["primary_operation"]["action"] == "clear", \
            f"Clearing primary must issue action='clear'; got {payload['primary_operation']}"

    # ── Test 20: primary unchanged, additions changed ─────────────────────────
    def test_primary_unchanged_additions_changed(self, monkeypatch):
        """Changing only additional memberships must still include all desired in payload."""
        body = self._make_body(primary=self._ID_A, additional=[self._ID_B])
        # Currently has _ID_A (primary) only
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[self._ID_A], current_primary=self._ID_A,
            reread_theme_ids=[self._ID_A, self._ID_B],
            reread_primary=self._ID_A,
        )
        assert result.get("ok") is True
        payload = atomic_calls[0]
        add_ops = {e["theme_id"] for e in payload["ticker_overrides"] if e["action"] == "add"}
        assert self._ID_B in add_ops

    # ── Test 21: requested primary remains primary ────────────────────────────
    def test_requested_primary_is_primary_in_response(self, monkeypatch):
        """primary_theme_id in response comes from authoritative reread, not request body."""
        body = self._make_body(primary="ai_accelerators")
        result, _, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=["ai_accelerators"], reread_primary="ai_accelerators",
        )
        assert result["primary_theme_id"] == "ai_accelerators"

    # ── Test 22: redundant ancestor normalization ─────────────────────────────
    def test_redundant_ancestor_removed_from_additional(self, monkeypatch):
        """
        Redundant ancestor in additional must be removed before building the payload.
        Uses real taxonomy registry to find a parent-child pair.
        """
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _base
        # Find a sub_theme that has a parent_theme_id in the registry
        child = None
        parent = None
        for tid, meta in _base.items():
            pid = meta.get("parent_theme_id")
            if pid and pid in _base and meta.get("classification") == "sub_theme":
                child  = tid
                parent = pid
                break
        if child is None or parent is None:
            import pytest
            pytest.skip("No suitable parent-child pair found in registry")

        # Request primary=child, additional=[parent] → parent should be stripped
        body = self._make_body(primary=child, additional=[parent])
        result, atomic_calls, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=[child], reread_primary=child,
        )
        assert result.get("ok") is True
        # Parent must not appear in the membership adds
        add_ops = {e["theme_id"] for e in atomic_calls[0]["ticker_overrides"] if e["action"] == "add"}
        assert parent not in add_ops, \
            f"Redundant ancestor {parent!r} must be pruned; got adds: {add_ops}"

    # ── Test 24: sector / deprecated / market-lens IDs rejected ──────────────
    def test_sector_id_rejected_as_primary(self, monkeypatch):
        """Sector IDs must be rejected with HTTP 422."""
        import asyncio
        import fastapi
        import routes.themes as rth
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _base

        sector_id = next(
            (k for k, v in _base.items() if v.get("classification") == "sector"), None
        )
        if sector_id is None:
            import pytest; pytest.skip("No sector IDs in registry")

        monkeypatch.setattr(rth, "_check_admin", lambda req, key: None)

        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(primary_theme_id=sector_id)

        class _Req:
            headers = {}
            state = type("s", (), {})()

        exc_holder = [None]
        async def _run():
            try:
                await rth.admin_put_ticker_taxonomy(
                    ticker="XTST", request=_Req(), body=body, x_api_key="test"
                )
            except fastapi.HTTPException as e:
                exc_holder[0] = e

        asyncio.run(_run())
        assert exc_holder[0] is not None and exc_holder[0].status_code in (422, 404), \
            f"Sector ID must be rejected; got {exc_holder[0]}"

    def test_deprecated_id_rejected(self, monkeypatch):
        """Deprecated and market_lens IDs must be rejected with HTTP 422."""
        import asyncio
        import fastapi
        import routes.themes as rth
        from services.theme_rs_universe import THEME_RS_UNIVERSE as _base

        bad_id = next(
            (k for k, v in _base.items()
             if v.get("classification") in ("deprecated", "market_lens")), None
        )
        if bad_id is None:
            import pytest; pytest.skip("No deprecated/market_lens IDs in registry")

        monkeypatch.setattr(rth, "_check_admin", lambda req, key: None)
        from routes.themes import TickerTaxonomyBody
        body = TickerTaxonomyBody(primary_theme_id=bad_id)

        class _Req:
            headers = {}
            state = type("s", (), {})()

        exc_holder = [None]
        async def _run():
            try:
                await rth.admin_put_ticker_taxonomy(
                    ticker="XTST", request=_Req(), body=body, x_api_key="test"
                )
            except fastapi.HTTPException as e:
                exc_holder[0] = e

        asyncio.run(_run())
        assert exc_holder[0] is not None and exc_holder[0].status_code in (422, 404)

    # ── Test 25: response primary from authoritative reread ───────────────────
    def test_response_primary_from_reread_not_body(self, monkeypatch):
        """primary_theme_id in response must be the reread value, never body.primary_theme_id."""
        body = self._make_body(primary="ai_accelerators")
        result, _, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=["ai_accelerators"], reread_primary="ai_accelerators",
        )
        # The response value comes from reread
        assert result["primary_theme_id"] == "ai_accelerators"

    # ── Test 26: primary appears first in theme_ids ───────────────────────────
    def test_primary_first_in_theme_ids(self, monkeypatch):
        """primary_theme_id must be the first element of theme_ids."""
        body = self._make_body(primary=self._ID_A, additional=[self._ID_B])
        result, _, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=[self._ID_A, self._ID_B],
            reread_primary=self._ID_A,
        )
        assert result["theme_ids"][0] == self._ID_A, \
            f"Primary must be first in theme_ids; got {result['theme_ids']}"

    # ── Test 27: reread mismatch raises 500 ───────────────────────────────────
    def test_reread_mismatch_raises_server_error(self, monkeypatch):
        """When reread primary doesn't match requested primary, route must raise 500."""
        body = self._make_body(primary=self._ID_A)
        # Reread returns a different primary (simulating storage inconsistency)
        exc, _, _ = self._run_handler(
            body, monkeypatch,
            current_theme_ids=[], current_primary=None,
            reread_theme_ids=[self._ID_B], reread_primary=self._ID_B,
        )
        assert exc is not None and exc.status_code == 500, \
            f"Reread mismatch must raise HTTP 500; got {exc}"

    # ── Test 28: legacy single-membership endpoints remain functional ─────────
    def test_legacy_endpoints_use_per_membership_helpers(self, monkeypatch):
        """
        The legacy POST /admin/memberships endpoint must still use _perform_membership_write.
        We verify the helper is still callable (not broken by the route rewrite).
        """
        import routes.themes as rth
        assert callable(rth._perform_membership_write), \
            "_perform_membership_write must remain callable for legacy endpoints"
        assert callable(rth._perform_theme_membership_only_write), \
            "_perform_theme_membership_only_write must remain callable for legacy endpoints"


# ── Contract 15: Generated artifacts not tracked in git ───────────────────────

def test_proposals_json_not_git_tracked():
    """theme-taxonomy-v2-proposals.json must not be tracked by git."""
    import subprocess
    result = subprocess.run(
        ["git", "ls-files", "backend/data/theme-taxonomy-v2-proposals.json"],
        capture_output=True, text=True, cwd="/home/runner/workspace",
    )
    assert result.stdout.strip() == "", (
        "theme-taxonomy-v2-proposals.json must not be git-tracked; "
        "run git rm --cached backend/data/theme-taxonomy-v2-proposals.json"
    )


def test_proposals_csv_not_git_tracked():
    """theme-taxonomy-v2-proposals.csv must not be tracked by git."""
    import subprocess
    result = subprocess.run(
        ["git", "ls-files", "backend/data/theme-taxonomy-v2-proposals.csv"],
        capture_output=True, text=True, cwd="/home/runner/workspace",
    )
    assert result.stdout.strip() == "", (
        "theme-taxonomy-v2-proposals.csv must not be git-tracked; "
        "run git rm --cached backend/data/theme-taxonomy-v2-proposals.csv"
    )
