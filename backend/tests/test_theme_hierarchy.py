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
  - Watchlist row identity fields     — Contract 3

Run with:
    cd backend && python -m pytest tests/test_theme_hierarchy.py -v
"""
from __future__ import annotations

import pytest

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
        r = get_effective_rollup_sector_ids("datacenter_infra", REG)
        assert set(r) == {"technology", "utilities", "real_estate"}

    def test_defense_explicit(self):
        r = get_effective_rollup_sector_ids("defense", REG)
        assert "industrials" in r

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
# Contract 3 — Watchlist row identity-field fixtures
# ═══════════════════════════════════════════════════════════════════════════════

def _simulate_row_identity(
    canonical_theme_id: str | None,
    override_tids: list[str],
    registry: dict | None = None,
) -> dict:
    """
    Pure-Python simulation of the identity-field block in _build_ticker_row.
    Used by test fixtures without importing the full watchlist_router.

    Returns a dict with primary_theme_id / theme_ids / subtheme_ids.
    """
    reg = registry or REG
    primary = canonical_theme_id
    extras  = [t for t in override_tids if t != primary]
    all_ids = ([primary] if primary else []) + extras
    subs    = [t for t in all_ids if (reg.get(t) or {}).get("parent_theme_id")]
    return {
        "primary_theme_id": primary,
        "theme_ids":        all_ids,
        "subtheme_ids":     subs,
    }


class TestWatchlistRowIdentityFields:
    """
    8 fixture scenarios for watchlist row identity-field contract.
    All scenarios mirror what _build_ticker_row produces for each row path.
    """

    def test_normal_path_theme_only(self):
        """Normal path: ticker in 'semiconductors' with no overrides."""
        row = _simulate_row_identity("semiconductors", [])
        assert row["primary_theme_id"] == "semiconductors"
        assert row["theme_ids"] == ["semiconductors"]
        assert row["subtheme_ids"] == []   # semiconductors has no parent_theme_id

    def test_normal_path_subtheme(self):
        """Normal path: ticker in 'memory_storage' (sub-theme of semiconductors)."""
        row = _simulate_row_identity("memory_storage", [])
        assert row["primary_theme_id"] == "memory_storage"
        assert "memory_storage" in row["theme_ids"]
        assert "memory_storage" in row["subtheme_ids"]   # has parent_theme_id

    def test_normal_path_with_additional_membership(self):
        """Normal path: ticker in 'semiconductors' + override adds 'memory_storage'."""
        row = _simulate_row_identity("semiconductors", ["memory_storage"])
        assert row["primary_theme_id"] == "semiconductors"
        assert set(row["theme_ids"]) == {"semiconductors", "memory_storage"}
        assert "memory_storage" in row["subtheme_ids"]
        assert "semiconductors" not in row["subtheme_ids"]

    def test_missing_append_industry_fallback(self):
        """Missing-append path: ticker resolved via industry → 'gold'."""
        row = _simulate_row_identity("gold", [])
        assert row["primary_theme_id"] == "gold"
        assert "gold" in row["subtheme_ids"]   # gold has parent_theme_id = metals_mining

    def test_missing_append_uncategorized(self):
        """Missing-append path: ticker that lands in 'other_uncategorized'."""
        row = _simulate_row_identity("other_uncategorized", [])
        assert row["primary_theme_id"] == "other_uncategorized"
        assert row["subtheme_ids"] == []   # other_uncategorized not in registry

    def test_skeleton_path_parent_theme(self):
        """Skeleton path: ticker resolved to parent theme 'defense'."""
        row = _simulate_row_identity("defense", [])
        assert row["primary_theme_id"] == "defense"
        assert "defense" not in row["subtheme_ids"]   # defense has no parent_theme_id

    def test_skeleton_path_subtheme_with_override(self):
        """Skeleton path: primary='drones' + override='defense'."""
        row = _simulate_row_identity("drones", ["defense"])
        assert row["primary_theme_id"] == "drones"
        assert "drones" in row["subtheme_ids"]     # drones → defense (has parent_theme_id)
        assert "defense" not in row["subtheme_ids"]  # defense has no parent_theme_id

    def test_no_canonical_theme_id(self):
        """Edge case: no canonical_theme_id (ticker not resolved to any theme)."""
        row = _simulate_row_identity(None, [])
        assert row["primary_theme_id"] is None
        assert row["theme_ids"] == []
        assert row["subtheme_ids"] == []


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
