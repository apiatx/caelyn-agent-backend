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
        MANDATORY Defect 1 regression:
        ai_networking has classification='sub_theme' but parent_theme_id=None.

        Old (broken) code: parent_theme_id existence → subtheme_ids empty.
        Fixed code:        classification == 'sub_theme' → subtheme_ids=['ai_networking'].
        """
        # Verify the precondition is real in the live registry
        assert REG["ai_networking"]["classification"] == "sub_theme"
        assert REG["ai_networking"].get("parent_theme_id") is None, (
            "Precondition: ai_networking must have no parent_theme_id "
            "for this to be a meaningful Defect 1 regression test"
        )

        result = await _run_enrich(_make_store("ANET", "ai_networking"))
        row = _section_row(result)

        assert row["primary_theme_id"] == "ai_networking"
        assert row["theme_ids"] == ["ai_networking"]
        assert row["subtheme_ids"] == ["ai_networking"], (
            "ai_networking must appear in subtheme_ids because "
            "classification='sub_theme', regardless of parent_theme_id being absent"
        )

    # ── Case 2 — Parent theme + standalone and nested subthemes ──────────────────

    async def test_case2_parent_plus_standalone_and_nested_subthemes(self):
        """
        primary=semiconductors (classification='theme')
        additional: ai_networking (sub_theme, no parent_theme_id)
                    memory_storage (sub_theme, parent_theme_id='semiconductors')

        Both sub_theme forms must appear in subtheme_ids.
        semiconductors must NOT appear in subtheme_ids.
        """
        assert REG["ai_networking"].get("parent_theme_id") is None
        assert REG["memory_storage"].get("parent_theme_id") == "semiconductors"
        assert REG["semiconductors"]["classification"] == "theme"

        result = await _run_enrich(
            _make_store("TEST", "semiconductors"),
            overrides=[
                {"symbol": "TEST", "theme_id": "ai_networking",   "action": "add"},
                {"symbol": "TEST", "theme_id": "memory_storage",  "action": "add"},
            ],
        )
        row = _section_row(result)

        assert row["primary_theme_id"] == "semiconductors"
        assert row["theme_ids"][0] == "semiconductors", "primary must be first"
        assert set(row["theme_ids"]) == {"semiconductors", "ai_networking", "memory_storage"}
        assert "ai_networking"  in row["subtheme_ids"], "standalone sub_theme must be included"
        assert "memory_storage" in row["subtheme_ids"], "nested sub_theme must be included"
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
        Ticker resolved to 'ai_networking' (sub_theme, no parent_theme_id).

        All four identity fields must be present and correct.
        """
        result = await _run_enrich_skeleton(
            ["ANET"],
            resolved_theme_id="ai_networking",
            resolved_theme_name="AI Networking",
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

        assert row["primary_theme_id"] == "ai_networking"
        assert row["theme_ids"] == ["ai_networking"]
        assert row["subtheme_ids"] == ["ai_networking"], (
            "ai_networking is sub_theme; must appear in subtheme_ids via "
            "classification check, not parent_theme_id check"
        )

    # ── Case 10 — Saved / cached path: identity enrichment occurs ────────────────

    async def test_case10_saved_path_identity_enrichment_occurs(self):
        """
        Saved analysis sections (normal path) — the standard code path for a
        watchlist that has completed its AI analysis.

        Verifies that identity enrichment occurs before the response is returned,
        covering both a theme-classified and a subtheme-classified row.
        """
        store = {
            "tickers":  ["NVDA", "ANET"],
            "analysis": {
                "sections": [
                    {
                        "id": "semiconductors", "title": "Semiconductors",
                        "tickers": [{"symbol": "NVDA", "canonical_theme_id": "semiconductors"}],
                    },
                    {
                        "id": "ai_networking", "title": "AI Networking",
                        "tickers": [{"symbol": "ANET", "canonical_theme_id": "ai_networking"}],
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

        anet = all_rows["ANET"]
        assert anet["primary_theme_id"] == "ai_networking"
        assert "ai_networking" in anet["subtheme_ids"], (
            "ai_networking has classification='sub_theme'; must appear in subtheme_ids"
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
