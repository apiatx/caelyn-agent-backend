"""
Taxonomy V3 Deprecated-Theme Migration
=======================================
Idempotent script — safe to run multiple times.

For every ticker that still has an active (action='add') theme_ticker_overrides
row pointing at a deprecated theme_id:
  1. Tombstones the deprecated row(s)  (action → 'remove' via upsert)
  2. Adds the correct active replacement theme_id where the ticker lacks one
  3. Updates watchlist_category_overrides for the 14 tickers whose primary
     category label still carries a deprecated name

All DB writes use the canonical atomic_taxonomy_write_db primitive.
Cache invalidation is performed once at the end.

Usage:
    cd /home/runner/workspace/backend
    python3.11 migrations/migrate_deprecated_themes.py
"""
from __future__ import annotations

import sys
import os

# Run from backend/ directory so service imports resolve correctly.
_BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

import psycopg2
import subprocess

# ── Canonical migration table ──────────────────────────────────────────────────
# Each entry: (ticker, deprecated_theme_ids[], new_primary_theme_id_or_None,
#              new_category_label_or_None)
#
# new_primary_theme_id = None  → ticker already has a good active primary;
#                                 just remove the deprecated membership(s).
# new_category_label   = None  → no watchlist_category_overrides row to update.
#
# Evidence/reasoning for each decision is in the companion report.

MIGRATIONS: list[tuple[str, list[str], str | None, str | None]] = [
    # ticker,    deprecated_ids,                              new_theme_id,               new_cat_label
    ("AAOI",  ["ai_networking", "photonics_lasers"],        "optical_interconnects",     "Optical Interconnects"),
    ("ABSI",  ["ai_networking"],                            None,                        None),   # keep biotech
    ("ACLS",  ["semicap_equipment"],                        "semicap_equip",             None),
    ("ADTN",  ["ai_networking"],                            "networking_fabric_infra",   "Networking & Fabric Infrastructure"),
    ("AEHR",  ["semicap_equipment", "substrates_packaging"],"test_measurement",          "Test & Measurement"),
    ("AMAT",  ["substrates_packaging"],                     "semicap_equip",             None),
    ("AMCR",  ["substrates_packaging"],                     "advanced_materials",        None),
    ("ASPI",  ["uranium_nuclear"],                          "uranium_nuclear_fuel",      "Uranium Mining & Nuclear Fuel"),
    ("BAND",  ["ai_networking"],                            None,                        None),   # keep memory_storage
    ("CEG",   ["uranium_nuclear"],                          "nuclear_utilities_operators","Nuclear Utilities & Operators"),
    ("CIEN",  ["ai_networking"],                            "optical_interconnects",     None),
    ("DNN",   ["uranium_nuclear"],                          "uranium_nuclear_fuel",      None),
    ("ELVA",  ["lithium_battery"],                          "battery_tech_storage",      "Battery Technology & Energy Storage"),
    ("ENVX",  ["lithium_battery"],                          "battery_tech_storage",      None),
    ("FN",    ["photonics_lasers"],                         "optical_components_lasers", None),
    ("GLW",   ["photonics_lasers", "substrates_packaging"], "optical_components_lasers", "Optical Components & Lasers"),
    ("IMSR",  ["uranium_nuclear"],                          "smr_advanced_reactors",     "SMRs & Advanced Reactors"),
    ("KLAC",  ["semicap_equipment", "substrates_packaging"],"semicap_equip",             None),
    ("LAC",   ["lithium_battery"],                          "lithium",                   None),
    ("LEU",   ["uranium_nuclear"],                          "uranium_nuclear_fuel",      None),
    ("LPTH",  ["semicap_equipment"],                        "optical_components_lasers", "Optical Components & Lasers"),
    ("MAXX",  ["chemicals_materials"],                      "advanced_materials",        None),
    ("MXL",   ["ai_networking"],                            "dc_connectivity_silicon",   None),
    ("NNE",   ["uranium_nuclear"],                          "smr_advanced_reactors",     "SMRs & Advanced Reactors"),
    ("OCC",   ["ai_networking"],                            "optical_components_lasers", None),
    ("OKLO",  ["uranium_nuclear"],                          "smr_advanced_reactors",     None),
    ("ONTO",  ["semicap_equipment", "substrates_packaging"],"semicap_equip",             None),
    ("OSS",   ["ai_networking"],                            "servers_compute_systems",   "Servers & Compute Systems"),
    ("QS",    ["lithium_battery"],                          "battery_tech_storage",      None),
    ("RDDT",  ["ai_networking"],                            None,                        None),   # keep software
    ("SATS",  ["ai_networking"],                            "satellite_comms",           None),
    ("SILC",  ["photonics_lasers"],                         "networking_fabric_infra",   "Networking & Fabric Infrastructure"),
    ("SMR",   ["uranium_nuclear"],                          "smr_advanced_reactors",     None),
    ("TRT",   ["semicap_equipment", "substrates_packaging"],"test_measurement",          "Test & Measurement"),
    ("TSM",   ["substrates_packaging"],                     None,                        None),   # keep semiconductors
    ("UEC",   ["uranium_nuclear"],                          "uranium_nuclear_fuel",      None),
    ("URG",   ["uranium_nuclear"],                          "uranium_nuclear_fuel",      "Uranium Mining & Nuclear Fuel"),
    # UUUU: keep rare_earth as active primary; add uranium_nuclear_fuel as additional
    ("UUUU",  ["uranium_nuclear"],                          "uranium_nuclear_fuel",      None),
    ("VIAV",  ["ai_networking"],                            "test_measurement",          None),
    ("VSAT",  ["ai_networking"],                            None,                        None),   # keep space
]

DEPRECATED_IDS = frozenset([
    "ai_networking", "semicap_equipment", "lithium_battery", "uranium_nuclear",
    "chemicals_materials", "photonics_lasers", "substrates_packaging", "travel_transportation",
])


def get_db_conn():
    db_url = subprocess.run(
        ["printenv", "NEON_DATABASE_URL"], capture_output=True, text=True
    ).stdout.strip()
    if not db_url:
        raise RuntimeError("NEON_DATABASE_URL not set")
    return psycopg2.connect(db_url)


def pre_flight_audit(conn) -> dict:
    """Read current deprecated membership counts before migration."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM theme_ticker_overrides WHERE theme_id = ANY(%s) AND action='add'",
        (list(DEPRECATED_IDS),)
    )
    override_count = cur.fetchone()[0]

    cur.execute(
        "SELECT COUNT(DISTINCT symbol) FROM theme_ticker_overrides WHERE theme_id = ANY(%s) AND action='add'",
        (list(DEPRECATED_IDS),)
    )
    ticker_count = cur.fetchone()[0]

    depr_cat_keywords = [
        "ai networking", "lithium & battery", "semi equipment",
        "photonics", "substrates", "uranium & nuclear", "chemicals & mat",
        "travel & trans", "[deprecated]"
    ]
    cur.execute("SELECT COUNT(*) FROM watchlist_category_overrides WHERE LOWER(category) SIMILAR TO %s",
                ("%" + "|".join(depr_cat_keywords) + "%",))
    # Use simpler ILIKE approach
    cat_count = 0
    for kw in ["ai networking", "lithium & battery", "semi equipment",
                "photonics / lasers", "photonics & lasers", "substrates / packaging",
                "substrates & packaging", "uranium & nuclear", "chemicals & mat",
                "travel & trans", "[deprecated]"]:
        cur.execute(
            "SELECT COUNT(*) FROM watchlist_category_overrides WHERE LOWER(category) LIKE %s",
            (f"%{kw}%",)
        )
        cat_count += cur.fetchone()[0]

    cur.close()
    return {
        "deprecated_override_rows": override_count,
        "deprecated_tickers": ticker_count,
        "deprecated_category_rows": cat_count,
    }


def run_migration() -> dict:
    """Execute the full migration. Returns a summary dict."""
    from data.pg_storage import atomic_taxonomy_write_db

    results = []
    succeeded = 0
    failed = 0

    for ticker, deprecated_ids, new_theme_id, cat_label in MIGRATIONS:
        # Build override list: tombstone all deprecated memberships
        ticker_overrides = [
            {
                "theme_id": did,
                "symbol": ticker,
                "action": "remove",
                "source": "taxonomy_migration_v3",
                "note": f"Deprecated theme retired by taxonomy_migration_v3.py",
                "created_by": "migration",
            }
            for did in deprecated_ids
        ]

        # Add new active theme if specified
        if new_theme_id:
            ticker_overrides.append({
                "theme_id": new_theme_id,
                "symbol": ticker,
                "action": "add",
                "source": "taxonomy_migration_v3",
                "note": (
                    f"Migrated from deprecated {deprecated_ids} "
                    f"by taxonomy_migration_v3.py"
                ),
                "created_by": "migration",
            })

        # Build primary_operation for category override update
        primary_op = None
        if cat_label:
            primary_op = {
                "action": "set",
                "user_id": "default",
                "ticker": ticker,
                "category": cat_label,
                "source": "taxonomy_migration_v3",
                "reason": (
                    f"Migrated from deprecated category to active '{cat_label}' "
                    f"by taxonomy_migration_v3.py"
                ),
            }

        result = atomic_taxonomy_write_db(
            ticker_overrides=ticker_overrides,
            primary_operation=primary_op,
        )
        results.append({
            "ticker": ticker,
            "deprecated_removed": deprecated_ids,
            "new_theme_added": new_theme_id,
            "category_updated": cat_label,
            "ok": result["ok"],
            "error": result.get("error"),
        })
        if result["ok"]:
            succeeded += 1
            status = "OK"
        else:
            failed += 1
            status = f"FAILED: {result.get('error')}"
        print(f"  [{status}] {ticker}: remove {deprecated_ids}"
              + (f" + add {new_theme_id}" if new_theme_id else "")
              + (f" + cat→'{cat_label}'" if cat_label else ""))

    return {"succeeded": succeeded, "failed": failed, "details": results}


def post_flight_audit(conn) -> dict:
    """Read deprecated membership counts after migration (should be 0)."""
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM theme_ticker_overrides WHERE theme_id = ANY(%s) AND action='add'",
        (list(DEPRECATED_IDS),)
    )
    override_count = cur.fetchone()[0]
    cur.execute(
        "SELECT COUNT(DISTINCT symbol) FROM theme_ticker_overrides WHERE theme_id = ANY(%s) AND action='add'",
        (list(DEPRECATED_IDS),)
    )
    ticker_count = cur.fetchone()[0]
    cur.close()
    return {
        "remaining_deprecated_override_rows": override_count,
        "remaining_deprecated_tickers": ticker_count,
    }


def invalidate_caches():
    """Best-effort cache invalidation after migration."""
    try:
        from services.theme_merge_layer import refresh_enriched_universe
        refresh_enriched_universe()
        print("  [OK] refresh_enriched_universe()")
    except Exception as exc:
        print(f"  [WARN] refresh_enriched_universe: {exc}")
    try:
        from services.theme_rs_service import invalidate_theme_rs_cache
        invalidate_theme_rs_cache()
        print("  [OK] invalidate_theme_rs_cache()")
    except Exception as exc:
        print(f"  [WARN] invalidate_theme_rs_cache: {exc}")
    try:
        from data.options_flow_sectors import invalidate_sectors_cache
        invalidate_sectors_cache()
        print("  [OK] invalidate_sectors_cache()")
    except Exception as exc:
        print(f"  [WARN] invalidate_sectors_cache: {exc}")


def main():
    print("=" * 70)
    print("TAXONOMY V3 DEPRECATED-THEME MIGRATION")
    print("=" * 70)

    conn = get_db_conn()
    print("\n[PRE-FLIGHT AUDIT]")
    pre = pre_flight_audit(conn)
    for k, v in pre.items():
        print(f"  {k}: {v}")
    conn.close()

    if pre["deprecated_override_rows"] == 0:
        print("\nNothing to migrate — all deprecated memberships already removed.")
        print("Running cache invalidation for completeness …")
        invalidate_caches()
        return

    print(f"\n[MIGRATION] Migrating {pre['deprecated_tickers']} tickers …")
    result = run_migration()
    print(f"\n  Succeeded: {result['succeeded']} / {result['succeeded'] + result['failed']}")
    if result["failed"]:
        print(f"  FAILED:    {result['failed']}")
        for d in result["details"]:
            if not d["ok"]:
                print(f"    {d['ticker']}: {d['error']}")

    conn2 = get_db_conn()
    print("\n[POST-FLIGHT AUDIT]")
    post = post_flight_audit(conn2)
    for k, v in post.items():
        print(f"  {k}: {v}")
    conn2.close()

    if post["remaining_deprecated_override_rows"] > 0:
        print("\n  WARNING: some deprecated rows remain — check errors above.")
    else:
        print("\n  All deprecated memberships successfully removed.")

    print("\n[CACHE INVALIDATION]")
    invalidate_caches()

    print("\n[DONE]")


if __name__ == "__main__":
    main()
