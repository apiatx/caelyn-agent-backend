"""
Post-Migration Quality Audit — Theme Assignment Corrections v1
==============================================================
Idempotent script.  Corrects proven primary/additional mismatches found in the
post-migration quality audit of the 40 migrated tickers.

Corrections:
  BAND  — Remove erroneous memory_storage membership; add cloud_software; set primary.
  ASPI  — Update category override from deprecated display name to current one.
  IMSR  — Update category override from deprecated display name to correct one.
  UUUU  — Set canonical primary to rare_earth (resolver was returning nuclear_energy).
  AMAT  — Set canonical primary to semicap_equip (resolver was returning semiconductors).
  OKLO  — Set canonical primary to smr_advanced_reactors (resolver: nuclear_energy).
  SMR   — Set canonical primary to smr_advanced_reactors (resolver: nuclear_energy).
  LEU   — Set canonical primary to uranium_nuclear_fuel (resolver: nuclear_energy).
  VIAV  — Set canonical primary to test_measurement (resolver: photonics_optical).
  MXL   — Set canonical primary to dc_connectivity_silicon (resolver: semiconductors).
  TSM   — Add packaging_substrates as intentional additional exposure.

All writes use atomic_taxonomy_write_db (canonical primitive).
"""
from __future__ import annotations
import sys, os

_BACKEND = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BACKEND not in sys.path:
    sys.path.insert(0, _BACKEND)

SOURCE = "taxonomy_quality_audit_v1"

# Each entry:
#   (ticker, theme_overrides[], primary_category_or_None)
# theme_overrides: list of {"theme_id", "action"}
# primary_category: display_name string or None (no primary_operation needed)
CORRECTIONS: list[tuple[str, list[dict], str | None]] = [
    # ── BAND ─────────────────────────────────────────────────────────────────
    # Bandwidth Inc. = CPaaS / cloud communications API platform.
    # memory_storage (from old watchlist_theme_patch) is factually wrong.
    # cloud_software is the correct primary theme.
    (
        "BAND",
        [
            {"theme_id": "memory_storage", "action": "remove"},
            {"theme_id": "cloud_software",  "action": "add"},
        ],
        "Cloud Software",
    ),

    # ── ASPI ─────────────────────────────────────────────────────────────────
    # ASP Isotopes = laser isotope enrichment for enriched uranium/nuclear fuel.
    # Cat override was "Uranium & Nuclear Energy" (deprecated display name) →
    # resolver returned that stale label.  Correct to active display name.
    (
        "ASPI",
        [],
        "Uranium Mining & Nuclear Fuel",
    ),

    # ── IMSR ─────────────────────────────────────────────────────────────────
    # Terrestrial Energy = integral MSR reactor designer.
    # Cat override was "Uranium & Nuclear Energy" (deprecated display name) →
    # resolver returned name/id mismatch.  Correct to active display name.
    (
        "IMSR",
        [],
        "SMRs & Advanced Reactors",
    ),

    # ── UUUU ─────────────────────────────────────────────────────────────────
    # Energy Fuels = uranium mining + rare earth processing.
    # Migration intent: rare_earth primary, uranium_nuclear_fuel additional.
    # Resolver returned nuclear_energy (mapper/static-universe order wins).
    # Set explicit primary_operation to lock in rare_earth as canonical primary.
    (
        "UUUU",
        [],
        "Rare Earth Elements",
    ),

    # ── AMAT ─────────────────────────────────────────────────────────────────
    # Applied Materials = semiconductor equipment manufacturer.
    # Migration added semicap_equip, but static universe lists AMAT under
    # semiconductors first → resolver returned semiconductors.
    (
        "AMAT",
        [],
        "Semiconductor Equipment",
    ),

    # ── OKLO ─────────────────────────────────────────────────────────────────
    # Oklo = nuclear microreactor developer.
    # Migration added smr_advanced_reactors, but nuclear_energy wins in mapper.
    (
        "OKLO",
        [],
        "SMRs & Advanced Reactors",
    ),

    # ── SMR ──────────────────────────────────────────────────────────────────
    # NuScale Power = small modular reactor company.
    # Same pattern as OKLO.
    (
        "SMR",
        [],
        "SMRs & Advanced Reactors",
    ),

    # ── LEU ──────────────────────────────────────────────────────────────────
    # Centrus Energy = uranium enrichment services.
    # Migration added uranium_nuclear_fuel; resolver returned nuclear_energy.
    (
        "LEU",
        [],
        "Uranium Mining & Nuclear Fuel",
    ),

    # ── VIAV ─────────────────────────────────────────────────────────────────
    # Viavi Solutions = test & measurement equipment for optical/fiber networks.
    # Migration added test_measurement; resolver returned photonics_optical
    # (from static universe where VIAV pre-existed under photonics_optical).
    (
        "VIAV",
        [],
        "Test & Measurement",
    ),

    # ── MXL ──────────────────────────────────────────────────────────────────
    # MaxLinear = Ethernet/networking connectivity silicon.
    # Migration added dc_connectivity_silicon; LLM override + static universe
    # caused resolver to return semiconductors instead.
    (
        "MXL",
        [],
        "Data Center Connectivity & Interconnect Silicon",
    ),

    # ── TSM ──────────────────────────────────────────────────────────────────
    # Taiwan Semiconductor = leading foundry + advanced packaging (CoWoS/SoIC).
    # Advanced packaging is meaningful thematic exposure: TSM's CoWoS is now
    # the critical supply-chain node for HBM/GPU integration.
    # Primary (semiconductors) is correct; packaging_substrates is intentional
    # additional.
    (
        "TSM",
        [
            {"theme_id": "packaging_substrates", "action": "add"},
        ],
        None,
    ),
]


def run_corrections() -> dict:
    from data.pg_storage import atomic_taxonomy_write_db

    results = []
    succeeded = 0
    failed = 0

    for ticker, membership_ops, cat_label in CORRECTIONS:
        ticker_overrides = [
            {
                "theme_id": op["theme_id"],
                "symbol": ticker,
                "action": op["action"],
                "source": SOURCE,
                "note": f"Quality audit correction by {SOURCE}",
                "created_by": "migration",
            }
            for op in membership_ops
        ]

        primary_op = None
        if cat_label:
            primary_op = {
                "action": "set",
                "user_id": "default",
                "ticker": ticker,
                "category": cat_label,
                "source": SOURCE,
                "reason": f"Primary corrected by post-migration quality audit ({SOURCE})",
            }

        result = atomic_taxonomy_write_db(
            ticker_overrides=ticker_overrides,
            primary_operation=primary_op,
        )
        results.append({
            "ticker": ticker,
            "membership_ops": membership_ops,
            "category_set": cat_label,
            "ok": result["ok"],
            "error": result.get("error"),
        })
        if result["ok"]:
            succeeded += 1
            status = "OK"
        else:
            failed += 1
            status = f"FAILED: {result.get('error')}"

        msg_parts = [f"  [{status}] {ticker}:"]
        for op in membership_ops:
            msg_parts.append(f"{op['action']}={op['theme_id']}")
        if cat_label:
            msg_parts.append(f"primary→'{cat_label}'")
        print(" ".join(msg_parts))

    return {"succeeded": succeeded, "failed": failed, "details": results}


def post_validation():
    """Quick sanity check after corrections."""
    import subprocess, psycopg2
    db_url = subprocess.run(
        ["printenv", "NEON_DATABASE_URL"], capture_output=True, text=True
    ).stdout.strip()
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    DEPRECATED_IDS = [
        "ai_networking", "semicap_equipment", "lithium_battery", "uranium_nuclear",
        "chemicals_materials", "photonics_lasers", "substrates_packaging", "travel_transportation",
    ]
    cur.execute(
        "SELECT COUNT(*) FROM theme_ticker_overrides WHERE theme_id = ANY(%s) AND action='add'",
        (DEPRECATED_IDS,),
    )
    dep_count = cur.fetchone()[0]

    cur.execute("SELECT theme_id, action FROM theme_ticker_overrides WHERE symbol='BAND' AND action='add'")
    band_rows = cur.fetchall()

    cur.execute("SELECT category FROM watchlist_category_overrides WHERE ticker='BAND' AND user_id='default'")
    band_cat = cur.fetchone()

    cur.execute("SELECT category FROM watchlist_category_overrides WHERE ticker='ASPI' AND user_id='default'")
    aspi_cat = cur.fetchone()

    cur.execute("SELECT category FROM watchlist_category_overrides WHERE ticker='IMSR' AND user_id='default'")
    imsr_cat = cur.fetchone()

    cur.execute("SELECT category FROM watchlist_category_overrides WHERE ticker='UUUU' AND user_id='default'")
    uuuu_cat = cur.fetchone()

    cur.execute("SELECT theme_id, action FROM theme_ticker_overrides WHERE symbol='TSM' AND action='add'")
    tsm_rows = cur.fetchall()

    conn.close()
    print(f"\n[POST-VALIDATION]")
    print(f"  Active deprecated rows: {dep_count}  (expected 0)")
    print(f"  BAND active memberships: {[r[0] for r in band_rows]}  (expected [cloud_software])")
    print(f"  BAND category: {band_cat[0] if band_cat else None}  (expected Cloud Software)")
    print(f"  ASPI category: {aspi_cat[0] if aspi_cat else None}  (expected Uranium Mining & Nuclear Fuel)")
    print(f"  IMSR category: {imsr_cat[0] if imsr_cat else None}  (expected SMRs & Advanced Reactors)")
    print(f"  UUUU category: {uuuu_cat[0] if uuuu_cat else None}  (expected Rare Earth Elements)")
    print(f"  TSM active memberships: {[r[0] for r in tsm_rows]}  (expected includes packaging_substrates)")


def main():
    print("=" * 70)
    print("POST-MIGRATION THEME ASSIGNMENT CORRECTIONS v1")
    print("=" * 70)

    print(f"\n[CORRECTIONS] {len(CORRECTIONS)} tickers …")
    result = run_corrections()
    print(f"\n  Succeeded: {result['succeeded']} / {result['succeeded'] + result['failed']}")
    if result["failed"]:
        print(f"  FAILED:    {result['failed']}")
        for d in result["details"]:
            if not d["ok"]:
                print(f"    {d['ticker']}: {d['error']}")

    post_validation()
    print("\n[DONE]")


if __name__ == "__main__":
    main()
