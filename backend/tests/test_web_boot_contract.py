from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
BACKEND = ROOT / "backend"


def _source(relative_path: str) -> str:
    return (ROOT / relative_path).read_text()


def test_web_boot_registers_heavy_recurring_jobs_restart_safe():
    source = _source("backend/main.py")
    registrations = [
        "_briefing_precompute_loop(skip_initial=True)",
        "_edgar_cache_loop(skip_initial=True)",
        "_master_screener_loop(skip_initial=True)",
        "_sectors_fast_backfill_loop(skip_initial=True)",
        "_theme_options_supplement_loop(skip_initial=True)",
        "_polygon_options_ingestion_loop(skip_initial=True)",
        "_macro_precompute_loop(skip_initial=True)",
        "_strategy_history_precompute_loop(skip_initial=True)",
        "_fn(skip_initial=True)",
        "_loop_fn(skip_initial=True)",
        "_x_consensus_loop(skip_initial_catchup=True)",
        "_ei_materials_loop(skip_initial=True)",
        "_insider_bg_loop(skip_initial=True)",
        "_cong_bg_loop(skip_initial=True)",
        "_whale_bg_loop(skip_initial=True)",
        "_earn_precompute_loop(skip_initial=True)",
        "skip_startup_stale_check=True",
        "_dynamic_thematic_universe_loop(skip_initial=True)",
        "_warmup_fn(skip_initial=True)",
        "from services.watchlist_rss_sweeper import rss_sweeper_loop as _rss_sweeper_loop",
    ]
    for registration in registrations:
        assert registration in source


def test_known_heavy_one_shot_warmups_are_not_registered_from_web_boot():
    source = _source("backend/main.py")
    forbidden = [
        "asyncio.create_task(_earnings_calendar_warmup())",
        "asyncio.create_task(_seed_whales_pv())",
        "_start_background_rebuild()",
        "asyncio.create_task(_terminal_prewarm())",
        "asyncio.create_task(_trading_dashboard_startup())",
        "asyncio.create_task(_em_catchup())",
    ]
    for registration in forbidden:
        assert registration not in source


def test_normal_scheduler_cadences_and_persisted_hydration_remain():
    main = _source("backend/main.py")
    canonical = _source("backend/services/canonical_history_backfill.py")
    calendar = _source("backend/services/calendar_snapshot_service.py")
    earnings = _source("backend/services/earnings_clean_service.py")
    congressional = _source("backend/services/congressional_trading_service.py")
    whale = _source("backend/services/whale_watch_service.py")

    assert "await asyncio.to_thread(_d2x_load_lkg)" in main
    assert "await asyncio.to_thread(_canon_preload)" in main
    assert "await asyncio.to_thread(_wl_stage2_load)" in main
    assert "await asyncio.to_thread(_load_opt_snapshot)" in main
    assert "await asyncio.to_thread(_load_earn_snaps)" in main
    assert "await asyncio.to_thread(_neon_rec)" in main

    assert "_CHECK_INTERVAL_S = 1800" in canonical
    assert "await asyncio.sleep(_CHECK_INTERVAL_S)" in canonical
    assert "await asyncio.sleep(3600)" in calendar
    assert "_STARTUP_DELAY_S = 6 * 3600 if skip_initial else 180" in earnings
    assert "await asyncio.sleep(_FETCH_INTERVAL)" in congressional
    assert "await asyncio.sleep(_REFRESH_INTERVAL if skip_initial else 30)" in whale


def test_required_hyperliquid_session_boot_remains_enabled():
    source = _source("backend/main.py")
    assert "asyncio.create_task(_hl_boot_and_run(_hl_state))" in source


def test_ping_is_db_and_provider_independent():
    source = _source("backend/main.py")
    start = source.index('@app.get("/ping")')
    end = source.index('@app.get("/health")', start)
    ping = source[start:end]
    assert "data_service" not in ping
    assert "pg_storage" not in ping
    assert "init_tables" not in ping
    assert "provider" not in ping.lower()


def test_health_is_observational_only():
    source = _source("backend/main.py")
    start = source.index('@app.get("/health")')
    end = source.index("@app.", start + len('@app.get("/health")'))
    health = source[start:end]
    assert "_init_postgres_chat_storage_on_startup" not in health
    assert "init_tables" not in health
    assert "CREATE TABLE" not in health
    assert "refresh_" not in health
    assert "provider" not in health.lower()


def test_agents_md_contains_permanent_boot_contract():
    source = _source("AGENTS.md")
    assert "A process restart is not a data-refresh event." in source
    assert "Persisted state serves startup" in source