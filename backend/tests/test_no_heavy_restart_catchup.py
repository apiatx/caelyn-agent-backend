from pathlib import Path


BACKEND = Path(__file__).resolve().parents[1]


def test_canonical_recurring_schedulers_remain_registered():
    source = (BACKEND / "main.py").read_text()
    required = [
        "asyncio.create_task(_itype_classify_loop())",
        "asyncio.create_task(_master_screener_loop(skip_initial=True))",
        "asyncio.create_task(_sectors_fast_backfill_loop(skip_initial=True))",
        "asyncio.create_task(_theme_options_supplement_loop(skip_initial=True))",
        "asyncio.create_task(_polygon_options_ingestion_loop(skip_initial=True))",
        "asyncio.create_task(_macro_precompute_loop(skip_initial=True))",
        "asyncio.create_task(_strategy_history_precompute_loop(skip_initial=True))",
        "asyncio.create_task(_hl_boot_and_run(_hl_state))",
        "asyncio.create_task(_bittensor_deferred())",
        "asyncio.create_task(_loop_fn(skip_initial=True))",
        "asyncio.create_task(_x_consensus_loop(skip_initial_catchup=True))",
        "asyncio.create_task(_dynamic_thematic_universe_loop(skip_initial=True))",
        "asyncio.create_task(_theme_rs_warmup_deferred())",
        "asyncio.create_task(_warmup_fn(skip_initial=True))",
        "asyncio.create_task(_em_tick())",
        "asyncio.create_task(_rss_sweeper_deferred())",
    ]
    for registration in required:
        assert registration in source


def test_explicit_restart_catchups_are_not_registered():
    source = (BACKEND / "main.py").read_text()
    assert "asyncio.create_task(_em_catchup())" not in source
    assert "asyncio.create_task(_cal_stale_check(" not in source
    assert "asyncio.create_task(_terminal_prewarm())" not in source
    assert "asyncio.create_task(_trading_dashboard_startup())" not in source


def test_master_options_uses_normal_idle_and_offloads_sync_work():
    source = (BACKEND / "main.py").read_text()
    start = source.index("async def _master_screener_loop(skip_initial: bool = False):")
    end = source.index("async def _sectors_fast_backfill_loop(", start)
    loop = source[start:end]
    assert "_MASTER_CYCLE_SLEEP = 60" in loop
    assert "await asyncio.to_thread(" in loop
    assert "_import_master_screener_dependencies" in loop
    assert "_enrich_master_rows" in loop
    assert "_save_master_prefilter_to_disk" in loop
    assert "_save_master_lkg_to_disk" in loop


def test_web_process_requests_restart_safe_scheduler_behavior():
    source = (BACKEND / "main.py").read_text()
    assert "_master_screener_loop(skip_initial=True)" in source
    assert "_strategy_history_precompute_loop(skip_initial=True)" in source
    assert "_dynamic_thematic_universe_loop(skip_initial=True)" in source
    assert "_x_consensus_loop(skip_initial_catchup=True)" in source
    assert "_loop_fn(skip_initial=True)" in source
    assert "_warmup_fn(skip_initial=True)" in source
    assert "_briefing_precompute_loop(skip_initial=True)" in source
    assert "_edgar_cache_loop(skip_initial=True)" in source
    assert "_sectors_fast_backfill_loop(skip_initial=True)" in source
    assert "_theme_options_supplement_loop(skip_initial=True)" in source
    assert "_polygon_options_ingestion_loop(skip_initial=True)" in source
    assert "_macro_precompute_loop(skip_initial=True)" in source
    assert "_ei_materials_loop(skip_initial=True)" in source


def test_stage2_startup_loads_lkg_without_full_universe_warmup():
    source = (BACKEND / "main.py").read_text()
    bootstrap_start = source.index("async def _post_yield_bootstrap():")
    bootstrap_end = source.index("asyncio.create_task(_post_yield_bootstrap())", bootstrap_start)
    bootstrap = source[bootstrap_start:bootstrap_end]

    assert "load_lkg as _wl_stage2_load" in bootstrap
    assert "await asyncio.to_thread(_wl_stage2_load)" in bootstrap
    assert "warmup_stage2_all_watchlists" not in bootstrap
    assert "_wl_stage2_warmup" not in bootstrap

    assert '@app.post("/api/admin/stage2/force-warmup")' in source
    assert "force_warmup_stage2_nulls as _force_nulls" in source

    scheduler = (BACKEND / "services" / "screener_hub_scheduler.py").read_text()
    assert "async def _run_watchlist_stage2_warm()" in scheduler
    assert '(_not_saturday, 3, 30, "watchlist_stage2_warm"' in scheduler
    assert "result = await warmup_stage2_all_watchlists()" in scheduler
