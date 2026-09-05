from pathlib import Path


BACKEND = Path(__file__).resolve().parents[1]


def _function_source(source: str, start_marker: str, end_marker: str) -> str:
    start = source.index(start_marker)
    end = source.index(end_marker, start)
    return source[start:end]


def test_main_loop_defaults_preserve_historical_behavior():
    source = (BACKEND / "main.py").read_text()
    assert "async def _x_consensus_loop(skip_initial_catchup: bool = False):" in source
    assert "async def _dynamic_thematic_universe_loop(skip_initial: bool = False):" in source
    assert "async def _master_screener_loop(skip_initial: bool = False):" in source
    assert "async def _strategy_history_precompute_loop(skip_initial: bool = False):" in source


def test_dynamic_universe_skip_uses_existing_interval_before_first_build():
    source = (BACKEND / "main.py").read_text()
    loop = _function_source(
        source,
        "async def _dynamic_thematic_universe_loop(",
        "async def _master_screener_loop(",
    )
    assert loop.index("if skip_initial:") < loop.index("await asyncio.sleep(15 * 60)")
    assert loop.index("await asyncio.sleep(15 * 60)") < loop.index("force_refresh=True")
    assert "else:\n        await asyncio.sleep(30)" in loop
    assert loop.count("await asyncio.sleep(15 * 60)") == 2


def test_master_and_strategy_skip_before_first_heavy_pass():
    source = (BACKEND / "main.py").read_text()
    master = _function_source(
        source,
        "async def _master_screener_loop(",
        "async def _sectors_fast_backfill_loop(",
    )
    assert master.index("if skip_initial:") < master.index("while True:")
    assert master.index("await asyncio.sleep(_initial_sleep)") < master.index("engine = UnifiedOptionsEngine")
    assert "_MASTER_CYCLE_SLEEP = 60" in master
    assert "_MASTER_OFFHOURS_SLEEP = 1200" in master

    strategy = _function_source(
        source,
        "async def _strategy_history_precompute_loop(",
        "async def _sector_rotation_precompute_loop(",
    )
    assert strategy.index("if skip_initial:") < strategy.index("while True:")
    assert strategy.index("await asyncio.sleep(10800)") < strategy.index("precompute_strategy_history")
    assert strategy.count("await asyncio.sleep(10800)") == 2


def test_x_consensus_suppresses_only_startup_catchup():
    source = (BACKEND / "main.py").read_text()
    loop = _function_source(
        source,
        "async def _x_consensus_loop(",
        "async def _terminal_prewarm(",
    )
    assert "if skip_initial_catchup:" in loop
    assert "await _run_refresh(data_service)" in loop
    assert "Next refresh:" in loop
    assert "Saturday — skipping refresh" in loop
    assert "canonical cache still fresh" in loop


def test_theme_rs_loads_lkg_and_schedules_restart_safe_loop():
    source = (BACKEND / "services" / "theme_rs_service.py").read_text()
    warmup = _function_source(
        source,
        "async def warmup_theme_rs(",
        "def _apply_classification_filter(",
    )
    assert "skip_initial: bool = False" in warmup
    assert warmup.index("lkg = _load_lkg()") < warmup.index("_load_refresh_ts()")
    assert warmup.index("_load_refresh_ts()") < warmup.index(
        "asyncio.create_task(_warmup_loop(skip_initial=skip_initial))"
    )

    loop = _function_source(
        source,
        "async def _warmup_loop(",
        "async def warmup_theme_rs(",
    )
    assert "skip_initial: bool = False" in loop
    assert "startup_not_before[\"1D\"] = started_at + _TTL_1D_MARKET" in loop
    assert 'startup_not_before[tf] = started_at + _HIST_FETCH_CADENCE' in loop
    assert "for tf in list(_TIMEFRAME_BARS.keys()):\n            await _locked_refresh(tf)" in loop


def test_bittensor_skip_waits_normal_interval_and_default_keeps_boot_delay():
    source = (BACKEND / "services" / "bittensor" / "router.py").read_text()
    loop = _function_source(
        source,
        "async def _dashboard_refresh_loop(",
        "def start_dashboard_refresh_task(",
    )
    assert "skip_initial: bool = False" in loop
    assert loop.index("await asyncio.sleep(_DASHBOARD_REFRESH_INTERVAL)") < loop.index(
        "data = await _fetch_dashboard_data()"
    )
    assert "else:\n        await asyncio.sleep(10)" in loop
    assert loop.count("await asyncio.sleep(_DASHBOARD_REFRESH_INTERVAL)") == 2