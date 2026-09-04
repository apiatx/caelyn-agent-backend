import ast
from pathlib import Path


MAIN = Path(__file__).resolve().parents[1] / "main.py"


def test_lifespan_does_not_register_provider_heavy_restart_catchup():
    source = MAIN.read_text(encoding="utf-8")
    tree = ast.parse(source)
    lifespan = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "lifespan"
    )
    registrations = [
        ast.unparse(call.args[0])
        for call in ast.walk(lifespan)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "create_task"
        and call.args
    ]
    denied = {
        "_briefing_precompute_loop()",
        "_terminal_prewarm()",
        "_trading_dashboard_startup()",
        "_cal_stale_check(_fmp_key_for_snap or '')",
        "_polygon_options_ingestion_loop()",
        "_macro_precompute_loop()",
        "_strategy_history_precompute_loop()",
        "_master_screener_loop()",
        "_sectors_fast_backfill_loop()",
        "_theme_options_supplement_loop()",
        "_dynamic_thematic_universe_loop()",
        "_theme_rs_warmup_deferred()",
    }
    assert denied.isdisjoint(registrations)
    assert "_loop_fn(skip_initial=True)" in registrations