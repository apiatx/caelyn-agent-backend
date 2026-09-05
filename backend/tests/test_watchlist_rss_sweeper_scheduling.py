import ast
import inspect
import sys
from pathlib import Path


BACKEND = Path(__file__).resolve().parents[1]
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from services import watchlist_rss_sweeper as sweeper


def _source():
    return (BACKEND / "services" / "watchlist_rss_sweeper.py").read_text()


def test_rss_concurrency_and_idle_bounds_are_explicit():
    source = _source()
    assert "_SWEEP_SEM_SIZE          = 8" in source
    assert "_DB_WRITE_SEM_SIZE       = 4" in source
    assert "_MIN_POST_SWEEP_IDLE_S   = 60" in source
    assert "asyncio.Semaphore(_SWEEP_SEM_SIZE)" in source
    assert "asyncio.Semaphore(_DB_WRITE_SEM_SIZE)" in source
    assert "max(_MIN_POST_SWEEP_IDLE_S, _TARGET_INTERVAL_S - elapsed)" in source


def test_membership_reads_and_archive_writes_are_offloaded():
    sweep = inspect.getsource(sweeper.run_rss_sweep)
    worker = inspect.getsource(sweeper._sweep_ticker)
    assert "await asyncio.to_thread(list_watchlists)" in sweep
    assert "await asyncio.to_thread(load_watchlist, wl_meta.get(\"id\"))" in sweep
    assert "async with db_write_sem:" in worker


def test_skip_initial_defers_first_sweep_to_normal_cadence():
    source = _source()
    tree = ast.parse(source)
    loop_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "rss_sweeper_loop"
    )
    assert [arg.arg for arg in loop_node.args.kwonlyargs] == ["skip_initial"]
    assert loop_node.args.kw_defaults[0].value is False
    loop = inspect.getsource(sweeper.rss_sweeper_loop)
    assert "if not skip_initial:" in loop
    assert "initial_wait = _TARGET_INTERVAL_S" in loop


def test_main_registers_rss_recurring_loop_with_skip_initial():
    source = (BACKEND / "main.py").read_text()
    assert "asyncio.create_task(_loop_fn(skip_initial=True))" in source
