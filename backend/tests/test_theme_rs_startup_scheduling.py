import ast
from pathlib import Path


THEME_RS_SERVICE = (
    Path(__file__).resolve().parents[1] / "services" / "theme_rs_service.py"
)


def _warmup_loop_node() -> ast.AsyncFunctionDef:
    tree = ast.parse(THEME_RS_SERVICE.read_text())
    for node in tree.body:
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_warmup_loop":
            return node
    raise AssertionError("_warmup_loop not found")


def test_initial_theme_refreshes_are_awaited_not_fanned_out():
    warmup = _warmup_loop_node()

    initial_loop = next(
        node
        for node in warmup.body
        if isinstance(node, ast.For)
        and isinstance(node.iter, ast.Call)
        and isinstance(node.iter.func, ast.Name)
        and node.iter.func.id == "list"
    )

    assert any(isinstance(node, ast.Await) for node in ast.walk(initial_loop))
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "asyncio"
        and node.func.attr == "create_task"
        for node in ast.walk(initial_loop)
    )