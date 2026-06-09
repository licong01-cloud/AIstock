from __future__ import annotations

import ast
from pathlib import Path


def test_vnpy_style_algo_core_import_boundary_has_no_runtime_or_broker_coupling() -> None:
    forbidden_prefixes = (
        "backend.db",
        "backend.infra",
        "backend.routers",
        "backend.services",
        "fastapi",
        "vnpy",
        "xtquant",
    )
    for path in Path("backend/execution_algos/vnpy_style").glob("*.py"):
        if path.name == "legacy_adapter.py":
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        for imported in imports:
            assert not imported.startswith(forbidden_prefixes), f"{path} imports forbidden runtime token {imported}"
