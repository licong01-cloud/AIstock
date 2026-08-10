from __future__ import annotations

import ast
from pathlib import Path


def test_m4a_training_does_not_import_runtime_selection_or_database_modules() -> None:
    root = Path("backend/services/advisory_model_first")
    files = sorted(root.glob("price_range_*.py"))
    assert files
    forbidden = (
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.paper_trading",
        "backend.db",
    )
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(name.startswith(forbidden) for name in imports), (path, imports)
