from __future__ import annotations

import ast
from pathlib import Path


def test_meta_label_modules_do_not_import_database_or_runtime_modules() -> None:
    root = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first"
    forbidden = ("backend.db", "backend.services.advisory_historical_range", "backend.services.paper_trading", "backend.services.simulation_runtime")
    for path in root.glob("meta_label_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        imports = [node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom) and node.module]
        assert not [value for value in imports if value.startswith(forbidden)], path.name
