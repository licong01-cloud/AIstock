from __future__ import annotations

import ast
from pathlib import Path


def test_daily_price_envelope_modules_do_not_import_cross_module_runtime_or_database() -> None:
    root = Path("backend/services/advisory_model_first")
    files = (
        root / "daily_price_envelope_contracts.py",
        root / "price_range_contracts.py",
        root / "price_range_inference.py",
    )
    forbidden = (
        "backend.services.quantevolver",
        "backend.services.selection_center",
        "backend.services.strategy_package",
        "backend.services.paper_trading",
        "backend.services.simulation_runtime",
        "backend.db",
    )
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not any(name.startswith(forbidden) for name in imports), (path, imports)
