from __future__ import annotations

import ast
from pathlib import Path


def test_m5c_modules_do_not_import_protected_business_modules() -> None:
    root = Path(__file__).resolve().parents[2]
    files = sorted((root / "services" / "advisory_model_first").glob("price_range_calibration*.py"))
    assert files
    protected = (
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.strategy_package",
        "backend.services.paper_trading",
        "backend.services.quantevolver",
        "backend.services.rdagent",
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
        assert not any(module.startswith(protected) for module in imports), (path, imports)


def test_protected_modules_do_not_import_m5c_calibration() -> None:
    root = Path(__file__).resolve().parents[2] / "services"
    for protected_root in (
        root / "selection_center",
        root / "simulation_runtime",
        root / "strategy_package",
        root / "paper_trading",
        root / "quantevolver",
    ):
        for path in protected_root.rglob("*.py"):
            assert "advisory_model_first.price_range_calibration" not in path.read_text(
                encoding="utf-8"
            ), path


def test_v1_runtime_modules_have_no_top_level_m5c_import() -> None:
    root = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first"
    for name in ("price_range_runtime_bundle.py", "price_range_inference.py"):
        path = root / name
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        top_level_imports = [
            node.module
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module
        ]
        assert not any(
            module.startswith("backend.services.advisory_model_first.price_range_calibration")
            for module in top_level_imports
        ), (path, top_level_imports)
