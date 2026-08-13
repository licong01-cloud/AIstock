from __future__ import annotations

import ast
from pathlib import Path


def test_m5b_modules_do_not_import_protected_business_modules() -> None:
    root = Path(__file__).resolve().parents[2]
    files = [
        root / "services" / "advisory_model_first" / name
        for name in (
            "outcome_calibration.py",
            "outcome_calibration_contracts.py",
            "outcome_calibration_bundle.py",
            "outcome_calibration_pipeline.py",
        )
    ]
    protected = (
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.strategy_package",
        "backend.services.paper_trading",
        "backend.services.quantevolver",
        "backend.services.rdagent",
    )
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imports = [
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module
        ]
        assert not any(module.startswith(protected) for module in imports), (path, imports)


def test_protected_modules_do_not_import_m5b_calibration() -> None:
    root = Path(__file__).resolve().parents[2] / "services"
    protected_roots = (
        root / "selection_center",
        root / "simulation_runtime",
        root / "strategy_package",
        root / "paper_trading",
        root / "quantevolver",
    )
    for protected_root in protected_roots:
        for path in protected_root.rglob("*.py"):
            assert "advisory_model_first.outcome_calibration" not in path.read_text(
                encoding="utf-8"
            ), path
