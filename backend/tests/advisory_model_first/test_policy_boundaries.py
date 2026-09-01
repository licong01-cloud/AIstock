from __future__ import annotations

import ast
from pathlib import Path


def test_policy_dataset_modules_do_not_import_runtime_or_database_boundaries() -> None:
    root = Path(__file__).resolve().parents[2] / "services" / "advisory_model_first"
    modules = [
        "policy_contracts.py",
        "policy_rank_source.py",
        "policy_episode_labels.py",
        "shadow_portfolio_policy.py",
        "policy_cpcv.py",
        "policy_dataset_bundle.py",
        "policy_dataset_pipeline.py",
    ]
    forbidden = (
        "backend.db",
        "backend.services.advisory_historical_range",
        "backend.services.paper_trading",
        "backend.services.simulation_runtime",
        "backend.services.selection_center",
    )
    for name in modules:
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.append(node.module)
        assert not [value for value in imports if value.startswith(forbidden)], name
