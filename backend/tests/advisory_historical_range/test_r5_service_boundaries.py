from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def test_r5_facade_does_not_import_protected_consumers() -> None:
    protected = ("selection_center", "simulation", "paper", "qmt", "qlib", "qe")
    for relative in (
        "backend/services/advisory_historical_range/service.py",
        "backend/services/advisory_historical_range/query_repository.py",
        "backend/services/advisory_historical_range/api_models.py",
    ):
        tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
        imports = "\n".join(
            node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
        )
        assert not any(name in imports.lower() for name in protected)


def test_r5_adds_no_migration_or_scheduler() -> None:
    changed_targets = {
        "api_models.py", "query_repository.py", "service.py", "composition.py", "advisory.py"
    }
    assert not any("migration" in name or "scheduler" in name for name in changed_targets)
