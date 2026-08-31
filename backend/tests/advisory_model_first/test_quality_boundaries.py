from __future__ import annotations

import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
QUALITY_SOURCES = (
    PROJECT_ROOT / "backend/services/advisory_model_first/quality_contracts.py",
    PROJECT_ROOT / "backend/services/advisory_model_first/quality_tournament.py",
    PROJECT_ROOT / "backend/services/advisory_model_first/quality_pipeline.py",
    PROJECT_ROOT / "backend/services/advisory_model_first/quality_bundle.py",
    PROJECT_ROOT / "scripts/advisory_model_quality_prepare_request.py",
    PROJECT_ROOT / "scripts/advisory_model_quality_train_wsl.py",
    PROJECT_ROOT / "scripts/wsl/advisory_model_quality_train.py",
)
FORBIDDEN_IMPORT_PREFIXES = (
    "backend.services.advisory_historical_range",
    "backend.services.advisory_model_first.outcome_",
    "backend.services.advisory_model_first.price_range_",
    "backend.services.paper_trading",
    "backend.services.quantevolver",
    "backend.services.selection_center",
    "backend.services.simulation_runtime",
)


def test_m5a_quality_sources_do_not_import_later_stage_or_protected_runtime_modules() -> None:
    violations = []
    for path in QUALITY_SOURCES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        imported = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
        for module in imported:
            if module.startswith(FORBIDDEN_IMPORT_PREFIXES):
                violations.append((str(path.relative_to(PROJECT_ROOT)), module))
    assert violations == []
