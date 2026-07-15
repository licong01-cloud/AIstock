from __future__ import annotations

import ast
from pathlib import Path


PHASE1G_MODULES = (
    Path("backend/services/advisory_phase1/phase1g_contract.py"),
    Path("backend/services/advisory_phase1/phase1g_artifact_ref.py"),
    Path("backend/services/advisory_phase1/phase1g_schema_guard.py"),
    Path("backend/services/advisory_phase1/phase1g_result_store.py"),
)

FORBIDDEN_IMPORT_PREFIXES = (
    "backend.db.pg_pool",
    "backend.services.advisory_phase1.release_schema_apply_postgres",
    "backend.services.selection_center",
    "backend.services.simulation_runtime",
    "backend.services.paper_trading",
    "backend.services.strategy_package",
    "backend.services.inference_engine",
    "backend.services.quantevolver",
)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            values.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            values.add(node.module)
    return values


def test_phase1g_g1_has_no_shared_runtime_writer_or_apply_imports() -> None:
    for path in PHASE1G_MODULES:
        imports = _imports(path)
        violations = sorted(
            imported
            for imported in imports
            if any(imported == prefix or imported.startswith(f"{prefix}.") for prefix in FORBIDDEN_IMPORT_PREFIXES)
        )
        assert violations == [], f"{path} imports forbidden runtime dependencies: {violations}"
