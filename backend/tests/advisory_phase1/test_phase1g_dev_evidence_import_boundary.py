from __future__ import annotations

import ast
from pathlib import Path


DENIED_PREFIXES = (
    "backend.db.pg_pool",
    "backend.services.selection_center",
    "backend.services.strategy_package",
    "backend.services.simulation_runtime",
    "backend.services.paper_trading",
    "backend.infra.qmt",
    "backend.services.quantevolver",
    "backend.services.rdagent",
    "backend.qlib_exporter",
    "rl_execution",
    "backend.services.advisory_phase1.release_schema_apply_postgres",
)

G5_FILES = (
    "backend/services/advisory_phase1/phase1g_command_factory.py",
    "backend/services/advisory_phase1/phase1g_dev_evidence_contract.py",
    "backend/services/advisory_phase1/phase1g_dev_evidence_store.py",
    "backend/services/advisory_phase1/phase1g_dev_inventory.py",
    "backend/services/advisory_phase1/phase1g_dev_rollback.py",
    "backend/services/advisory_phase1/phase1g_dev_evidence_postgres.py",
    "backend/services/advisory_phase1/phase1g_l3_validation_evidence.py",
    "backend/services/advisory_phase1/phase1g_dev_evidence.py",
    "scripts/advisory_phase1g_dev_evidence.py",
)


def _imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            values.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            values.append(node.module)
    return tuple(values)


def test_g5_has_no_shared_runtime_pool_or_ddl_imports() -> None:
    root = Path(__file__).resolve().parents[3]
    imports = tuple(
        name
        for relative in G5_FILES
        for name in _imports(root / relative)
    )
    assert not any(
        name == denied or name.startswith(f"{denied}.")
        for name in imports
        for denied in DENIED_PREFIXES
    )


def test_g4_does_not_import_g5_modules() -> None:
    root = Path(__file__).resolve().parents[3]
    imports = _imports(root / "backend/services/advisory_phase1/phase1g_service.py")
    assert not any("phase1g_dev" in name for name in imports)


def test_g5_has_no_approval_backup_or_production_target_surface() -> None:
    root = Path(__file__).resolve().parents[3]
    text = "\n".join(
        (root / relative).read_text(encoding="utf-8").lower()
        for relative in G5_FILES
    )
    for forbidden in (
        "approval_required",
        "approved_by",
        "authorization_gate",
        "backup_gate",
        "--target-db",
        "targetlabel.production",
        "runtime_activated=true",
    ):
        assert forbidden not in text
