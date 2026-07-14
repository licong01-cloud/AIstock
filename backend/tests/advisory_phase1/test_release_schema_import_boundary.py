"""Static import-boundary checks for the Phase 1F read-only verifier."""

from __future__ import annotations

import ast
from pathlib import Path


_ROOT = Path("backend/services/advisory_phase1")


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            values.update(item.name for item in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            values.add(node.module)
    return values


def _module_path(module: str) -> Path | None:
    if not module.startswith("backend."):
        return None
    base = Path(*module.split("."))
    module_file = base.with_suffix(".py")
    if module_file.is_file():
        return module_file
    package_file = base / "__init__.py"
    return package_file if package_file.is_file() else None


def _transitive_imports(entrypoints: tuple[Path, ...]) -> set[str]:
    pending = list(entrypoints)
    visited: set[Path] = set()
    imports: set[str] = set()
    while pending:
        path = pending.pop()
        resolved = path.resolve()
        if resolved in visited:
            continue
        visited.add(resolved)
        direct = _imports(path)
        imports.update(direct)
        for module in direct:
            module_path = _module_path(module)
            if module_path is not None:
                pending.append(module_path)
    return imports


def test_verifier_never_imports_apply_or_migration_loader() -> None:
    imports = _transitive_imports((_ROOT / "release_schema_verify_postgres.py",))
    forbidden = {
        "backend.services.advisory_phase1.release_schema_apply_postgres",
        "backend.services.advisory_phase1.release_schema_receipt_store",
    }
    assert not imports & forbidden
    source = (_ROOT / "release_schema_verify_postgres.py").read_text(encoding="utf-8")
    assert "_load_frozen_migration" not in source
    assert "cursor.execute(text)" not in source


def test_phase1_package_does_not_reexport_executor() -> None:
    source = (_ROOT / "__init__.py").read_text(encoding="utf-8")
    assert "release_schema_apply_postgres" not in source
    assert "apply_release_schema_plan" not in source


def test_release_modules_do_not_import_shared_runtime_or_trading_paths() -> None:
    files = (
        _ROOT / "release_schema_contract.py",
        _ROOT / "release_schema_verify_postgres.py",
        _ROOT / "release_schema_apply_postgres.py",
        _ROOT / "release_schema_receipt_store.py",
        Path("scripts/advisory_phase1_release_schema.py"),
    )
    forbidden_fragments = (
        "selection_center",
        "strategy_package",
        "simulation_runtime",
        "paper_trading",
        "miniqmt",
        "qmt",
        "quantevolver",
        "rdagent",
        "qlib",
    )
    imports = _transitive_imports(files)
    assert not [value for value in imports for forbidden in forbidden_fragments if forbidden in value]


def test_no_runtime_approval_or_backup_implementation_exists() -> None:
    source = "\n".join(
        path.read_text(encoding="utf-8").lower()
        for path in (
            _ROOT / "release_schema_contract.py",
            _ROOT / "release_schema_verify_postgres.py",
            _ROOT / "release_schema_apply_postgres.py",
            _ROOT / "release_schema_receipt_store.py",
            Path("scripts/advisory_phase1_release_schema.py"),
        )
    )
    assert "approval table" not in source
    assert "rbac" not in source
    assert "pg_dump" not in source
    assert "dr snapshot" not in source


def test_release_cli_has_no_force_skip_or_arbitrary_sql_bypass() -> None:
    source = Path("scripts/advisory_phase1_release_schema.py").read_text(encoding="utf-8")
    for forbidden in ("--force", "--skip", "--ignore-drift", "--sql"):
        assert forbidden not in source
