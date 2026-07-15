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


def _imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            values.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            values.append(node.module)
    return tuple(values)


def _top_level_imports(path: Path) -> tuple[str, ...]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            values.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            values.append(node.module)
    return tuple(values)


def _module_path(root: Path, module: str) -> Path | None:
    relative = Path(*module.split("."))
    module_file = root / relative.with_suffix(".py")
    if module_file.is_file():
        return module_file
    package_file = root / relative / "__init__.py"
    return package_file if package_file.is_file() else None


def _transitive_local_imports(
    root: Path, entrypoints: tuple[Path, ...]
) -> tuple[str, ...]:
    pending = list(entrypoints)
    visited: set[Path] = set()
    imported: set[str] = set()
    while pending:
        path = pending.pop()
        resolved = path.resolve()
        if resolved in visited:
            continue
        visited.add(resolved)
        for name in _top_level_imports(resolved):
            imported.add(name)
            dependency = _module_path(root, name)
            if dependency is not None and dependency.resolve() not in visited:
                pending.append(dependency)
    return tuple(sorted(imported))


def test_g4_service_and_cli_have_no_shared_runtime_or_ddl_imports() -> None:
    root = Path(__file__).resolve().parents[3]
    paths = (
        root / "backend/services/advisory_phase1/phase1g_service.py",
        root / "scripts/advisory_phase1g_capture_observations.py",
    )

    imports = tuple(name for path in paths for name in _imports(path))
    assert not any(
        name == denied or name.startswith(f"{denied}.")
        for name in imports
        for denied in DENIED_PREFIXES
    )


def test_g4_transitive_local_import_closure_has_no_shared_runtime_or_ddl() -> None:
    root = Path(__file__).resolve().parents[3]
    imports = _transitive_local_imports(
        root,
        (
            root / "backend/services/advisory_phase1/phase1g_service.py",
            root / "scripts/advisory_phase1g_capture_observations.py",
        ),
    )

    assert not any(
        name == denied or name.startswith(f"{denied}.")
        for name in imports
        for denied in DENIED_PREFIXES
    )


def test_g4_changed_modules_contain_no_approval_or_runtime_activation_surface() -> None:
    root = Path(__file__).resolve().parents[3]
    text = "\n".join(
        (root / relative).read_text(encoding="utf-8").lower()
        for relative in (
            "backend/services/advisory_phase1/phase1g_service.py",
            "scripts/advisory_phase1g_capture_observations.py",
        )
    )
    for forbidden in (
        "approval_required",
        "approved_by",
        "rbac",
        "authorization_gate",
        "backup_gate",
        "runtime_activated=true",
    ):
        assert forbidden not in text
