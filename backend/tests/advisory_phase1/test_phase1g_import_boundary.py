from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


PHASE1G_MODULES = (
    Path("backend/services/advisory_phase1/phase1g_contract.py"),
    Path("backend/services/advisory_phase1/phase1g_artifact_ref.py"),
    Path("backend/services/advisory_phase1/phase1g_schema_guard.py"),
    Path("backend/services/advisory_phase1/phase1g_result_store.py"),
    Path("backend/services/advisory_phase1/phase1g_phase1e_projection.py"),
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


def _local_module_path(module_name: str) -> Path | None:
    path = Path(*module_name.split("."))
    module_file = path.with_suffix(".py")
    if module_file.is_file():
        return module_file
    package_file = path / "__init__.py"
    return package_file if package_file.is_file() else None


def _transitive_imports(paths: tuple[Path, ...]) -> dict[Path, set[str]]:
    pending = list(paths)
    visited: set[Path] = set()
    result: dict[Path, set[str]] = {}
    while pending:
        path = pending.pop()
        if path in visited:
            continue
        visited.add(path)
        imported_modules = _imports(path)
        result[path] = imported_modules
        for imported in imported_modules:
            if not imported.startswith("backend."):
                continue
            local_path = _local_module_path(imported)
            if local_path is not None and local_path not in visited:
                pending.append(local_path)
    return result


def test_phase1g_g1_has_no_shared_runtime_writer_or_apply_imports() -> None:
    for path, imports in _transitive_imports(PHASE1G_MODULES).items():
        violations = sorted(
            imported
            for imported in imports
            if any(imported == prefix or imported.startswith(f"{prefix}.") for prefix in FORBIDDEN_IMPORT_PREFIXES)
        )
        assert violations == [], f"{path} imports forbidden runtime dependencies: {violations}"


def test_phase1g_g1_runtime_import_does_not_load_forbidden_shared_modules() -> None:
    module_names = tuple(".".join(path.with_suffix("").parts) for path in PHASE1G_MODULES)
    script = (
        "import importlib, json, sys; "
        f"modules={module_names!r}; prefixes={FORBIDDEN_IMPORT_PREFIXES!r}; "
        "[importlib.import_module(name) for name in modules]; "
        "violations=sorted(name for name in sys.modules "
        "if any(name == prefix or name.startswith(prefix + '.') for prefix in prefixes)); "
        "print(json.dumps(violations)); "
        "raise SystemExit(1 if violations else 0)"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout or completed.stderr
    assert completed.stdout.strip() == "[]"
