from __future__ import annotations

import ast
from pathlib import Path


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    return imports


def test_vnpy_style_algo_core_import_boundary_has_no_runtime_or_broker_coupling() -> None:
    allowed_shadow_contract_modules = {
        "backend.services.miniqmt_execution_runtime.deterministic_context",
        "backend.services.miniqmt_execution_runtime.kernel_callback_events",
        "backend.services.miniqmt_execution_runtime.plugin_canonical",
        "backend.services.miniqmt_execution_runtime.plugin_contracts",
        "backend.services.miniqmt_execution_runtime.plugin_registry",
    }
    forbidden_prefixes = (
        "backend.db",
        "backend.infra",
        "backend.routers",
        "backend.services",
        "fastapi",
        "vnpy",
        "xtquant",
    )
    for path in Path("backend/execution_algos/vnpy_style").glob("*.py"):
        if path.name in {"legacy_adapter.py", "plugin_manifests.py"}:
            continue
        for imported in _imports(path):
            if imported.startswith("backend.services"):
                assert imported in allowed_shadow_contract_modules, f"{path} imports non-contract service {imported}"
                continue
            assert not imported.startswith(forbidden_prefixes), f"{path} imports forbidden runtime token {imported}"


def test_plugin_manifest_imports_only_shadow_contract_services() -> None:
    path = Path("backend/execution_algos/vnpy_style/plugin_manifests.py")
    allowed_contract_modules = {
        "backend.services.miniqmt_execution_runtime.plugin_canonical",
        "backend.services.miniqmt_execution_runtime.plugin_contracts",
        "backend.services.miniqmt_execution_runtime.plugin_registry",
    }
    forbidden_prefixes = ("backend.db", "backend.infra", "backend.routers", "fastapi", "vnpy", "xtquant")
    for imported in _imports(path):
        assert not imported.startswith(forbidden_prefixes), f"{path} imports forbidden runtime token {imported}"
        if imported.startswith("backend.services"):
            assert imported in allowed_contract_modules, f"{path} imports non-contract service {imported}"
