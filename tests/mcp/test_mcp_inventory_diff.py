from __future__ import annotations

import ast
import re
from pathlib import Path

from backend.mcp.tool_manifest import MODULE_TOOL_NAMES


def _tool_names_from_standalone_script(path: str) -> set[str]:
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    return set(re.findall(r"@mcp\.tool\(\)\s*\ndef\s+(\w+)\(", text))


def _tool_names_from_module_constant(path: str) -> set[str]:
    tree = ast.parse(Path(path).read_text(encoding="utf-8-sig"))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "TOOL_NAMES" and isinstance(node.value, (ast.Tuple, ast.List)):
                    return {elt.value for elt in node.value.elts if isinstance(elt, ast.Constant)}
    raise AssertionError(f"TOOL_NAMES not found in {path}")


def test_legacy_validation_inventory_migrated_to_gateway_module() -> None:
    legacy = _tool_names_from_standalone_script("scripts/aistock_mcp_server.py")
    migrated = _tool_names_from_module_constant("backend/mcp/modules/validation.py")
    assert len(legacy) == 19
    assert legacy == migrated == set(MODULE_TOOL_NAMES["validation"])


def test_legacy_qe_inventory_migrated_to_gateway_modules() -> None:
    qe_experiment = _tool_names_from_standalone_script("scripts/aistock_qe_experiment_mcp_server.py")
    qe_archive = _tool_names_from_standalone_script("scripts/aistock_qe_archive_mcp_server.py")
    assert len(qe_experiment) == 26
    assert len(qe_archive) == 28
    assert qe_experiment == set(MODULE_TOOL_NAMES["qe_experiment"])
    assert qe_archive == set(MODULE_TOOL_NAMES["qe_archive"])


def test_mcp_modules_do_not_import_backend_services_directly() -> None:
    offenders: list[str] = []
    for path in Path("backend/mcp/modules").glob("*.py"):
        if path.name == "__init__.py":
            continue
        text = path.read_text(encoding="utf-8-sig")
        if "backend.services" in text or "backend.db" in text:
            offenders.append(path.as_posix())
    assert offenders == []
