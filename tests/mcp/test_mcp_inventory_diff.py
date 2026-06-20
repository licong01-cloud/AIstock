from __future__ import annotations

import ast
import importlib
import re
import sys
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
    assert len(legacy) == 20
    assert legacy == migrated == set(MODULE_TOOL_NAMES["validation"])


def test_legacy_qe_inventory_migrated_to_gateway_modules() -> None:
    qe_experiment = _tool_names_from_standalone_script("scripts/aistock_qe_experiment_mcp_server.py")
    qe_archive = _tool_names_from_standalone_script("scripts/aistock_qe_archive_mcp_server.py")
    assert len(qe_experiment) == 27
    assert len(qe_archive) == 29
    assert qe_experiment < set(MODULE_TOOL_NAMES["qe_experiment"])
    assert set(MODULE_TOOL_NAMES["qe_experiment"]) - qe_experiment == {
        "qe_template_create_and_run_confirmed",
        "qe_single_experiment_create_pending",
        "qe_single_experiment_get_config",
        "qe_single_experiment_update_config_confirmed",
        "qe_custom_evo_create_pending",
        "qe_custom_evo_update_config_confirmed",
    }
    assert qe_archive < set(MODULE_TOOL_NAMES["qe_archive"])
    assert set(MODULE_TOOL_NAMES["qe_archive"]) - qe_archive == {
        "multi_alpha_orthogonality",
        "multi_alpha_combine_preview",
        "multi_alpha_combine_backtest_run",
        "multi_alpha_combine_backtest_result_get",
        "multi_alpha_combine_backtest_list",
        "prediction_store_get_pointer",
        "prediction_store_pull_pred",
        "prediction_store_pull_label",
        "model_store_health",
    }


def test_mcp_modules_do_not_import_backend_services_directly() -> None:
    offenders: list[str] = []
    for path in Path("backend/mcp/modules").glob("*.py"):
        if path.name in {"__init__.py", "_gateway_specs.py"}:
            continue
        text = path.read_text(encoding="utf-8-sig")
        if "backend.services" in text or "backend.db" in text:
            offenders.append(path.as_posix())
    assert offenders == []


def test_mcp_modules_do_not_import_scripts_or_transitive_business_code() -> None:
    offenders: list[str] = []
    module_names: list[str] = []
    for path in Path("backend/mcp/modules").glob("*.py"):
        if path.name in {"__init__.py", "_gateway_specs.py"}:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                if any(alias.name == "scripts" or alias.name.startswith("scripts.") for alias in node.names):
                    offenders.append(path.as_posix())
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module == "scripts" or module.startswith("scripts."):
                    offenders.append(path.as_posix())
        module_names.append(path.stem)

    before = set(sys.modules)
    for module_name in sorted(module_names):
        importlib.import_module(f"backend.mcp.modules.{module_name}")
    added = set(sys.modules) - before

    assert sorted(set(offenders)) == []
    assert "scripts.aistock_mcp_server" not in added
