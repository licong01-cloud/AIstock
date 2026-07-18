from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PHASE1E_MODULES = (
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_derived_pit.py",
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_input_builder.py",
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_inputs.py",
    ROOT / "backend/services/advisory_dev_input_onboarding/phase1e_source_mapping.py",
)


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names.update(node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))
    return names


def test_phase1e_modules_do_not_import_shared_runtime_or_consumer_domains() -> None:
    forbidden = (
        "backend.services.strategy_package",
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.paper_trading",
        "backend.services.miniqmt_execution_runtime",
        "backend.inference_engine",
        "backend.services.quantevolver",
        "backend.services.rdagent",
        "backend.qlib_exporter",
        "backend.infra.qmt_client",
    )
    for path in PHASE1E_MODULES:
        imports = _imports(path)
        assert not {name for name in imports if name.startswith(forbidden)}


def test_o4_files_contain_no_unrequested_gate_approval_or_production_switch() -> None:
    paths = (
        *PHASE1E_MODULES,
        ROOT / "backend/services/strategy_package/advisory_input_projection.py",
        ROOT / "scripts/advisory_phase1_source_observer.py",
    )
    source = "\n".join(path.read_text(encoding="utf-8").lower() for path in paths)
    forbidden = (
        "approved_by",
        "approval_status",
        "manual_approval",
        "acknowledgement",
        "source_observer_enabled",
        "--target-db",
        "--prod",
        "backup_gate",
        "force_gate",
        "skip_gate",
    )
    assert all(token not in source for token in forbidden)


def test_strategy_package_projection_has_no_io_or_secondary_validation_calls() -> None:
    path = ROOT / "backend/services/strategy_package/advisory_input_projection.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports = _imports(path)
    forbidden_imports = (
        "repository",
        "asset_store",
        "validator",
        "health",
        "live_inference",
        "multi_alpha_live",
        "selection_center",
        "simulation",
        "paper_trading",
        "psycopg",
        "requests",
        "urllib",
    )
    assert not {name for name in imports if any(fragment in name for fragment in forbidden_imports)}

    forbidden_calls = {"open", "connect", "urlopen", "request", "read_text", "read_bytes", "write_text", "write_bytes"}
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }
    called_names.update(
        node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
    )
    assert called_names.isdisjoint(forbidden_calls)
