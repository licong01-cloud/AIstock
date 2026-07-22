import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
R3_FILES = (
    ROOT / "backend/services/advisory_historical_range/executor.py",
    ROOT / "backend/services/advisory_historical_range/list_transition.py",
    ROOT / "backend/services/advisory_historical_range/decision_mark_provider.py",
    ROOT / "backend/services/advisory_historical_range/composition.py",
)
FORBIDDEN_IMPORTS = (
    "backend.services.paper_trading",
    "backend.services.simulation_runtime",
    "backend.services.selection_center",
    "backend.services.quantevolver",
    "backend.infra.qmt",
)


def test_r3_execution_path_has_no_protected_runtime_dependency() -> None:
    imported: set[str] = set()
    for path in R3_FILES:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
    assert not {
        name
        for name in imported
        if any(name == prefix or name.startswith(f"{prefix}.") for prefix in FORBIDDEN_IMPORTS)
    }


def test_r3_has_no_role_approval_or_package_readmission_flow() -> None:
    source = "\n".join(path.read_text(encoding="utf-8").lower() for path in R3_FILES)
    for forbidden in (
        "approval_role",
        "approver",
        "manual_approval",
        "readmit_package",
        "validate_package_assets",
        "paper_order",
        "simulation_order",
    ):
        assert forbidden not in source
    assert "from_environment" not in (ROOT / "backend/services/advisory_historical_range/composition.py").read_text(
        encoding="utf-8"
    )
