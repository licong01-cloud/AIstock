from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TARGETS = (
    ROOT / "backend/services/advisory_phase1/readiness_plan.py",
    ROOT / "backend/services/advisory_phase1/readiness_plan_postgres.py",
    ROOT / "backend/services/advisory_phase1/readiness_plan_store.py",
    ROOT / "backend/services/advisory_phase0a/evidence_projection.py",
    ROOT / "backend/services/advisory_phase0a/evidence_projection_postgres.py",
    ROOT / "backend/services/advisory_phase0a/resolvers.py",
    ROOT / "backend/services/advisory_phase0a/historical_research_postgres.py",
    ROOT / "scripts/advisory_phase0a_audit.py",
    ROOT / "scripts/advisory_phase1e_readiness_plan.py",
)
BANNED_PREFIXES = (
    "backend.services.advisory_program",
    "backend.services.selection_center",
    "backend.services.simulation_runtime",
    "backend.services.strategy_package",
    "backend.services.paper_trading",
    "backend.services.quantevolver",
    "backend.services.rdagent",
    "backend.qlib_exporter",
    "backend.infra.qmt",
    "backend.infra.qmt_client",
)


def _imports(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def test_phase1e_and_audit_facade_do_not_import_shared_runtime_modules() -> None:
    violations = {
        str(path.relative_to(ROOT)): [
            module for module in _imports(path) if module.startswith(BANNED_PREFIXES)
        ]
        for path in TARGETS
    }
    assert not {path: modules for path, modules in violations.items() if modules}


def test_phase1e_has_no_runtime_registration_or_ddl_paths() -> None:
    joined = "\n".join(path.read_text(encoding="utf-8") for path in TARGETS)
    forbidden = ("FastAPI(", "@app.", "CREATE TABLE", "ALTER TABLE", "scheduler", "startup_event")
    assert not [item for item in forbidden if item in joined]
