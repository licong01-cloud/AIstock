from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
OUTCOME_FILES = tuple(
    sorted((ROOT / "backend/services/advisory_model_first").glob("outcome_*.py"))
)


def test_outcome_source_does_not_import_protected_business_modules() -> None:
    protected = (
        "backend.services.paper_trading",
        "backend.services.selection_center",
        "backend.services.simulation_runtime",
        "backend.services.quantevolver",
        "backend.services.strategy_package",
    )
    combined = "\n".join(path.read_text(encoding="utf-8") for path in OUTCOME_FILES)
    for module in protected:
        assert f"from {module}" not in combined
        assert f"import {module}" not in combined


def test_outcome_source_has_no_role_or_approval_workflow() -> None:
    combined = "\n".join(path.read_text(encoding="utf-8").lower() for path in OUTCOME_FILES)
    for forbidden in ("approval_role", "approver_role", "requires_approval"):
        assert forbidden not in combined
