from __future__ import annotations

from pathlib import Path

from backend.services.advisory_phase1.source_observer import SourceObserverConfigBundle


def test_source_observer_modules_do_not_import_frozen_runtime_domains() -> None:
    root = Path(__file__).resolve().parents[3]
    modules = [
        root / "backend/services/advisory_phase1/source_observer.py",
        root / "backend/services/advisory_phase1/source_observer_postgres.py",
        root / "backend/services/advisory_phase1/source_capacity.py",
        root / "scripts/advisory_phase1_source_observer.py",
    ]
    forbidden = (
        "backend.services.selection_center",
        "backend.services.paper_trading",
        "backend.services.simulation_runtime",
        "backend.services.miniqmt_execution_runtime",
        "backend.infra.qmt_client",
        "backend.services.broker",
        "backend.services.order",
    )
    source = "\n".join(path.read_text(encoding="utf-8") for path in modules)
    assert all(token not in source for token in forbidden)


def test_observer_config_cannot_carry_sql_or_approval_fields() -> None:
    field_names = set(SourceObserverConfigBundle.model_fields)
    assert "sql" not in field_names
    assert not ({"approved_by", "approval_status", "role", "roles", "authorization", "manual_override"} & field_names)
