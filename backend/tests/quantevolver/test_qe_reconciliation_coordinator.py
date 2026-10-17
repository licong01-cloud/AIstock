"""Batch A quiet reconciliation and read-only GET contracts."""

from __future__ import annotations

import inspect

from backend.services.quantevolver import qe_evolution_service
from backend.services.quantevolver import qe_reconciliation_coordinator


def test_reconciliation_minimum_scan_interval_is_sixty_seconds() -> None:
    assert qe_reconciliation_coordinator._MIN_SCAN_INTERVAL_SECONDS >= 60.0


def test_task_detail_get_is_persisted_only() -> None:
    source = inspect.getsource(qe_evolution_service.AutoEvolutionScheduler.get_task_detail)

    assert "get_loop_status(" not in source
    assert "UPDATE qe_evolution_loops" not in source
    assert "UPDATE qe_evolution_tasks" not in source
