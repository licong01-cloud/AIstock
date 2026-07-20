from __future__ import annotations

import asyncio
from typing import Any

import pytest

from backend.routers import quantevolver
from backend.services.quantevolver.qe_experiment_status_scanner import (
    QEExperimentStatusScanner,
)


def test_pending_capacity_experiment_is_resubmitted_without_ui(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanner = QEExperimentStatusScanner()
    monkeypatch.setattr(
        scanner,
        "_load_pending_capacity_experiments",
        lambda: [
            {
                "experiment_id": "exp_single",
                "qe_task_id": "task_single",
                "qe_loop_id": "Loop1",
                "alpha_mode": "single",
            }
        ],
    )
    monkeypatch.setattr(scanner, "_load_running_experiments", lambda: [])
    calls: list[dict[str, Any]] = []

    async def resume_single(
        experiment_id: str,
        node_id: str | None = None,
        *,
        _capacity_resume: bool = False,
    ) -> dict[str, Any]:
        calls.append(
            {
                "experiment_id": experiment_id,
                "node_id": node_id,
                "capacity_resume": _capacity_resume,
            }
        )
        return {"status": "running"}

    monkeypatch.setattr(quantevolver, "_run_experiment_unified", resume_single)

    stats = asyncio.run(scanner.scan_once())

    assert calls == [
        {
            "experiment_id": "exp_single",
            "node_id": None,
            "capacity_resume": True,
        }
    ]
    assert stats["pending_checked"] == 1
    assert stats["capacity_resubmitted"] == 1
    assert stats["errors"] == 0


def test_running_multi_alpha_resumes_pending_node_before_status_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scanner = QEExperimentStatusScanner()
    monkeypatch.setattr(scanner, "_load_pending_capacity_experiments", lambda: [])
    monkeypatch.setattr(
        scanner,
        "_load_running_experiments",
        lambda: [
            {
                "experiment_id": "exp_multi",
                "qe_task_id": "task_multi",
                "qe_loop_id": "Loop1",
                "alpha_mode": "multi",
                "has_pending_capacity_groups": True,
            }
        ],
    )
    call_order: list[str] = []

    async def resume_multi(
        experiment_id: str,
        node_id: str | None = None,
        *,
        _capacity_resume: bool = False,
    ) -> dict[str, Any]:
        assert experiment_id == "exp_multi"
        assert node_id is None
        assert _capacity_resume is True
        call_order.append("resume")
        return {"status": "running"}

    async def status(experiment_id: str) -> dict[str, Any]:
        assert experiment_id == "exp_multi"
        call_order.append("status")
        return {"status": "running"}

    monkeypatch.setattr(quantevolver, "_run_multi_alpha_experiment", resume_multi)
    monkeypatch.setattr(quantevolver, "get_experiment_run_status", status)

    stats = asyncio.run(scanner.scan_once())

    assert call_order == ["resume", "status"]
    assert stats["capacity_resubmitted"] == 1
    assert stats["still_running"] == 1
    assert stats["errors"] == 0
