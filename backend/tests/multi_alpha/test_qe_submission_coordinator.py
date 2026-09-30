from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import (
    MAX_RESTART_SAFE_LEASE_SECONDS,
    QEActiveExecutionCapacityService,
    QEWorkspaceSubmissionCoordinatorError,
    QEWorkspaceSubmissionSource,
    submission_intent_hash_for_source,
)


@pytest.mark.parametrize(
    ("node_id", "requested", "backtest_only", "parallel_training", "expected"),
    [
        ("wsl2-5080", None, False, False, 1),
        ("WSL2-5080", 8, False, False, 1),
        ("wsl2-5080", None, True, False, 2),
        ("wsl2-5080", 1, True, False, 1),
        ("wsl2-5080", 2, True, False, 2),
        ("wsl2-5080", 3, True, False, 3),
        ("wsl2-5080", 4, True, False, 4),
        ("wsl2-5080", 8, True, False, 4),
        ("wsl2-5080", 8, False, True, 2),
        ("rdagent-node1", None, False, False, 4),
        ("rdagent-node1", 3, False, False, 3),
        ("rdagent-node1", 8, True, False, 4),
    ],
)
def test_node_capacity_contract(
    node_id: str,
    requested: int | None,
    backtest_only: bool,
    parallel_training: bool,
    expected: int,
) -> None:
    service = QEActiveExecutionCapacityService()

    assert service.resolve_node_capacity(
        node_id,
        requested,
        backtest_only=backtest_only,
        parallel_training=parallel_training,
    ) == expected


@pytest.mark.parametrize(
    ("node_id", "requested", "reason_code"),
    [
        ("wsl", None, "qe_execution_capacity_node_alias_noncanonical"),
        ("LOCAL", None, "qe_execution_capacity_node_alias_noncanonical"),
        ("rdagent-node1", 0, "qe_execution_capacity_request_invalid"),
    ],
)
def test_invalid_capacity_requests_fail_closed(
    node_id: str,
    requested: int | None,
    reason_code: str,
) -> None:
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as excinfo:
        QEActiveExecutionCapacityService().resolve_node_capacity(node_id, requested)

    assert excinfo.value.reason_code == reason_code


def test_capacity_mode_is_unambiguous() -> None:
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as excinfo:
        QEActiveExecutionCapacityService().resolve_node_capacity(
            "wsl2-5080",
            2,
            backtest_only=True,
            parallel_training=True,
        )

    assert excinfo.value.reason_code == "qe_execution_capacity_contract_invalid"


def _source(**overrides: Any) -> QEWorkspaceSubmissionSource:
    values: dict[str, Any] = {
        "source_kind": "qe_evolution_loop",
        "source_execution_id": "qe_task_1_Loop1",
        "node_id": "wsl2-5080",
        "submission_intent_hash": "a" * 64,
        "owner_id": "worker_1",
        "claim_source": lambda _cursor: {"status": "running"},
        "record_waiting_capacity": lambda _cursor, _active, _capacity: {
            "status": "pending"
        },
    }
    values.update(overrides)
    return QEWorkspaceSubmissionSource(**values)


def test_submission_lease_is_restart_safe() -> None:
    source = _source()

    assert source.lease_seconds == MAX_RESTART_SAFE_LEASE_SECONDS == 45
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as excinfo:
        replace(source, lease_seconds=MAX_RESTART_SAFE_LEASE_SECONDS + 1)

    assert excinfo.value.reason_code == "qe_execution_reservation_lease_not_restart_safe"


def test_submission_intent_is_deterministic_and_attempt_scoped() -> None:
    fields = {
        "source_kind": "qe_evolution_loop",
        "source_execution_id": "qe_task_1_Loop1",
        "node_id": "wsl2-5080",
        "task_id": "qe_task_1",
        "loop_id": "Loop1",
    }

    first = submission_intent_hash_for_source(**fields)
    assert first == submission_intent_hash_for_source(**fields)
    assert first != submission_intent_hash_for_source(
        **{**fields, "loop_id": "Loop2"}
    )
