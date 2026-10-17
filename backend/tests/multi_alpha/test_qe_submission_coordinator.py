from __future__ import annotations

from dataclasses import replace
import asyncio
from typing import Any
from unittest.mock import Mock

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import (
    MAX_RESTART_SAFE_LEASE_SECONDS,
    QEActiveExecutionCapacityService,
    QEExecutionReservationReconciler,
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionCoordinatorError,
    QEWorkspaceSubmissionSource,
    submission_intent_hash_for_source,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceSubmissionInspection
from backend.services.quantevolver.qe_execution_reservation import (
    QEExecutionCapacityObservation,
    QEExecutionReservationAcquireResult,
    QEExecutionReservationRepository,
    QEExecutionReservationSpec,
)
from backend.tests.multi_alpha.test_durable_capacity import (
    ScriptedCursor, ScriptedProvider, Step, _spec, _reservation_row,
)
from backend.services.multi_alpha.durable_execution_adapter import (
    DurableSubmissionIntent,
    QEWorkspacePredBacktestAdapter,
)


@pytest.mark.parametrize("released,remote_status,pid", [(True, "not_reserved", None),
                                                       (False, "not_reserved", None),
                                                       (False, "running", None),
                                                       (False, "unreachable", None),
                                                       (False, "not_reserved", 123)])
def test_reconciler_rechecks_deleted_source_even_when_not_reserved_is_unchanged(
    monkeypatch: pytest.MonkeyPatch, released: bool, remote_status: str, pid: int | None,
) -> None:
    row = _reservation_row(replace(_spec(), source_kind="qe_evolution_loop"),
                           status="reconciling", remote_status="not_reserved")
    repository = Mock()
    repository.release_deleted_evolution_reservation.return_value = row if released else None
    reconciler = QEExecutionReservationReconciler(repository=repository, owner_id="reconciler")

    class Client:
        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *_: Any) -> None:
            pass

        async def inspect_loop_submission(self, task_id: str, loop_id: str, **_: Any) -> Any:
            if remote_status == "unreachable":
                raise ConnectionError("node receipt unavailable")
            return QEWorkspaceSubmissionInspection(
                "qe_submission_receipt_v1", task_id, loop_id, remote_status,
                submission_intent_hash="a" * 64 if remote_status != "not_reserved" else None,
                pid=pid,
            )

    monkeypatch.setattr(
        "backend.services.quantevolver.qe_active_execution_capacity.QEWorkspaceClient.for_node",
        lambda _: Client(),
    )
    notify = Mock()
    monkeypatch.setattr(
        "backend.services.quantevolver.qe_reconciliation_coordinator.notify_qe_reconciliation",
        notify,
    )
    if remote_status == "running":
        row.update(status="running", remote_status="running", submission_intent_hash="a" * 64)
    if remote_status == "unreachable":
        with pytest.raises(ConnectionError):
            asyncio.run(reconciler._reconcile_one(row))
        repository.release_deleted_evolution_reservation.assert_not_called()
        return
    if pid is not None:
        with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as exc:
            asyncio.run(reconciler._reconcile_one(row))
        assert exc.value.reason_code == "qe_workspace_submission_receipt_invalid"
        repository.release_deleted_evolution_reservation.assert_not_called()
        return
    result = asyncio.run(reconciler._reconcile_one(row))
    assert result == ("terminal_released" if released else None)
    assert repository.release_deleted_evolution_reservation.call_count == (
        1 if remote_status == "not_reserved" else 0
    )
    repository.claim_reservation_for_source.assert_not_called()
    repository.transition_execution_reservation.assert_not_called()
    assert notify.call_count == (2 if released else 0)


@pytest.mark.parametrize("eligible,incompatible,capacity", [(1, 0, 4), (1, 1, 1), (0, 0, 1)])
def test_durable_replay_cohort_proof_preserves_global_capacity_and_training_exclusion(
    eligible: int, incompatible: int, capacity: int,
) -> None:
    spec = _spec()
    steps = [Step("AS eligible_count", one={"eligible_count": eligible})]
    if eligible:
        steps.append(Step("AS incompatible_count", one={"incompatible_count": incompatible}))
    cursor = ScriptedCursor(steps)
    result = QEExecutionReservationRepository._effective_node_capacity_for_source(
        cursor, spec, requested_node_capacity=4,
        allow_same_task_backtest_parallelism=False,
        allow_same_task_parallel_training=False,
        allow_durable_prediction_replay_parallelism=True,
    )
    assert result == capacity
    sql, params = cursor.executions[0]
    assert "attempt.submission_intent_hash = %s" in sql
    assert params == (spec.source_execution_id, spec.node_id, spec.qe_task_id,
                      spec.qe_loop_id, spec.submission_intent_hash)
    if eligible:
        cohort_sql, _ = cursor.executions[1]
        assert "attempt.submission_intent_hash = reservation.submission_intent_hash" in cohort_sql
        assert "reservation.qe_task_id = %s" not in cohort_sql  # Independent parent runs can replay.
    assert not cursor.steps


def test_durable_replay_capacity_proof_cannot_be_used_for_unrelated_training_source() -> None:
    spec = QEExecutionReservationSpec(
        node_id="wsl2-5080", source_kind="qe_experiment", source_execution_id="training1",
        qe_task_id="remote_task", qe_loop_id="Loop1", submission_intent_hash="a" * 64,
    )
    cursor = ScriptedCursor([])
    assert QEExecutionReservationRepository._effective_node_capacity_for_source(
        cursor, spec, requested_node_capacity=4, allow_same_task_backtest_parallelism=False,
        allow_same_task_parallel_training=False, allow_durable_prediction_replay_parallelism=True,
    ) == 1
    assert not cursor.executions


@pytest.mark.parametrize("active,accepted", [(3, True), (4, False)])
def test_durable_replay_admission_keeps_node_lock_and_once_only_transaction(
    active: int, accepted: bool,
) -> None:
    spec = _spec(qe_task_id="different_parent_remote_task")
    steps = [
        Step("pg_advisory_xact_lock"), Step("pg_advisory_xact_lock"),
        Step("SELECT node_id FROM infra.compute_nodes", one={"node_id": spec.node_id}),
        Step("AS eligible_count", one={"eligible_count": 1}),
        Step("AS incompatible_count", one={"incompatible_count": 0}),
        Step("WHERE source_kind = %s AND source_execution_id = %s", one=None),
        Step("WHERE node_id = %s AND qe_task_id = %s", one=None),
        Step("SELECT COUNT(*) AS active_count", one={"active_count": active}),
    ]
    if accepted:
        steps.extend([Step("CLAIM_SOURCE", one={"status": "submitting"}),
                      Step("INSERT INTO infra.qe_execution_reservation", one=_reservation_row(spec))])
    else:
        steps.append(Step("WAIT_CAPACITY", one={"status": "queued"}))
    provider = ScriptedProvider(steps)

    def claim(cur: Any) -> Any:
        cur.execute("CLAIM_SOURCE")
        return cur.fetchone()

    def waiting(cur: Any, actual: int, capacity: int) -> Any:
        assert actual == capacity == 4
        cur.execute("WAIT_CAPACITY")
        return cur.fetchone()

    result = QEExecutionReservationRepository(provider).reserve_execution_and_claim_source(
        spec, node_capacity=4, allow_durable_prediction_replay_parallelism=True,
        owner_id="worker_1", lease_seconds=30, claim_source=claim,
        record_waiting_capacity=waiting,
    )
    assert result.acquired is accepted and result.node_capacity == 4
    assert result.active_count == 4
    assert provider.commits == 1 and provider.rollbacks == 0
    assert not provider.cursor.steps


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


@pytest.mark.parametrize("kind,backtest,training,capacity", [
    ("multi_alpha_durable_attempt", True, False, 4),
    ("multi_alpha_durable_attempt", False, False, 1),
    ("qe_experiment", True, False, 1),
    ("qe_evolution_loop", True, False, 4),
    ("qe_evolution_loop", False, True, 2),
])
def test_wsl_submission_uses_execution_mode_not_durable_training_default(
    kind: str, backtest: bool, training: bool, capacity: int,
) -> None:
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.reserve_execution_and_claim_source.return_value = QEExecutionReservationAcquireResult(
        acquired=False, duplicate_replay=False, active_count=capacity,
        node_capacity=capacity, reservation=None, source_claim=None,
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    client = Mock()
    result = asyncio.run(coordinator.submit(
        client=client,
        source=_source(source_kind=kind, backtest_only=backtest,
                       parallel_training_eligible=training, requested_node_capacity=4),
        payload=QEWorkspaceSubmissionPayload("remote_task", 1, {}, {}, "qrun --pred-backtest pred.pkl"),
    ))
    assert result.waiting_capacity and result.node_capacity == capacity
    call = repository.reserve_execution_and_claim_source.call_args.kwargs
    assert call["node_capacity"] == capacity
    assert call["allow_durable_prediction_replay_parallelism"] == (
        kind == "multi_alpha_durable_attempt" and backtest
    )
    client.assert_not_called()


@pytest.mark.parametrize("kind,backtest,capacity", [
    ("multi_alpha_durable_attempt", True, 4),
    ("multi_alpha_durable_attempt", False, 1),
    ("qe_experiment", True, 1),
])
def test_waiting_capacity_probe_preserves_durable_replay_mode(
    kind: str, backtest: bool, capacity: int,
) -> None:
    repository = Mock(spec=QEExecutionReservationRepository)
    repository.observe_execution_capacity.return_value = QEExecutionCapacityObservation(
        available=False, duplicate_replay=False, active_count=4,
        node_capacity=capacity, reservation=None,
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    coordinator.observe_capacity(
        node_id="wsl2-5080", requested_node_capacity=4, source_kind=kind,
        source_execution_id="attempt1", qe_task_id="remote_task", qe_loop_id="Loop1",
        submission_intent_hash="a" * 64, backtest_only=backtest,
    )
    assert repository.observe_execution_capacity.call_args.kwargs["node_capacity"] == capacity


def test_durable_adapter_capacity_observation_marks_prediction_replay(tmp_path: Any) -> None:
    coordinator = Mock(spec=QEWorkspaceSubmissionCoordinator)
    adapter = QEWorkspacePredBacktestAdapter(
        repository=Mock(), model_store=Mock(), panel_builder=Mock(),
        workspace_root=tmp_path, submission_coordinator=coordinator,
    )
    intent = DurableSubmissionIntent("parent", "child", "attempt", 1, "wsl2-5080",
                                     "remote_task", "Loop1", "a" * 64)
    adapter.observe_submission_capacity(run={"node_parallelism_json": {"wsl2-5080": 4}},
                                        intent=intent)
    arguments = coordinator.observe_capacity.call_args.kwargs
    assert arguments["backtest_only"] is True
    assert arguments["requested_node_capacity"] == 4
    assert arguments["source_execution_id"] == intent.attempt_id
