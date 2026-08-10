from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import pytest

from backend.services.quantevolver.qe_active_execution_capacity import (
    QEActiveExecutionCapacityService,
    QEExecutionReservationReconciler,
    QEWorkspaceSubmissionCoordinator,
    QEWorkspaceSubmissionCoordinatorError,
    QEWorkspaceSubmissionPayload,
    QEWorkspaceSubmissionSource,
    canonical_qe_workspace_request_digest,
    set_qe_capacity_queue_only_nodes,
    submission_intent_hash_for_source,
)
from backend.services.quantevolver.qe_execution_reservation import (
    QEExecutionReservationAcquireResult,
    QEExecutionReservationError,
)
from backend.services.quantevolver.qe_workspace_client import QEWorkspaceClient
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceSubmissionInspection,
    QEWorkspaceSubmissionReceipt,
    QEWorkspaceSubmissionRejected,
    QEWorkspaceSubmissionTransportError,
)


class FakeReservationRepository:
    def __init__(
        self,
        *,
        acquired: bool = True,
        duplicate_replay: bool = False,
        owner_id: str = "worker_1",
        active_count: int = 1,
    ) -> None:
        self.acquired = acquired
        self.duplicate_replay = duplicate_replay
        self.owner_id = owner_id
        self.active_count = active_count
        self.preflight_calls = 0
        self.reserve_calls: list[dict[str, Any]] = []
        self.transitions: list[dict[str, Any]] = []
        self.claim_source_calls: list[dict[str, Any]] = []
        self.queue_only_calls: list[dict[str, Any]] = []
        self._row_version = 1

    def preflight_schema(self, *, raise_on_error: bool = False) -> object:
        assert raise_on_error is True
        self.preflight_calls += 1
        return object()

    def reserve_execution_and_claim_source(
        self,
        spec: Any,
        *,
        node_capacity: int,
        owner_id: str,
        lease_seconds: int,
        claim_source: Any,
        record_waiting_capacity: Any,
    ) -> QEExecutionReservationAcquireResult:
        self.reserve_calls.append(
            {
                "spec": spec,
                "node_capacity": node_capacity,
                "owner_id": owner_id,
                "lease_seconds": lease_seconds,
            }
        )
        if not self.acquired:
            assert record_waiting_capacity(object(), self.active_count, node_capacity)
            return QEExecutionReservationAcquireResult(
                acquired=False,
                duplicate_replay=False,
                active_count=self.active_count,
                node_capacity=node_capacity,
                reservation=None,
                source_claim=None,
            )
        source_claim = None if self.duplicate_replay else claim_source(object())
        reservation = {
            "reservation_id": spec.reservation_id,
            "status": "running" if self.duplicate_replay else "reserved",
            "remote_status": "running" if self.duplicate_replay else None,
            "owner_id": self.owner_id,
            "fencing_token": 1,
            "row_version": self._row_version,
        }
        return QEExecutionReservationAcquireResult(
            acquired=True,
            duplicate_replay=self.duplicate_replay,
            active_count=self.active_count,
            node_capacity=node_capacity,
            reservation=reservation,
            source_claim=source_claim,
        )

    def claim_reservation_for_source(
        self,
        *,
        source_kind: str,
        source_execution_id: str,
        owner_id: str,
        lease_seconds: int,
    ) -> dict[str, Any] | None:
        self.claim_source_calls.append(
            {
                "source_kind": source_kind,
                "source_execution_id": source_execution_id,
                "owner_id": owner_id,
                "lease_seconds": lease_seconds,
            }
        )
        self.owner_id = owner_id
        self._row_version += 1
        return {
            "reservation_id": "qer_" + "a" * 64,
            "status": "running",
            "remote_status": "running",
            "owner_id": owner_id,
            "fencing_token": 2,
            "row_version": self._row_version,
        }

    def record_queue_only_wait_if_unreserved(
        self,
        spec: Any,
        *,
        node_capacity: int,
        record_waiting_capacity: Any,
    ) -> QEExecutionReservationAcquireResult:
        self.queue_only_calls.append(
            {"spec": spec, "node_capacity": node_capacity}
        )
        record_waiting_capacity(object(), self.active_count, node_capacity)
        return QEExecutionReservationAcquireResult(
            acquired=False,
            duplicate_replay=False,
            active_count=self.active_count,
            node_capacity=node_capacity,
            reservation=None,
            source_claim=None,
        )

    def transition_execution_reservation(
        self,
        reservation_id: str,
        *,
        token: Any,
        expected_statuses: Any,
        next_status: str,
        remote_status: str | None = None,
        release_reason_code: str | None = None,
    ) -> dict[str, Any]:
        self._row_version += 1
        call = {
            "reservation_id": reservation_id,
            "token": token,
            "expected_statuses": tuple(expected_statuses),
            "next_status": next_status,
            "remote_status": remote_status,
            "release_reason_code": release_reason_code,
        }
        self.transitions.append(call)
        return {
            "reservation_id": reservation_id,
            "status": next_status,
            "remote_status": remote_status,
            "owner_id": token.owner_id,
            "fencing_token": token.fencing_token,
            "row_version": self._row_version,
        }


class FakeWorkspaceClient:
    def __init__(self, payload: QEWorkspaceSubmissionPayload, intent_hash: str) -> None:
        self.payload = payload
        self.intent_hash = intent_hash
        self.submit_calls = 0
        self.inspect_calls = 0
        self.submit_error: Exception | None = None
        self.inspection: QEWorkspaceSubmissionInspection | Exception = QEWorkspaceSubmissionInspection(
            schema_version="qe_submission_receipt_v1",
            task_id=payload.task_id,
            loop_id=payload.loop_id,
            status="not_reserved",
        )

    async def submit_loop(self, *_args: Any, **_kwargs: Any) -> QEWorkspaceSubmissionReceipt:
        self.submit_calls += 1
        if self.submit_error is not None:
            raise self.submit_error
        return QEWorkspaceSubmissionReceipt(
            task_id=self.payload.task_id,
            loop_id=self.payload.loop_id,
            submission_intent_hash=self.intent_hash,
            request_digest=canonical_qe_workspace_request_digest(self.payload),
            receipt_status="reserved",
            duplicate_replay=False,
        )

    async def inspect_loop_submission(
        self,
        _task_id: str,
        _loop_id: str,
        **_kwargs: Any,
    ) -> QEWorkspaceSubmissionInspection:
        self.inspect_calls += 1
        if isinstance(self.inspection, Exception):
            raise self.inspection
        return self.inspection


def _payload() -> QEWorkspaceSubmissionPayload:
    return QEWorkspaceSubmissionPayload(
        task_id="qe_task_1",
        loop_index=1,
        config={"model_id": "lgbm", "factor_list": ["f1"]},
        experiment_files={"conf.yaml": "body"},
        wsl_command="python qrun.py conf.yaml",
    )


def _source(
    payload: QEWorkspaceSubmissionPayload,
    *,
    requested_node_capacity: int | None = None,
) -> tuple[QEWorkspaceSubmissionSource, dict[str, Any]]:
    evidence: dict[str, Any] = {"claimed": 0, "waiting": 0}
    intent_hash = submission_intent_hash_for_source(
        source_kind="qe_evolution_loop",
        source_execution_id="qe_task_1_Loop1",
        node_id="wsl2-5080",
        task_id=payload.task_id,
        loop_id=payload.loop_id,
    )

    def claim_source(_cur: Any) -> Mapping[str, Any]:
        evidence["claimed"] += 1
        return {"status": "running"}

    def waiting(_cur: Any, active_count: int, node_capacity: int) -> Mapping[str, Any]:
        evidence["waiting"] += 1
        evidence["active_count"] = active_count
        evidence["node_capacity"] = node_capacity
        return {"status": "pending"}

    return (
        QEWorkspaceSubmissionSource(
            source_kind="qe_evolution_loop",
            source_execution_id="qe_task_1_Loop1",
            node_id="wsl2-5080",
            submission_intent_hash=intent_hash,
            owner_id="worker_1",
            claim_source=claim_source,
            record_waiting_capacity=waiting,
            requested_node_capacity=requested_node_capacity,
        ),
        evidence,
    )


def test_capacity_contract_is_wsl_one_remote_four_and_request_can_only_lower() -> None:
    service = QEActiveExecutionCapacityService()
    assert service.resolve_node_capacity("wsl2-5080") == 1
    assert service.resolve_node_capacity("wsl2-5080", 1) == 1
    assert service.resolve_node_capacity("wsl2-5080", 8) == 1
    assert service.resolve_node_capacity("rdagent-node1") == 4
    assert service.resolve_node_capacity("rdagent-node1", 3) == 3
    assert service.resolve_node_capacity("rdagent-node1", 8) == 4
    with pytest.raises(QEWorkspaceSubmissionCoordinatorError):
        service.resolve_node_capacity("rdagent-node1", 0)


def test_full_capacity_persists_waiting_and_never_posts() -> None:
    payload = _payload()
    source, evidence = _source(payload)
    repository = FakeReservationRepository(acquired=False, active_count=2)
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "waiting_capacity"
    assert outcome.active_count == 2
    assert outcome.node_capacity == 1
    assert client.submit_calls == 0
    assert evidence == {"claimed": 0, "waiting": 1, "active_count": 2, "node_capacity": 1}


def test_activation_unresolved_node_is_queue_only_without_failing_source() -> None:
    payload = _payload()
    source, evidence = _source(payload)
    repository = FakeReservationRepository(active_count=1)
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)
    set_qe_capacity_queue_only_nodes(
        {
            "wsl2-5080": (
                {
                    "reason_code": "qe_capacity_identity_unresolved",
                    "source_execution_id": "legacy_unknown",
                },
            )
        }
    )
    try:
        outcome = asyncio.run(
            coordinator.submit(client=client, source=source, payload=payload)
        )
    finally:
        set_qe_capacity_queue_only_nodes({})

    assert outcome.waiting_capacity is True
    assert outcome.detail["reason_code"] == "qe_capacity_node_queue_only"  # type: ignore[index]
    assert client.submit_calls == 0
    assert evidence["waiting"] == 1
    assert repository.reserve_calls == []


def test_new_reservation_transitions_before_post_and_validates_receipt_digest() -> None:
    payload = _payload()
    source, evidence = _source(payload)
    repository = FakeReservationRepository()
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "submitted"
    assert outcome.reservation_status == "submitting"
    assert outcome.remote_status == "reserved"
    assert client.submit_calls == 1
    assert evidence["claimed"] == 1
    assert [item["next_status"] for item in repository.transitions] == ["submitting", "submitting"]
    assert repository.transitions[0]["remote_status"] == "post_pending"


def test_existing_reservation_recovers_receipt_without_second_post() -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    repository = FakeReservationRepository(duplicate_replay=True)
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    client.inspection = QEWorkspaceSubmissionInspection(
        schema_version="qe_submission_receipt_v1",
        task_id=payload.task_id,
        loop_id=payload.loop_id,
        status="running",
        submission_intent_hash=source.submission_intent_hash,
        request_digest=canonical_qe_workspace_request_digest(payload),
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "receipt_recovered"
    assert outcome.remote_status == "running"
    assert client.inspect_calls == 1
    assert client.submit_calls == 0


def test_expired_duplicate_reservation_is_reclaimed_before_receipt_reconciliation() -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    repository = FakeReservationRepository(
        duplicate_replay=True,
        owner_id="expired_worker",
    )
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    client.inspection = QEWorkspaceSubmissionInspection(
        schema_version="qe_submission_receipt_v1",
        task_id=payload.task_id,
        loop_id=payload.loop_id,
        status="running",
        submission_intent_hash=source.submission_intent_hash,
        request_digest=canonical_qe_workspace_request_digest(payload),
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "receipt_recovered"
    assert repository.claim_source_calls == [
        {
            "source_kind": source.source_kind,
            "source_execution_id": source.source_execution_id,
            "owner_id": source.owner_id,
            "lease_seconds": source.lease_seconds,
        }
    ]
    assert client.submit_calls == 0


def test_transport_response_loss_recovers_persisted_receipt() -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    repository = FakeReservationRepository()
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    client.submit_error = QEWorkspaceSubmissionTransportError(
        "lost response",
        reason_code="qe_workspace_submission_transport_unknown",
    )
    client.inspection = QEWorkspaceSubmissionInspection(
        schema_version="qe_submission_receipt_v1",
        task_id=payload.task_id,
        loop_id=payload.loop_id,
        status="reserved",
        submission_intent_hash=source.submission_intent_hash,
        request_digest=canonical_qe_workspace_request_digest(payload),
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "receipt_recovered"
    assert outcome.remote_acceptance_unknown is False
    assert client.submit_calls == 1
    assert client.inspect_calls == 1


def test_transport_and_receipt_unavailable_stays_reconciling_and_keeps_slot() -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    repository = FakeReservationRepository()
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    client.submit_error = QEWorkspaceSubmissionTransportError(
        "lost response",
        reason_code="qe_workspace_submission_transport_unknown",
    )
    client.inspection = QEWorkspaceSubmissionTransportError(
        "inspect unavailable",
        reason_code="qe_workspace_submission_inspection_unavailable",
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "reconciling"
    assert outcome.remote_acceptance_unknown is True
    assert outcome.reservation_status == "reconciling"
    assert repository.transitions[-1]["next_status"] == "reconciling"
    assert repository.transitions[-1]["release_reason_code"] is None


def test_authoritative_409_marks_reservation_failed_and_raises() -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    repository = FakeReservationRepository()
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    client.submit_error = QEWorkspaceSubmissionRejected(
        "different hash",
        status_code=409,
        reason_code="qe_workspace_submission_identity_conflict",
    )
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    with pytest.raises(QEWorkspaceSubmissionRejected):
        asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert repository.transitions[-1]["next_status"] == "failed"
    assert (
        repository.transitions[-1]["release_reason_code"]
        == "qe_workspace_submission_identity_conflict"
    )


def test_receipt_request_digest_mismatch_stays_reconciling_after_remote_acceptance() -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    repository = FakeReservationRepository()
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)

    original_submit = client.submit_loop

    async def mismatched(*args: Any, **kwargs: Any) -> QEWorkspaceSubmissionReceipt:
        return replace(await original_submit(*args, **kwargs), request_digest="f" * 64)

    client.submit_loop = mismatched  # type: ignore[method-assign]
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "reconciling"
    assert outcome.remote_acceptance_unknown is True
    assert outcome.detail["reason_code"] == "qe_workspace_accepted_local_persistence_unknown"  # type: ignore[index]
    assert outcome.detail["persistence_error_code"] == (  # type: ignore[index]
        "qe_workspace_submission_request_digest_mismatch"
    )


def test_remote_acceptance_survives_local_receipt_transition_failure() -> None:
    payload = _payload()
    source, _evidence = _source(payload)

    class FailingReceiptRepository(FakeReservationRepository):
        def transition_execution_reservation(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            if kwargs.get("remote_status") == "reserved":
                raise QEExecutionReservationError(
                    "database write failed after remote acceptance",
                    reason_code="qe_execution_reservation_cas_failed",
                )
            return super().transition_execution_reservation(*args, **kwargs)

    repository = FailingReceiptRepository()
    client = FakeWorkspaceClient(payload, source.submission_intent_hash)
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    outcome = asyncio.run(coordinator.submit(client=client, source=source, payload=payload))

    assert outcome.state == "reconciling"
    assert outcome.remote_acceptance_unknown is True
    assert outcome.reservation_status == "submitting"
    assert outcome.detail["persistence_error_code"] == "qe_execution_reservation_cas_failed"  # type: ignore[index]
    assert [item["next_status"] for item in repository.transitions] == ["submitting"]


def test_exact_terminal_receipt_releases_reservation_owned_by_capacity_reconciler() -> None:
    reservation_id = "qer_" + "d" * 64
    reservation = {
        "reservation_id": reservation_id,
        "status": "running",
        "remote_status": "running",
        "owner_id": "qe_submit_capacity_reconciler",
        "fencing_token": 9,
        "row_version": 17,
    }

    class TerminalHandoffRepository(FakeReservationRepository):
        def get_reservation_for_source(self, **_kwargs: Any) -> dict[str, Any]:
            return dict(reservation)

        def claim_reservation_for_source(self, **_kwargs: Any) -> None:
            pytest.fail("an exact terminal receipt must not wait for the reconciler lease")

    repository = TerminalHandoffRepository(owner_id="qe_submit_capacity_reconciler")
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    released = coordinator.record_authoritative_remote_status_for_source(
        source_kind="multi_alpha_durable_attempt",
        source_execution_id="macba_terminal_handoff",
        remote_status="completed",
        owner_id="macb-worker:host:1:worker",
        expected_reservation_id=reservation_id,
    )

    assert released["status"] == "released"
    assert repository.claim_source_calls == []
    transition = repository.transitions[-1]
    assert transition["token"].owner_id == "qe_submit_capacity_reconciler"
    assert transition["token"].fencing_token == 9
    assert transition["token"].row_version == 17
    assert transition["release_reason_code"] == "qe_workspace_remote_completed"


def test_nonterminal_status_cannot_bypass_active_reservation_owner() -> None:
    reservation_id = "qer_" + "e" * 64
    reservation = {
        "reservation_id": reservation_id,
        "status": "running",
        "remote_status": "running",
        "owner_id": "qe_submit_capacity_reconciler",
        "fencing_token": 4,
        "row_version": 8,
    }

    class ActiveOwnerRepository(FakeReservationRepository):
        def get_reservation_for_source(self, **_kwargs: Any) -> dict[str, Any]:
            return dict(reservation)

        def claim_reservation_for_source(self, **kwargs: Any) -> None:
            self.claim_source_calls.append(dict(kwargs))
            return None

    repository = ActiveOwnerRepository(owner_id="qe_submit_capacity_reconciler")
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    with pytest.raises(
        QEWorkspaceSubmissionCoordinatorError,
        match="could not claim the QE execution reservation",
    ) as exc_info:
        coordinator.record_authoritative_remote_status_for_source(
            source_kind="multi_alpha_durable_attempt",
            source_execution_id="macba_nonterminal_owner",
            remote_status="running",
            owner_id="macb-worker:host:1:worker",
            expected_reservation_id=reservation_id,
        )

    assert exc_info.value.reason_code == "qe_execution_reservation_owner_mismatch"
    assert len(repository.claim_source_calls) == 1
    assert repository.transitions == []


def test_terminal_status_without_exact_reservation_id_cannot_bypass_owner() -> None:
    reservation = {
        "reservation_id": "qer_" + "f" * 64,
        "status": "running",
        "remote_status": "running",
        "owner_id": "qe_submit_capacity_reconciler",
        "fencing_token": 2,
        "row_version": 3,
    }

    class MissingIdentityRepository(FakeReservationRepository):
        def get_reservation_for_source(self, **_kwargs: Any) -> dict[str, Any]:
            return dict(reservation)

        def claim_reservation_for_source(self, **kwargs: Any) -> None:
            self.claim_source_calls.append(dict(kwargs))
            return None

    repository = MissingIdentityRepository(owner_id="qe_submit_capacity_reconciler")
    coordinator = QEWorkspaceSubmissionCoordinator(reservation_repository=repository)

    with pytest.raises(QEWorkspaceSubmissionCoordinatorError) as exc_info:
        coordinator.record_authoritative_remote_status_for_source(
            source_kind="multi_alpha_durable_attempt",
            source_execution_id="macba_terminal_without_identity",
            remote_status="completed",
            owner_id="macb-worker:host:1:worker",
        )

    assert exc_info.value.reason_code == "qe_execution_reservation_owner_mismatch"
    assert repository.transitions == []


def test_reconciler_releases_terminal_receipt_and_keeps_capacity_auditable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _payload()
    source, _evidence = _source(payload)
    reservation = {
        "reservation_id": "qer_" + "b" * 64,
        "node_id": "wsl2-5080",
        "source_kind": source.source_kind,
        "source_execution_id": source.source_execution_id,
        "qe_task_id": payload.task_id,
        "qe_loop_id": payload.loop_id,
        "submission_intent_hash": source.submission_intent_hash,
        "status": "running",
        "remote_status": "running",
        "owner_id": source.owner_id,
        "fencing_token": 1,
        "row_version": 1,
        "updated_at": datetime.now(timezone.utc) - timedelta(minutes=1),
    }

    class ReconcileRepository:
        def __init__(self) -> None:
            self.transitions: list[dict[str, Any]] = []

        def preflight_schema(self, *, raise_on_error: bool) -> object:
            assert raise_on_error is True
            return object()

        def list_active_reservations(self) -> list[dict[str, Any]]:
            return [dict(reservation)]

        def heartbeat_execution_reservation(
            self,
            _reservation_id: str,
            *,
            token: Any,
            lease_seconds: int,
        ) -> dict[str, Any]:
            assert lease_seconds > 0
            return {**reservation, "row_version": token.row_version + 1}

        def claim_reservation_for_source(self, **_kwargs: Any) -> None:
            pytest.fail("live owner must not be replaced during reconciliation")

        def transition_execution_reservation(
            self,
            reservation_id: str,
            **kwargs: Any,
        ) -> dict[str, Any]:
            self.transitions.append({"reservation_id": reservation_id, **kwargs})
            return {**reservation, "status": kwargs["next_status"]}

    class InspectClient:
        async def __aenter__(self) -> "InspectClient":
            return self

        async def __aexit__(self, *_args: Any) -> None:
            return None

        async def inspect_loop_submission(
            self,
            task_id: str,
            loop_id: str,
            **_kwargs: Any,
        ) -> QEWorkspaceSubmissionInspection:
            return QEWorkspaceSubmissionInspection(
                schema_version="qe_submission_receipt_v1",
                task_id=task_id,
                loop_id=loop_id,
                status="completed",
                submission_intent_hash=source.submission_intent_hash,
                request_digest="c" * 64,
            )

    repository = ReconcileRepository()
    monkeypatch.setattr(
        QEWorkspaceClient,
        "for_node",
        classmethod(lambda _cls, _node_id: InspectClient()),
    )
    reconciler = QEExecutionReservationReconciler(
        repository=repository,  # type: ignore[arg-type]
        owner_id=source.owner_id,
    )

    stats = asyncio.run(reconciler.scan_once())

    assert stats["terminal_released"] == 1
    assert stats["errors"] == 0
    assert repository.transitions[0]["next_status"] == "released"
    assert repository.transitions[0]["release_reason_code"] == (
        "qe_workspace_remote_completed"
    )
