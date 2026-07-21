from __future__ import annotations

import asyncio
from typing import Any, Mapping

from backend.services.multi_alpha.durable_cancellation import (
    DurableCancellationDeliveryWorker,
)
from backend.services.multi_alpha.durable_models import (
    kill_intent_hash_for,
    process_identity_hash_for,
)
from backend.services.quantevolver.qe_workspace_client import (
    QEWorkspaceSubmissionInspection,
    QEWorkspaceTypedKillReceipt,
)


PROCESS_IDENTITY = {"pid": 43210, "pgid": 43210, "start_time_ticks": 987654321}
SUBMISSION_HASH = "a" * 64
KILL_TARGET_KEY = "b" * 64
COMMAND_ID = "macmd_2b84ea4e40d2d69ca8cc3c71d938ad30"


class _Repository:
    def __init__(self, row: Mapping[str, Any]) -> None:
        self.row = dict(row)
        self.claimed = False
        self.events: list[dict[str, Any]] = []

    def claim_next_cancel_delivery(self, **_kwargs: Any) -> Mapping[str, Any] | None:
        if self.claimed:
            return None
        self.claimed = True
        if self.row["status"] == "pending":
            self.row["status"] = "sending"
        self.row["owner_id"] = "worker"
        self.row["fencing_token"] = int(self.row["fencing_token"]) + 1
        self.row["row_version"] = int(self.row["row_version"]) + 1
        return dict(self.row)

    def mark_run_cancelling_from_delivery(self, delivery_id: str, **kwargs: Any) -> Mapping[str, Any]:
        assert delivery_id == self.row["delivery_id"]
        assert kwargs["token"].row_version == self.row["row_version"]
        self.events.append({"kind": "run_cancelling", **kwargs})
        return {"id": self.row["run_id"], "status": "cancelling"}

    def record_cancel_delivery_evidence(self, delivery_id: str, **kwargs: Any) -> Mapping[str, Any]:
        assert delivery_id == self.row["delivery_id"]
        token = kwargs["token"]
        assert token.row_version == self.row["row_version"]
        assert self.row["status"] in kwargs["expected_statuses"]
        if kwargs.get("persist_kill_intent", True):
            self.row["expected_process_identity_json"] = kwargs.get("expected_process_identity")
            self.row["expected_process_identity_hash"] = kwargs.get("expected_process_identity_hash")
            self.row["kill_intent_generation"] = kwargs.get("kill_intent_generation")
            self.row["kill_intent_hash"] = kwargs.get("kill_intent_hash")
        if kwargs.get("kill_receipt") is not None:
            self.row["kill_receipt_json"] = dict(kwargs["kill_receipt"])
        self.row["remote_status"] = kwargs.get("remote_status")
        self.row["row_version"] += 1
        self.events.append({"kind": "evidence", **kwargs})
        return dict(self.row)

    def transition_cancel_delivery_with_event(self, delivery_id: str, **kwargs: Any) -> Mapping[str, Any]:
        assert delivery_id == self.row["delivery_id"]
        token = kwargs["token"]
        assert token.row_version == self.row["row_version"]
        assert self.row["status"] in kwargs["expected_statuses"]
        self.row["status"] = kwargs["next_status"]
        self.row["remote_status"] = kwargs.get("remote_status")
        if kwargs.get("kill_receipt") is not None:
            self.row["kill_receipt_json"] = dict(kwargs["kill_receipt"])
        if kwargs.get("expected_process_identity") is not None:
            self.row["expected_process_identity_json"] = dict(kwargs["expected_process_identity"])
            self.row["expected_process_identity_hash"] = kwargs["expected_process_identity_hash"]
        self.row["kill_intent_generation"] = kwargs.get(
            "kill_intent_generation", self.row["kill_intent_generation"],
        )
        self.row["kill_intent_hash"] = kwargs.get("kill_intent_hash", self.row["kill_intent_hash"])
        self.row["row_version"] += 1
        if self.row["status"] == "succeeded":
            self.row["owner_id"] = None
        self.events.append({"kind": "transition", **kwargs})
        return dict(self.row)

    def yield_cancel_delivery_ownership(self, delivery_id: str, **kwargs: Any) -> Mapping[str, Any]:
        assert delivery_id == self.row["delivery_id"]
        token = kwargs["token"]
        assert token.row_version == self.row["row_version"]
        self.row["owner_id"] = None
        self.row["row_version"] += 1
        self.events.append({"kind": "yield", **kwargs})
        return dict(self.row)


class _Client:
    def __init__(
        self,
        inspection: QEWorkspaceSubmissionInspection,
        receipt: QEWorkspaceTypedKillReceipt | None,
    ) -> None:
        self.inspection = inspection
        self.receipt = receipt
        self.typed_calls: list[dict[str, Any]] = []
        self.closed = False

    async def inspect_loop_submission(self, _task_id: str, _loop_id: str, **_kwargs: Any) -> QEWorkspaceSubmissionInspection:
        return self.inspection

    async def kill_loop_typed(self, _task_id: str, _loop_id: str, **kwargs: Any) -> QEWorkspaceTypedKillReceipt:
        self.typed_calls.append(dict(kwargs))
        assert self.receipt is not None
        return self.receipt

    async def close(self) -> None:
        self.closed = True


def _delivery_row(*, status: str = "pending") -> dict[str, Any]:
    return {
        "delivery_id": "macdl_45a4aaf57badc4b87694eb7e0d44c9b2",
        "originating_command_id": COMMAND_ID,
        "run_id": "macb_5f4033e6af2ad5f8d25a96fd5b5d2d09",
        "child_id": "macbc_357d0b752ac8ee0e2b9df985f7eac7c1",
        "attempt_id": "macba_7bf164c8f2a838580d3964b5944dfe13",
        "node_id": "wsl2-5080",
        "qe_task_id": "qe_task",
        "qe_loop_id": "Loop1",
        "submission_intent_hash": SUBMISSION_HASH,
        "kill_target_key": KILL_TARGET_KEY,
        "expected_process_identity_json": None,
        "expected_process_identity_hash": None,
        "kill_intent_generation": 1,
        "kill_intent_hash": None,
        "kill_receipt_json": {},
        "status": status,
        "remote_status": None,
        "owner_id": None,
        "fencing_token": 0,
        "row_version": 1,
    }


def _inspection(*, status: str, identity: dict[str, int] | None) -> QEWorkspaceSubmissionInspection:
    return QEWorkspaceSubmissionInspection(
        schema_version="qe_submission_receipt_v1",
        task_id="qe_task",
        loop_id="Loop1",
        status=status,
        submission_intent_hash=SUBMISSION_HASH,
        request_digest="c" * 64,
        pid=identity["pid"] if identity is not None else None,
        process_identity=identity,
    )


def _typed_receipt(*, status: str, expected_identity: dict[str, int] | None) -> QEWorkspaceTypedKillReceipt:
    kill_hash = kill_intent_hash_for(
        kill_target_key=KILL_TARGET_KEY,
        process_identity_hash=(
            process_identity_hash_for(expected_identity)
            if expected_identity is not None
            else None
        ),
        generation=1,
    )
    return QEWorkspaceTypedKillReceipt(
        schema_version="qe_kill_receipt_v1",
        task_id="qe_task",
        loop_id="Loop1",
        command_id=COMMAND_ID,
        kill_intent_generation=1,
        kill_intent_hash=kill_hash,
        expected_submission_intent_hash=SUBMISSION_HASH,
        expected_process_identity=expected_identity,
        expected_phase="pre_process_start" if expected_identity is None else None,
        process_identity=expected_identity,
        status=status,
        signal_attempt_count=1 if expected_identity is not None else 0,
        signal_sent_at="2026-07-21T00:00:01Z" if expected_identity is not None else None,
        signal_sent=expected_identity is not None,
        process_observation={"identity": expected_identity},
        result_observation={"path": "qlib_results_enhanced.json", "present": False, "valid": False},
        submission_receipt_status="cancelled" if status == "cancelled" else "running",
        terminal_reason="cancelled_after_exact_signal" if status == "cancelled" else None,
        error=None,
        created_at="2026-07-21T00:00:00Z",
        updated_at="2026-07-21T00:00:01Z",
        completed_at="2026-07-21T00:00:01Z" if status == "cancelled" else None,
    )


def test_typed_delivery_persists_identity_before_signal_and_never_calls_legacy() -> None:
    repository = _Repository(_delivery_row())
    expected_kill_hash = kill_intent_hash_for(
        kill_target_key=KILL_TARGET_KEY,
        process_identity_hash=process_identity_hash_for(PROCESS_IDENTITY),
        generation=1,
    )
    receipt = _typed_receipt(status="cancelled", expected_identity=PROCESS_IDENTITY)
    client = _Client(_inspection(status="running", identity=PROCESS_IDENTITY), receipt)
    worker = DurableCancellationDeliveryWorker(
        repository=repository,  # type: ignore[arg-type]
        workspace_client_factory=lambda _node: client,
    )

    assert asyncio.run(worker.deliver_once(owner_id="worker", lease_seconds=60)) is True
    assert repository.row["status"] == "succeeded"
    assert client.typed_calls == [
        {
            "command_id": COMMAND_ID,
            "kill_intent_generation": 1,
            "kill_intent_hash": expected_kill_hash,
            "expected_submission_intent_hash": SUBMISSION_HASH,
            "expected_process_identity": PROCESS_IDENTITY,
            "expected_phase": None,
        }
    ]
    evidence = next(event for event in repository.events if event["kind"] == "evidence")
    assert evidence["kill_intent_hash"] == expected_kill_hash
    assert client.closed is True


def test_typed_delivery_keeps_missing_process_identity_visible_without_signal() -> None:
    repository = _Repository(_delivery_row())
    client = _Client(_inspection(status="running", identity=None), None)
    worker = DurableCancellationDeliveryWorker(
        repository=repository,  # type: ignore[arg-type]
        workspace_client_factory=lambda _node: client,
    )

    assert asyncio.run(worker.deliver_once(owner_id="worker", lease_seconds=60)) is True
    assert repository.row["status"] == "reconciling"
    assert repository.row["kill_intent_hash"] is None
    assert client.typed_calls == []
    assert repository.events[-1]["kind"] == "yield"


def test_existing_typed_receipt_is_not_re_signalled_after_restart() -> None:
    row = _delivery_row(status="reconciling")
    row["kill_intent_hash"] = kill_intent_hash_for(
        kill_target_key=KILL_TARGET_KEY,
        process_identity_hash=None,
        generation=1,
    )
    row["kill_receipt_json"] = {
        "status": "reconciling",
        "signal_sent": True,
        "terminal_reason": None,
    }
    repository = _Repository(row)
    client = _Client(_inspection(status="running", identity=PROCESS_IDENTITY), None)
    worker = DurableCancellationDeliveryWorker(
        repository=repository,  # type: ignore[arg-type]
        workspace_client_factory=lambda _node: client,
    )

    assert asyncio.run(worker.deliver_once(owner_id="worker", lease_seconds=60)) is True
    assert client.typed_calls == []
    assert repository.row["status"] == "reconciling"
    assert repository.events[-1]["kind"] == "yield"
