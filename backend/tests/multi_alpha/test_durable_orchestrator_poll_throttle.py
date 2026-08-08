from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

from backend.services.multi_alpha.durable_execution_adapter import (
    DurableCollectedResult,
    DurablePublishedArtifacts,
    DurableSubmissionIntent,
)
from backend.services.multi_alpha.durable_models import OwnershipToken
from backend.services.multi_alpha.durable_orchestrator import (
    DurableMultiAlphaOrchestrator,
    DurableOrchestratorConfig,
    DurableOrchestratorError,
)
from backend.services.multi_alpha.durable_control import DurableMultiAlphaControlService


class _Noop:
    pass


class _ThrottleRepository:
    """Simulates the reconcile claim throttle (min_recheck_interval_seconds on
    updated_at) and the unchanged-state event suppression contract."""

    def __init__(self) -> None:
        self.now = 0.0
        self.attempt = {
            "attempt_id": "macba_poll",
            "child_id": "macbc_poll",
            "run_id": "macb_poll",
            "status": "running",
            "phase": "running",
            "owner_id": None,
            "fencing_token": 0,
            "row_version": 1,
            "remote_status": "running",
            "updated_at": -100.0,
            "lease_expires_at": None,
            "node_id": "wsl2-5080",
            "submitted_at": None,
            "started_at": None,
            "finished_at": None,
            "error_code": None,
            "error_json": None,
            "result_manifest_json": {},
            "artifact_manifest_json": {},
        }
        self.child = {
            "child_id": "macbc_poll",
            "run_id": "macb_poll",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "status": "running",
            "selected_attempt_id": None,
        }
        self.run = {"id": "macb_poll", "status": "running", "started_at": None, "created_at": None}
        self.events: list[dict[str, Any]] = []
        self.claims = 0
        self.heartbeats = 0

    def advance(self, seconds: float = 2.0) -> None:
        self.now += seconds

    def claim_next_attempt(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        claim_kind: str = "dispatch",
        node_id: str | None = None,
        excluded_attempt_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 0,
        write_claim_event: bool = True,
    ) -> Mapping[str, Any] | None:
        if (
            claim_kind == "reconcile"
            and self.attempt["status"] != "submitting"
            and self.now - self.attempt["updated_at"] < min_recheck_interval_seconds
        ):
            return None
        self.claims += 1
        self.attempt["owner_id"] = owner_id
        self.attempt["fencing_token"] += 1
        self.attempt["lease_expires_at"] = self.now + lease_seconds
        self.attempt["updated_at"] = self.now
        return dict(self.attempt)

    def get_child(self, child_id: str) -> Mapping[str, Any] | None:
        return dict(self.child) if child_id == self.child["child_id"] else None

    def get_run(self, run_id: str) -> Mapping[str, Any] | None:
        return dict(self.run) if run_id == self.run["id"] else None

    def get_attempt(self, attempt_id: str) -> Mapping[str, Any] | None:
        return dict(self.attempt) if attempt_id == self.attempt["attempt_id"] else None

    def heartbeat_attempt(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> Mapping[str, Any]:
        self.heartbeats += 1
        self.attempt["lease_expires_at"] = self.now + lease_seconds
        self.attempt["updated_at"] = self.now
        self.attempt["row_version"] += 1
        return dict(self.attempt)

    def update_attempt_remote_status(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        remote_status: str,
    ) -> Mapping[str, Any]:
        self.attempt["remote_status"] = remote_status
        self.attempt["updated_at"] = self.now
        self.attempt["row_version"] += 1
        return dict(self.attempt)

    def yield_attempt_ownership(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> Mapping[str, Any]:
        self.attempt["owner_id"] = None
        self.attempt["lease_expires_at"] = None
        self.attempt["updated_at"] = self.now
        self.attempt["row_version"] += 1
        if write_event:
            self.events.append({"phase": phase, "attempt_id": attempt_id})
        return dict(self.attempt)

    def transition_attempt_with_event(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: Sequence[str],
        next_status: str,
        phase: str,
        remote_status: str | None = None,
        result_manifest: Mapping[str, Any] | None = None,
        **_kwargs: Any,
    ) -> Mapping[str, Any]:
        assert self.attempt["status"] in expected_statuses
        self.attempt["status"] = next_status
        self.attempt["phase"] = phase
        if remote_status is not None:
            self.attempt["remote_status"] = remote_status
        if result_manifest is not None:
            self.attempt["result_manifest_json"] = dict(result_manifest)
        self.attempt["updated_at"] = self.now
        self.attempt["row_version"] += 1
        self.events.append({"phase": phase, "attempt_id": attempt_id, "status": next_status})
        return dict(self.attempt)

    def transition_child_with_event(
        self,
        child_id: str,
        *,
        expected_statuses: Sequence[str],
        next_status: str,
        phase: str,
        **_kwargs: Any,
    ) -> Mapping[str, Any]:
        assert self.child["status"] in expected_statuses
        self.child["status"] = next_status
        self.events.append({"phase": phase, "child_id": child_id, "status": next_status})
        return dict(self.child)

    def set_child_reconciling_attempt(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        phase: str,
        event_payload: Mapping[str, Any] | None = None,
    ) -> Mapping[str, Any]:
        self.child["selected_attempt_id"] = selected_attempt_id
        self.events.append({"phase": phase, "child_id": child_id})
        return dict(self.child)

    def append_error_if_fingerprint_new(
        self,
        *,
        run_id: str,
        phase: str,
        error: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
    ) -> Mapping[str, Any] | None:
        return None

    def append_event_if_phase_new(
        self,
        *,
        run_id: str,
        phase: str,
        event_type: str,
        payload: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
        reason_code: str | None = None,
    ) -> Mapping[str, Any] | None:
        return None


class _InspectAdapter:
    def __init__(self, repository: _ThrottleRepository, remote_status: str = "running") -> None:
        self.repository = repository
        self.remote_status = remote_status
        self.inspect_calls = 0
        self.submit_calls = 0
        self.terminal_calls = 0
        self.collect_calls = 0

    @staticmethod
    def request_from_run(_run: Mapping[str, Any]) -> Any:
        return SimpleNamespace(scheme_timeout_seconds=60, run_timeout_seconds=120)

    def prepare_submission_intent(
        self,
        *,
        run: Mapping[str, Any],
        child: Mapping[str, Any],
        attempt: Mapping[str, Any],
        node_id: str,
    ) -> DurableSubmissionIntent:
        return DurableSubmissionIntent(
            run_id=str(run["id"]),
            child_id=str(child["child_id"]),
            attempt_id=str(attempt["attempt_id"]),
            attempt_no=1,
            node_id=node_id,
            qe_task_id="qe_task",
            qe_loop_id="Loop1",
            submission_intent_hash="a" * 64,
        )

    def load_published_artifacts(
        self,
        *,
        run_id: str,
        child_id: str,
        attempt_id: str,
    ) -> DurablePublishedArtifacts:
        return DurablePublishedArtifacts(
            workspace=None,
            prediction_path=None,
            artifact_manifest_path=None,
            artifact_manifest={},
        )

    async def inspect_remote(self, *, intent: DurableSubmissionIntent) -> SimpleNamespace:
        self.inspect_calls += 1
        return SimpleNamespace(
            status={"status": self.remote_status},
            receipt={"receipt": "r1"},
        )

    async def submit(
        self,
        *,
        artifacts: DurablePublishedArtifacts,
        intent: DurableSubmissionIntent,
        attempt_token: OwnershipToken,
    ) -> SimpleNamespace:
        self.submit_calls += 1
        return SimpleNamespace(
            waiting_capacity=False,
            remote_status=self.remote_status,
            detail={},
        )

    async def record_remote_terminal(
        self,
        *,
        intent: DurableSubmissionIntent,
        owner_id: str,
        remote_status: str,
    ) -> Mapping[str, Any]:
        self.terminal_calls += 1
        return {"status": "released"}

    async def collect_result(
        self,
        *,
        intent: DurableSubmissionIntent,
        artifacts: DurablePublishedArtifacts,
        execution_deadline_evidence: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> DurableCollectedResult:
        self.collect_calls += 1
        manifest = {"manifest_hash": "f" * 64}
        return DurableCollectedResult(
            metrics={"sharpe": 1.2},
            result_manifest=manifest,
            result_manifest_path=None,
        )


def _orchestrator(
    repository: _ThrottleRepository,
    adapter: _InspectAdapter,
    *,
    remote_poll_seconds: int = 60,
) -> DurableMultiAlphaOrchestrator:
    return DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
        archive_capture=_Noop(),  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        recovery_worker=_Noop(),  # type: ignore[arg-type]
        config=DurableOrchestratorConfig(
            poll_seconds=0.2,
            lease_seconds=600,
            heartbeat_seconds=5,
            items_per_pass=1,
            archive_batch_size=10,
            remote_poll_seconds=remote_poll_seconds,
        ),
        owner_id="worker",
    )


def test_reconcile_poll_throttle_limits_remote_reads_and_events() -> None:
    """120 cycles at 2s with a constantly-running remote must not re-inspect the
    remote more than once per minute and must not append unchanged-state events."""
    repository = _ThrottleRepository()
    adapter = _InspectAdapter(repository, remote_status="running")
    orchestrator = _orchestrator(repository, adapter, remote_poll_seconds=60)

    for _ in range(120):
        asyncio.run(orchestrator.reconcile_pass_once())
        repository.advance(2.0)

    assert adapter.inspect_calls <= 5
    assert adapter.submit_calls == 0
    # unchanged running state produced no durable events at all
    assert repository.events == []


def test_running_to_succeeded_records_one_terminal_event_and_result_once() -> None:
    """A real running -> succeeded transition records one terminal event and
    collects the result exactly once (no duplicate result writes)."""
    repository = _ThrottleRepository()
    adapter = _InspectAdapter(repository, remote_status="completed")
    orchestrator = _orchestrator(repository, adapter)

    asyncio.run(
        orchestrator.reconcile_pass_once()
    )

    assert repository.attempt["status"] == "succeeded"
    assert adapter.collect_calls == 1
    terminal_events = [
        event
        for event in repository.events
        if event.get("status") == "succeeded" and event.get("phase") == "result_persisted"
    ]
    assert len(terminal_events) == 1
    assert repository.child["selected_attempt_id"] == "macba_poll"


def test_restart_recovery_resumes_and_does_not_resubmit_running_process() -> None:
    """A fresh orchestrator instance resumes from the persisted attempt row and
    re-checks within ~60s without re-submitting the already-running process."""
    repository = _ThrottleRepository()
    adapter = _InspectAdapter(repository, remote_status="running")
    orchestrator = _orchestrator(repository, adapter, remote_poll_seconds=60)

    # New orchestrator instance (fresh memory) sees the persisted row whose
    # updated_at is old (-100s) -> immediately claimable and re-checked.
    asyncio.run(orchestrator.reconcile_pass_once())

    assert adapter.inspect_calls == 1
    assert adapter.submit_calls == 0
    assert repository.attempt["status"] == "running"


def test_business_finalize_error_repeats_keep_first_event_only() -> None:
    """100 identical finalize-error cycles keep only the first error evidence."""
    repository = _FinalizeErrorRepository()
    orchestrator = DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=_Noop(),  # type: ignore[arg-type]
        archive_capture=_Noop(),  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        recovery_worker=_Noop(),  # type: ignore[arg-type]
        config=DurableOrchestratorConfig(
            poll_seconds=0.2,
            lease_seconds=600,
            heartbeat_seconds=5,
            items_per_pass=1,
            archive_batch_size=10,
            remote_poll_seconds=60,
        ),
        owner_id="worker",
    )

    for _ in range(100):
        asyncio.run(orchestrator.finalizer_pass_once())

    error_events = [event for event in repository.events if event.get("event_type") == "error"]
    assert len(error_events) == 1
    assert error_events[0]["phase"] == "business_finalize_error"
    # unchanged-state yield produced no status event
    assert not any(
        event.get("phase") == "business_finalize_error" and event.get("event_type") != "error"
        for event in repository.events
    )


class _FinalizeErrorRepository:
    def __init__(self) -> None:
        self.run = {
            "id": "macb_finalize",
            "owner_id": "worker",
            "fencing_token": 1,
            "row_version": 1,
        }
        self.events: list[dict[str, Any]] = []
        self._seen_fingerprints: set[str] = set()

    def claim_next_finalizable_run(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_run_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 0,
        write_claim_event: bool = True,
    ) -> Mapping[str, Any] | None:
        return dict(self.run)

    def list_children(self, run_id: str) -> list[Mapping[str, Any]]:
        raise DurableOrchestratorError(
            "business readback mismatch",
            reason_code="business_readback_mismatch",
            context={"run_id": run_id},
        )

    def get_run(self, run_id: str) -> Mapping[str, Any] | None:
        return dict(self.run)

    def yield_run_ownership(
        self,
        run_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> Mapping[str, Any]:
        if write_event:
            self.events.append({"phase": phase, "event_type": "status"})
        return dict(self.run)

    def append_error_if_fingerprint_new(
        self,
        *,
        run_id: str,
        phase: str,
        error: Mapping[str, Any],
        child_id: str | None = None,
        attempt_id: str | None = None,
    ) -> Mapping[str, Any] | None:
        fingerprint = f"{error.get('reason_code')}|{error.get('message')}"
        if fingerprint in self._seen_fingerprints:
            return None
        self._seen_fingerprints.add(fingerprint)
        self.events.append({"phase": phase, "event_type": "error"})
        return {"event_id": len(self.events)}


def test_control_reconciliation_pending_is_not_reappended_while_unchanged() -> None:
    """A long-reconciling control command does not append a new
    control_reconciliation_pending event while its state is unchanged."""
    repository = _ControlRepository()
    service = DurableMultiAlphaControlService(repository=repository)  # type: ignore[arg-type]

    # First apply: command is accepted -> claim transitions to applying/reconciling.
    repository.command["status"] = "accepted"
    result = service.apply_one_local_command(
        owner_id="worker",
        lease_seconds=600,
        min_recheck_interval_seconds=60,
    )
    assert result is not None

    # Re-claim a still-reconciling command -> throttled, no new event.
    repository.command["status"] = "reconciling"
    repository.command["updated_at"] = 0.0
    for _ in range(30):
        service.apply_one_local_command(
            owner_id="worker",
            lease_seconds=600,
            min_recheck_interval_seconds=60,
        )
        repository.advance(2.0)

    pending = [
        event
        for event in repository.events
        if event.get("phase") == "control_reconciliation_pending"
    ]
    assert len(pending) <= 1


class _ControlRepository:
    def __init__(self) -> None:
        self.command = {
            "command_id": "macmd_reconcile",
            "run_id": "macb_ctl",
            "child_id": None,
            "attempt_id": None,
            "status": "accepted",
            "action": "pause",
            "owner_id": None,
            "fencing_token": 0,
            "row_version": 1,
            "updated_at": -100.0,
        }
        self.events: list[dict[str, Any]] = []
        self.now = 0.0

    def advance(self, seconds: float = 2.0) -> None:
        self.now += seconds

    def preflight_p0_2_schema(self, *, raise_on_error: bool = False) -> SimpleNamespace:
        return SimpleNamespace(ready=True)

    def claim_next_command(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        excluded_command_ids: Sequence[str] = (),
        actions: Sequence[str] | None = None,
        min_recheck_interval_seconds: int = 0,
        write_claim_event: bool = True,
    ) -> Mapping[str, Any] | None:
        if (
            self.command["status"] == "reconciling"
            and self.now - self.command["updated_at"] < min_recheck_interval_seconds
        ):
            return None
        self.command["owner_id"] = owner_id
        self.command["fencing_token"] += 1
        self.command["lease_expires_at"] = self.now + lease_seconds
        self.command["updated_at"] = self.now
        if self.command["status"] == "accepted":
            self.command["status"] = "applying"
        return dict(self.command)

    def apply_control_command_intent(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
    ) -> Mapping[str, Any]:
        self.command["status"] = "reconciling"
        self.events.append({"phase": "command_applied", "event_type": "control"})
        return dict(self.command)

    def reconcile_control_command(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
    ) -> Mapping[str, Any]:
        # stays reconciling (run not yet paused)
        return dict(self.command)

    def yield_command_ownership(
        self,
        command_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> Mapping[str, Any]:
        self.command["owner_id"] = None
        if write_event:
            self.events.append({"phase": phase, "event_type": "control"})
        return dict(self.command)


def test_event_cursor_read_compatibility_survives_event_gaps() -> None:
    """list_events / SSE cursor semantics remain compatible with event gaps."""
    repository = _ThrottleRepository()
    adapter = _InspectAdapter(repository, remote_status="running")
    orchestrator = _orchestrator(repository, adapter)

    asyncio.run(orchestrator.reconcile_pass_once())
    repository.advance(2.0)
    asyncio.run(orchestrator.reconcile_pass_once())

    # Throttle suppressed all unchanged-state events; cursor reads are no-ops.
    assert repository.events == []
    assert adapter.inspect_calls == 1
