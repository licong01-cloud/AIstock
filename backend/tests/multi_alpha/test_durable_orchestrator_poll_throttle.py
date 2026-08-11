from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, Mapping, Sequence

import pytest

from backend.services.multi_alpha.durable_execution_adapter import (
    DurableCollectedResult,
    DurablePublishedArtifacts,
    DurableSubmissionIntent,
)
from backend.services.multi_alpha.durable_models import OwnershipToken
from backend.services.multi_alpha.durable_orchestrator import (
    DurableMultiAlphaOrchestrator,
    DurableOrchestratorCycleResult,
    DurableOrchestratorConfig,
    DurableOrchestratorError,
)
from backend.services.multi_alpha.durable_control import DurableMultiAlphaControlService
from backend.services.multi_alpha.durable_wakeup import notify_durable_orchestrator


class _Noop:
    pass


class _IdleRepository:
    def __init__(self) -> None:
        self.due_reads = 0

    def has_due_orchestrator_work(self, **_kwargs: Any) -> bool:
        self.due_reads += 1
        return False


class _BacklogRepository:
    def __init__(self, pending: int) -> None:
        self.pending = pending
        self.due_reads = 0

    def has_due_orchestrator_work(self, **_kwargs: Any) -> bool:
        self.due_reads += 1
        return self.pending > 0


def test_idle_worker_uses_only_coalesced_due_reads_and_never_enters_claim_cycle() -> None:
    repository = _IdleRepository()
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
            heartbeat_seconds=60,
            items_per_pass=1,
            archive_batch_size=1,
            remote_poll_seconds=60,
            safety_sweep_seconds=0.01,
        ),
        owner_id="idle-worker",
    )
    orchestrator._activation_import_completed = True
    orchestrator._p0_2_schema_ready = True

    async def forbidden_cycle() -> Any:
        raise AssertionError("an idle due-summary must bypass all claim/DML passes")

    orchestrator.run_cycle = forbidden_cycle  # type: ignore[method-assign]

    async def scenario() -> None:
        stop_event = asyncio.Event()
        worker = asyncio.create_task(orchestrator.run_forever(stop_event))
        await asyncio.sleep(0.025)
        # A burst of commits coalesces; no work in PostgreSQL still means no
        # claim cycle and therefore no DML/event/remote side effect.
        for _ in range(20):
            notify_durable_orchestrator()
        await asyncio.sleep(0.01)
        stop_event.set()
        notify_durable_orchestrator()
        await worker

    asyncio.run(scenario())

    assert DurableOrchestratorConfig().safety_sweep_seconds == 60.0
    assert 2 <= repository.due_reads <= 6


def test_runtime_config_rejects_database_heartbeat_faster_than_once_per_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AISTOCK_MULTI_ALPHA_DURABLE_HEARTBEAT_SECONDS", "30")

    with pytest.raises(DurableOrchestratorError) as caught:
        DurableOrchestratorConfig.from_env()

    assert caught.value.reason_code == "multi_alpha_durable_config_invalid"


def test_bounded_backlog_self_wakes_until_more_than_one_batch_is_drained() -> None:
    repository = _BacklogRepository(pending=17)
    orchestrator = DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=_Noop(),  # type: ignore[arg-type]
        archive_capture=_Noop(),  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        recovery_worker=_Noop(),  # type: ignore[arg-type]
        config=DurableOrchestratorConfig(
            lease_seconds=600,
            heartbeat_seconds=60,
            items_per_pass=8,
            archive_batch_size=8,
            remote_poll_seconds=60,
            safety_sweep_seconds=60,
        ),
        owner_id="backlog-worker",
    )
    orchestrator._activation_import_completed = True
    orchestrator._p0_2_schema_ready = True
    batches: list[int] = []

    async def scenario() -> None:
        stop_event = asyncio.Event()

        async def drain_one_batch() -> DurableOrchestratorCycleResult:
            count = min(orchestrator._config.items_per_pass, repository.pending)
            repository.pending -= count
            batches.append(count)
            if repository.pending == 0:
                stop_event.set()
            return DurableOrchestratorCycleResult(
                planned_runs=count,
                dispatched_attempts=0,
                reconciled_attempts=0,
                finalized_runs=0,
                archive_events=0,
            )

        orchestrator.run_cycle = drain_one_batch  # type: ignore[method-assign]
        await asyncio.wait_for(orchestrator.run_forever(stop_event), timeout=0.5)

    asyncio.run(scenario())

    assert batches == [8, 8, 1]
    assert repository.pending == 0
    assert repository.due_reads == 3


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
        self.yields = 0
        self.claim_conflict = False

    def advance(self, seconds: float = 2.0) -> None:
        self.now += seconds

    def claim_next_attempt(
        self,
        *,
        owner_id: str,
        lease_seconds: int,
        p0_2_schema_ready: bool = True,
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

    def observe_next_reconcilable_attempt(
        self,
        *,
        p0_2_schema_ready: bool = True,
        excluded_attempt_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 60,
    ) -> Mapping[str, Any] | None:
        if self.attempt["attempt_id"] in excluded_attempt_ids:
            return None
        if (
            self.attempt["status"] != "submitting"
            and self.now - self.attempt["updated_at"] < min_recheck_interval_seconds
        ):
            return None
        return dict(self.attempt)

    def claim_observed_attempt(
        self,
        attempt_id: str,
        *,
        p0_2_schema_ready: bool = True,
        expected_row_version: int,
        owner_id: str,
        lease_seconds: int,
    ) -> Mapping[str, Any] | None:
        if self.claim_conflict:
            return None
        if (
            attempt_id != self.attempt["attempt_id"]
            or expected_row_version != self.attempt["row_version"]
        ):
            return None
        self.claims += 1
        self.attempt["owner_id"] = owner_id
        self.attempt["fencing_token"] += 1
        self.attempt["lease_expires_at"] = self.now + lease_seconds
        self.attempt["updated_at"] = self.now
        self.attempt["row_version"] += 1
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
        self.yields += 1
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


class _PreP0ThrottleRepository(_ThrottleRepository):
    """Baseline attempt storage has neither run_id nor execution_kind.

    observe/claim return the parent identity derived from the baseline child
    join, matching the repository SQL contract rather than forging a P0-2
    column in the stored attempt row.
    """

    def __init__(self) -> None:
        super().__init__()
        self.attempt.pop("run_id")

    def observe_next_reconcilable_attempt(
        self,
        *,
        p0_2_schema_ready: bool = True,
        excluded_attempt_ids: Sequence[str] = (),
        min_recheck_interval_seconds: int = 60,
    ) -> Mapping[str, Any] | None:
        assert p0_2_schema_ready is False
        row = super().observe_next_reconcilable_attempt(
            p0_2_schema_ready=p0_2_schema_ready,
            excluded_attempt_ids=excluded_attempt_ids,
            min_recheck_interval_seconds=min_recheck_interval_seconds,
        )
        return {**row, "run_id": self.child["run_id"]} if row is not None else None

    def claim_observed_attempt(
        self,
        attempt_id: str,
        *,
        p0_2_schema_ready: bool = True,
        expected_row_version: int,
        owner_id: str,
        lease_seconds: int,
    ) -> Mapping[str, Any] | None:
        assert p0_2_schema_ready is False
        row = super().claim_observed_attempt(
            attempt_id,
            p0_2_schema_ready=p0_2_schema_ready,
            expected_row_version=expected_row_version,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )
        return {**row, "run_id": self.child["run_id"]} if row is not None else None


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

    def record_remote_terminal(
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
        monotonic=lambda: repository.now,
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

    assert adapter.inspect_calls == 4
    assert adapter.submit_calls == 0
    assert repository.claims == 0
    assert repository.heartbeats == 0
    assert repository.yields == 0
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


def test_pre_p0_remote_status_change_uses_child_derived_run_identity() -> None:
    repository = _PreP0ThrottleRepository()
    repository.attempt["remote_status"] = "reserved"
    adapter = _InspectAdapter(repository, remote_status="running")
    orchestrator = _orchestrator(repository, adapter)
    orchestrator._p0_2_schema_ready = False

    asyncio.run(orchestrator.reconcile_pass_once())

    assert "run_id" not in repository.attempt
    assert "execution_kind" not in repository.attempt
    assert repository.attempt["status"] == "running"
    assert repository.attempt["remote_status"] == "running"
    assert repository.claims == 1
    assert repository.yields == 1


def test_pre_p0_terminal_observation_completes_without_attempt_run_id_column() -> None:
    repository = _PreP0ThrottleRepository()
    adapter = _InspectAdapter(repository, remote_status="completed")
    orchestrator = _orchestrator(repository, adapter)
    orchestrator._p0_2_schema_ready = False

    asyncio.run(orchestrator.reconcile_pass_once())

    assert "run_id" not in repository.attempt
    assert "execution_kind" not in repository.attempt
    assert repository.attempt["status"] == "succeeded"
    assert adapter.terminal_calls == 1
    assert adapter.collect_calls == 1
    assert repository.child["selected_attempt_id"] == "macba_poll"


def test_stale_remote_observation_is_discarded_when_snapshot_claim_loses_race() -> None:
    repository = _ThrottleRepository()
    repository.claim_conflict = True
    adapter = _InspectAdapter(repository, remote_status="completed")
    orchestrator = _orchestrator(repository, adapter)

    asyncio.run(orchestrator.reconcile_pass_once())

    assert adapter.inspect_calls == 1
    assert adapter.collect_calls == 0
    assert adapter.terminal_calls == 0
    assert repository.attempt["status"] == "running"
    assert repository.events == []


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
    assert repository.preflight_calls == 0


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
        self.preflight_calls = 0

    def advance(self, seconds: float = 2.0) -> None:
        self.now += seconds

    def preflight_p0_2_schema(self, *, raise_on_error: bool = False) -> SimpleNamespace:
        self.preflight_calls += 1
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
