from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping

from backend.services.multi_alpha.durable_execution_adapter import (
    DurableCollectedResult,
    DurableExecutionAdapterError,
    DurablePublishedArtifacts,
    DurableSubmissionIntent,
)
from backend.services.multi_alpha.durable_models import OwnershipToken
from backend.services.multi_alpha.durable_orchestrator import (
    DurableMultiAlphaOrchestrator,
    DurableOrchestratorCycleResult,
    DurableOrchestratorConfig,
)


class _SchemaHealth:
    def __init__(self, *, ready: bool, reason_code: str | None = None) -> None:
        self.ready = ready
        self.reason_code = reason_code

    def as_dict(self) -> dict[str, Any]:
        return {"ready": self.ready, "reason_code": self.reason_code}


class _PreP0_2Repository:
    def preflight_schema(self, *, raise_on_error: bool = False) -> _SchemaHealth:
        assert raise_on_error is True
        return _SchemaHealth(ready=True)

    def preflight_p0_2_schema(self, *, raise_on_error: bool = False) -> _SchemaHealth:
        assert raise_on_error is False
        return _SchemaHealth(ready=False, reason_code="multi_alpha_p0_2_schema_unavailable")


class _ActiveImport:
    async def import_current_active_sources_verified(self) -> SimpleNamespace:
        return SimpleNamespace(
            discovered_count=0,
            imported_count=0,
            deduplicated_count=0,
            unresolved=(),
            queue_only_nodes={},
        )


class _Repository:
    def __init__(self) -> None:
        self.attempt = {
            "attempt_id": "macba_test",
            "child_id": "macbc_test",
            "run_id": "macb_test",
            "status": "running",
            "owner_id": "worker",
            "fencing_token": 1,
            "row_version": 1,
            "result_manifest_json": {},
        }
        self.child = {
            "child_id": "macbc_test",
            "run_id": "macb_test",
            "child_key": "scheme:equal",
            "child_kind": "scheme",
            "weighting_scheme": "equal",
            "status": "running",
            "selected_attempt_id": None,
        }
        self.events: list[dict[str, Any]] = []
        self.yields = 0

    def get_attempt(self, attempt_id: str) -> Mapping[str, Any] | None:
        return self.attempt if attempt_id == self.attempt["attempt_id"] else None

    def get_child(self, child_id: str) -> Mapping[str, Any] | None:
        return self.child if child_id == self.child["child_id"] else None

    def transition_attempt_with_event(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        expected_statuses: tuple[str, ...],
        next_status: str,
        **_kwargs: Any,
    ) -> Mapping[str, Any]:
        assert attempt_id == self.attempt["attempt_id"]
        assert token.row_version == self.attempt["row_version"]
        assert self.attempt["status"] in expected_statuses
        self.attempt["status"] = next_status
        self.attempt["row_version"] += 1
        if _kwargs.get("result_manifest") is not None:
            self.attempt["result_manifest_json"] = dict(_kwargs["result_manifest"])
        self.events.append(
            {
                "attempt_id": attempt_id,
                "phase": _kwargs.get("phase"),
                "status": next_status,
            }
        )
        if next_status in {"succeeded", "failed", "cancelled"}:
            self.attempt["owner_id"] = None
        return dict(self.attempt)

    def record_attempt_deadline_evidence(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        evidence: Mapping[str, Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        assert attempt_id == self.attempt["attempt_id"]
        assert token.row_version == self.attempt["row_version"]
        manifest = dict(self.attempt.get("result_manifest_json") or {})
        existing = dict(manifest.get("execution_deadline") or {})
        existing.update({key: dict(value) for key, value in evidence.items()})
        manifest["execution_deadline"] = existing
        self.attempt["result_manifest_json"] = manifest
        self.attempt["row_version"] += 1
        self.events.append(
            {
                "attempt_id": attempt_id,
                "phase": "deadline_exceeded",
                "payload": {"execution_deadline": existing},
            }
        )
        return dict(self.attempt)

    def heartbeat_attempt(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> Mapping[str, Any]:
        assert attempt_id == self.attempt["attempt_id"]
        assert lease_seconds > 0
        assert token.owner_id == self.attempt["owner_id"]
        assert token.fencing_token == self.attempt["fencing_token"]
        assert token.row_version == self.attempt["row_version"]
        self.attempt["row_version"] += 1
        return dict(self.attempt)

    def transition_child_with_event(
        self,
        child_id: str,
        *,
        expected_statuses: tuple[str, ...],
        next_status: str,
        **_kwargs: Any,
    ) -> Mapping[str, Any]:
        assert child_id == self.child["child_id"]
        assert self.child["status"] in expected_statuses
        self.child["status"] = next_status
        return dict(self.child)

    def set_child_reconciling_attempt(
        self,
        child_id: str,
        *,
        selected_attempt_id: str,
        **_kwargs: Any,
    ) -> Mapping[str, Any]:
        assert child_id == self.child["child_id"]
        assert self.attempt["status"] == "succeeded"
        self.child["status"] = "reconciling"
        self.child["selected_attempt_id"] = selected_attempt_id
        return dict(self.child)

    def finalize_scheme_child_without_result(
        self,
        child_id: str,
        *,
        next_status: str,
        selected_attempt_id: str | None = None,
        **_kwargs: Any,
    ) -> Mapping[str, Any]:
        assert child_id == self.child["child_id"]
        self.child["status"] = next_status
        self.child["selected_attempt_id"] = selected_attempt_id
        self.events.append(
            {
                "child_id": child_id,
                "phase": "business_result_unavailable",
                "status": next_status,
            }
        )
        return dict(self.child)

    def yield_attempt_ownership(
        self,
        attempt_id: str,
        *,
        token: OwnershipToken,
        phase: str,
        write_event: bool = True,
    ) -> Mapping[str, Any]:
        assert attempt_id == self.attempt["attempt_id"]
        assert phase in {
            "completed_result_collection_pending",
            "remote_status_unmapped",
            "terminal_reservation_reconciliation_pending",
        }
        assert token.row_version == self.attempt["row_version"]
        self.yields += 1
        self.attempt["owner_id"] = None
        self.attempt["row_version"] += 1
        return dict(self.attempt)

    def append_event(self, **kwargs: Any) -> Mapping[str, Any]:
        self.events.append(dict(kwargs))
        return kwargs


class _Adapter:
    def __init__(self, root: Path, repository: _Repository) -> None:
        self.root = root
        self.repository = repository
        self.collect_calls = 0
        self.terminal_calls = 0
        self.fail_first_reason = "multi_alpha_child_result_not_visible"

    def record_remote_terminal(self, **_kwargs: Any) -> Mapping[str, Any]:
        self.terminal_calls += 1
        return {"status": "released"}

    @staticmethod
    def request_from_run(_run: Mapping[str, Any]) -> Any:
        return SimpleNamespace(scheme_timeout_seconds=60, run_timeout_seconds=120)

    async def collect_result(
        self,
        *,
        intent: DurableSubmissionIntent,
        artifacts: DurablePublishedArtifacts,
        execution_deadline_evidence: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> DurableCollectedResult:
        self.collect_calls += 1
        if self.collect_calls == 1:
            raise DurableExecutionAdapterError(
                "result collection failed",
                reason_code=self.fail_first_reason,
            )
        manifest = {
            "manifest_hash": "f" * 64,
            "attempt_id": intent.attempt_id,
            "completed_after_deadline": bool(execution_deadline_evidence),
        }
        if execution_deadline_evidence:
            manifest["execution_deadline"] = {
                key: dict(value) for key, value in execution_deadline_evidence.items()
            }
        return DurableCollectedResult(
            metrics={"sharpe": 1.2},
            result_manifest=manifest,
            result_manifest_path=artifacts.workspace / "result_manifest.json",
        )


class _Noop:
    pass


class _RunHeartbeatRepository:
    def __init__(self) -> None:
        self.run = {
            "id": "macb_test",
            "owner_id": "worker",
            "fencing_token": 1,
            "row_version": 1,
        }
        self.heartbeat_calls = 0

    def heartbeat_run(
        self,
        run_id: str,
        *,
        token: OwnershipToken,
        lease_seconds: int,
    ) -> Mapping[str, Any]:
        assert run_id == self.run["id"]
        assert lease_seconds > 0
        assert token.owner_id == self.run["owner_id"]
        assert token.fencing_token == self.run["fencing_token"]
        assert token.row_version == self.run["row_version"]
        self.heartbeat_calls += 1
        self.run["row_version"] += 1
        return dict(self.run)


def _orchestrator(repository: _Repository, adapter: _Adapter) -> DurableMultiAlphaOrchestrator:
    return DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=adapter,  # type: ignore[arg-type]
        archive_capture=_Noop(),  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        recovery_worker=_Noop(),  # type: ignore[arg-type]
        config=DurableOrchestratorConfig(
            poll_seconds=0.2,
            lease_seconds=60,
            heartbeat_seconds=5,
            items_per_pass=1,
            archive_batch_size=1,
        ),
        owner_id="worker",
    )


def test_sync_operation_error_returns_latest_run_heartbeat_token() -> None:
    repository = _RunHeartbeatRepository()
    orchestrator = DurableMultiAlphaOrchestrator(
        repository=repository,  # type: ignore[arg-type]
        planner=_Noop(),  # type: ignore[arg-type]
        adapter=_Noop(),  # type: ignore[arg-type]
        archive_capture=_Noop(),  # type: ignore[arg-type]
        active_import_service=_Noop(),  # type: ignore[arg-type]
        recovery_worker=_Noop(),  # type: ignore[arg-type]
        owner_id="worker",
    )
    orchestrator._config = SimpleNamespace(heartbeat_seconds=0.01, lease_seconds=60)

    def failing_materialization() -> None:
        time.sleep(0.04)
        raise RuntimeError("scheme not computable")

    value, token, operation_error = asyncio.run(
        orchestrator._run_sync_with_run_heartbeat(
            run_id="macb_test",
            token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
            operation=failing_materialization,
        )
    )

    assert value is None
    assert isinstance(operation_error, RuntimeError)
    assert str(operation_error) == "scheme not computable"
    assert repository.heartbeat_calls >= 1
    assert token.row_version == repository.run["row_version"]
    refreshed = repository.heartbeat_run(
        "macb_test",
        token=token,
        lease_seconds=60,
    )
    assert refreshed["row_version"] == token.row_version + 1


def test_completed_remote_result_collection_survives_restart_window(tmp_path: Path) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path, repository)
    orchestrator = _orchestrator(repository, adapter)
    intent = DurableSubmissionIntent(
        run_id="macb_test",
        child_id="macbc_test",
        attempt_id="macba_test",
        attempt_no=1,
        node_id="wsl2-5080",
        qe_task_id="qe_test",
        qe_loop_id="Loop1",
        submission_intent_hash="a" * 64,
    )
    artifacts = DurablePublishedArtifacts(
        workspace=tmp_path,
        prediction_path=tmp_path / "combined_prediction.pkl",
        artifact_manifest_path=tmp_path / "artifact_manifest.json",
        artifact_manifest={"manifest_hash": "b" * 64},
    )
    run = {"id": "macb_test"}

    asyncio.run(
        orchestrator._apply_remote_status(
            run=run,
            child=dict(repository.child),
            attempt_id="macba_test",
            token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
            intent=intent,
            artifacts=artifacts,
            remote_status="completed",
            remote_payload={"status": "completed"},
        )
    )

    assert repository.attempt["status"] == "reconciling"
    assert repository.child["status"] == "reconciling"
    assert repository.yields == 1
    assert repository.events[-1]["phase"] == "completed_result_collection_pending"

    repository.attempt["owner_id"] = "worker"
    repository.attempt["fencing_token"] += 1
    repository.attempt["row_version"] += 1
    token = OwnershipToken(
        owner_id="worker",
        fencing_token=repository.attempt["fencing_token"],
        row_version=repository.attempt["row_version"],
    )
    asyncio.run(
        orchestrator._apply_remote_status(
            run=run,
            child=dict(repository.child),
            attempt_id="macba_test",
            token=token,
            intent=intent,
            artifacts=artifacts,
            remote_status="completed",
            remote_payload={"status": "completed"},
        )
    )

    assert repository.attempt["status"] == "succeeded"
    assert repository.child["status"] == "reconciling"
    assert repository.child["selected_attempt_id"] == "macba_test"
    assert adapter.collect_calls == 2
    assert adapter.terminal_calls == 2


def test_terminal_reservation_owner_race_remains_reconciling_until_retry(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path, repository)

    def owner_race(**_kwargs: Any) -> Mapping[str, Any]:
        adapter.terminal_calls += 1
        raise DurableExecutionAdapterError(
            "capacity reconciler still owns the exact terminal reservation",
            reason_code="qe_execution_reservation_owner_mismatch",
        )

    adapter.record_remote_terminal = owner_race  # type: ignore[method-assign]
    orchestrator = _orchestrator(repository, adapter)
    intent = DurableSubmissionIntent(
        run_id="macb_test",
        child_id="macbc_test",
        attempt_id="macba_test",
        attempt_no=1,
        node_id="wsl2-5080",
        qe_task_id="qe_test",
        qe_loop_id="Loop1",
        submission_intent_hash="a" * 64,
    )
    artifacts = DurablePublishedArtifacts(
        workspace=tmp_path,
        prediction_path=tmp_path / "combined_prediction.pkl",
        artifact_manifest_path=tmp_path / "artifact_manifest.json",
        artifact_manifest={"manifest_hash": "b" * 64},
    )

    asyncio.run(
        orchestrator._apply_remote_status(
            run={"id": "macb_test"},
            child=dict(repository.child),
            attempt_id="macba_test",
            token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
            intent=intent,
            artifacts=artifacts,
            remote_status="completed",
            remote_payload={"status": "completed"},
        )
    )

    assert repository.attempt["status"] == "reconciling"
    assert repository.child["status"] == "reconciling"
    assert repository.yields == 1
    assert adapter.collect_calls == 0
    assert repository.events[-1]["phase"] == (
        "terminal_reservation_reconciliation_pending"
    )


def test_completed_after_deadline_is_ingested_with_evidence(tmp_path: Path) -> None:
    repository = _Repository()
    repository.attempt["submitted_at"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    adapter = _Adapter(tmp_path, repository)
    adapter.collect_calls = 1
    orchestrator = _orchestrator(repository, adapter)
    intent = DurableSubmissionIntent(
        run_id="macb_test",
        child_id="macbc_test",
        attempt_id="macba_test",
        attempt_no=1,
        node_id="wsl2-5080",
        qe_task_id="qe_test",
        qe_loop_id="Loop1",
        submission_intent_hash="a" * 64,
    )
    artifacts = DurablePublishedArtifacts(
        workspace=tmp_path,
        prediction_path=tmp_path / "combined_prediction.pkl",
        artifact_manifest_path=tmp_path / "artifact_manifest.json",
        artifact_manifest={"manifest_hash": "b" * 64},
    )
    run = {
        "id": "macb_test",
        "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "created_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
    }

    asyncio.run(
        orchestrator._apply_remote_status(
            run=run,
            child=dict(repository.child),
            attempt_id="macba_test",
            token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
            intent=intent,
            artifacts=artifacts,
            remote_status="completed",
            remote_payload={
                "status": "completed",
                "submission_receipt": {
                    "finished_at": "2026-01-01T00:10:00Z",
                },
            },
        )
    )

    manifest = repository.attempt["result_manifest_json"]
    assert repository.attempt["status"] == "succeeded"
    assert manifest["completed_after_deadline"] is True
    assert set(manifest["execution_deadline"]) == {"scheme", "run"}
    assert manifest["execution_deadline"]["scheme"]["timestamp_source"] == (
        "submission_receipt.finished_at"
    )
    assert any(event["phase"] == "deadline_exceeded" for event in repository.events)
    repository.child["status"] = "succeeded"
    completed_children = orchestrator._completed_after_deadline_children(
        [repository.child]
    )
    assert completed_children == [
        {
            "child_id": "macbc_test",
            "child_key": repository.child["child_key"],
            "attempt_id": "macba_test",
            "execution_deadline": manifest["execution_deadline"],
        }
    ]


def test_unqualified_remote_timeout_remains_reconciling_not_failed(tmp_path: Path) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path, repository)
    orchestrator = _orchestrator(repository, adapter)
    intent = DurableSubmissionIntent(
        run_id="macb_test",
        child_id="macbc_test",
        attempt_id="macba_test",
        attempt_no=1,
        node_id="wsl2-5080",
        qe_task_id="qe_test",
        qe_loop_id="Loop1",
        submission_intent_hash="a" * 64,
    )
    artifacts = DurablePublishedArtifacts(
        workspace=tmp_path,
        prediction_path=tmp_path / "combined_prediction.pkl",
        artifact_manifest_path=tmp_path / "artifact_manifest.json",
        artifact_manifest={"manifest_hash": "b" * 64},
    )

    asyncio.run(
        orchestrator._apply_remote_status(
            run={"id": "macb_test"},
            child=dict(repository.child),
            attempt_id="macba_test",
            token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
            intent=intent,
            artifacts=artifacts,
            remote_status="timeout",
            remote_payload={"status": "timeout"},
        )
    )

    assert repository.attempt["status"] == "reconciling"
    assert repository.child["status"] == "reconciling"
    assert repository.yields == 1
    assert repository.events[-1]["phase"] == "remote_status_unmapped"


def test_completed_invalid_result_is_explicitly_failed_not_retried_forever(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path, repository)
    adapter.fail_first_reason = "multi_alpha_child_result_invalid"
    orchestrator = _orchestrator(repository, adapter)
    intent = DurableSubmissionIntent(
        run_id="macb_test",
        child_id="macbc_test",
        attempt_id="macba_test",
        attempt_no=1,
        node_id="wsl2-5080",
        qe_task_id="qe_test",
        qe_loop_id="Loop1",
        submission_intent_hash="a" * 64,
    )
    artifacts = DurablePublishedArtifacts(
        workspace=tmp_path,
        prediction_path=tmp_path / "combined_prediction.pkl",
        artifact_manifest_path=tmp_path / "artifact_manifest.json",
        artifact_manifest={"manifest_hash": "b" * 64},
    )

    asyncio.run(
        orchestrator._apply_remote_status(
            run={"id": "macb_test"},
            child=dict(repository.child),
            attempt_id="macba_test",
            token=OwnershipToken(owner_id="worker", fencing_token=1, row_version=1),
            intent=intent,
            artifacts=artifacts,
            remote_status="completed",
            remote_payload={"status": "completed"},
        )
    )

    assert repository.attempt["status"] == "failed"
    assert repository.child["status"] == "failed"
    assert repository.yields == 0
    assert repository.events[-1]["phase"] == "business_result_unavailable"


def test_stale_worker_cannot_terminalize_successor_attempt_or_child(tmp_path: Path) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path, repository)
    orchestrator = _orchestrator(repository, adapter)
    stale_token = OwnershipToken(owner_id="worker", fencing_token=1, row_version=1)
    repository.attempt["fencing_token"] = 2
    repository.attempt["row_version"] = 2

    asyncio.run(
        orchestrator._fail_attempt_from_current_owner(
            attempt_id="macba_test",
            token=stale_token,
            error=RuntimeError("late worker failure"),
            phase="dispatch_failed",
        )
    )

    assert repository.attempt["status"] == "running"
    assert repository.child["status"] == "running"
    assert repository.yields == 0
    assert repository.events[-1]["event_type"] == "error"
    assert repository.events[-1]["phase"] == "dispatch_failed"


def test_worker_retries_transient_initialization_instead_of_exiting(tmp_path: Path) -> None:
    repository = _Repository()
    adapter = _Adapter(tmp_path, repository)
    orchestrator = _orchestrator(repository, adapter)
    orchestrator._config = DurableOrchestratorConfig(
        poll_seconds=0.2,
        lease_seconds=60,
        heartbeat_seconds=5,
        items_per_pass=1,
        archive_batch_size=1,
        safety_sweep_seconds=0.01,
    )
    repository.has_due_orchestrator_work = lambda **_kwargs: True  # type: ignore[attr-defined]
    stop_event = asyncio.Event()
    initialize_calls = 0

    async def initialize() -> Mapping[str, Any]:
        nonlocal initialize_calls
        initialize_calls += 1
        if initialize_calls == 1:
            raise RuntimeError("temporary schema connection failure")
        orchestrator._activation_import_completed = True
        return {"ready": True}

    async def run_cycle() -> DurableOrchestratorCycleResult:
        stop_event.set()
        return DurableOrchestratorCycleResult(
            planned_runs=0,
            dispatched_attempts=0,
            reconciled_attempts=0,
            finalized_runs=0,
            archive_events=0,
        )

    orchestrator.initialize = initialize  # type: ignore[method-assign]
    orchestrator.run_cycle = run_cycle  # type: ignore[method-assign]

    asyncio.run(orchestrator.run_forever(stop_event))

    assert initialize_calls == 2


def test_pre_p0_2_schema_keeps_p0_1b_orchestrator_cycle_available() -> None:
    orchestrator = DurableMultiAlphaOrchestrator(
        repository=_PreP0_2Repository(),  # type: ignore[arg-type]
        planner=SimpleNamespace(),  # type: ignore[arg-type]
        adapter=SimpleNamespace(),  # type: ignore[arg-type]
        archive_capture=SimpleNamespace(),  # type: ignore[arg-type]
        active_import_service=_ActiveImport(),  # type: ignore[arg-type]
        control_service=SimpleNamespace(),  # type: ignore[arg-type]
        cancellation_delivery_worker=SimpleNamespace(),  # type: ignore[arg-type]
        recovery_worker=SimpleNamespace(),  # type: ignore[arg-type]
        owner_id="pre-p0-2-test",
    )

    health = asyncio.run(orchestrator.initialize())
    assert health["schema_health"]["ready"] is True
    assert health["p0_2_schema_health"] == {
        "ready": False,
        "reason_code": "multi_alpha_p0_2_schema_unavailable",
    }

    async def forbidden_p0_2_pass() -> int:
        raise AssertionError("P0-2 worker pass must not run before its additive schema exists")

    async def bounded_pass(_operation: Any) -> int:
        return 1

    async def attempt_pass(_operation: Any) -> int:
        return 1

    async def dispatch_pass() -> int:
        return 1

    async def finalizer_pass() -> int:
        return 1

    async def archive_pass() -> int:
        return 1

    orchestrator._run_control_pass = forbidden_p0_2_pass  # type: ignore[method-assign]
    orchestrator._run_recovery_pass = forbidden_p0_2_pass  # type: ignore[method-assign]
    orchestrator._run_cancel_delivery_pass = forbidden_p0_2_pass  # type: ignore[method-assign]
    orchestrator._run_pause_drain_pass = forbidden_p0_2_pass  # type: ignore[method-assign]
    orchestrator._run_bounded_pass = bounded_pass  # type: ignore[method-assign]
    orchestrator._run_dispatch_pass = dispatch_pass  # type: ignore[method-assign]
    orchestrator._run_attempt_pass = attempt_pass  # type: ignore[method-assign]
    orchestrator._run_finalizer_pass = finalizer_pass  # type: ignore[method-assign]
    orchestrator.archive_pass = archive_pass  # type: ignore[method-assign]

    result = asyncio.run(orchestrator.run_cycle())

    assert result == DurableOrchestratorCycleResult(
        planned_runs=1,
        dispatched_attempts=1,
        reconciled_attempts=1,
        finalized_runs=1,
        archive_events=1,
        applied_control_commands=0,
        executed_recovery_commands=0,
        delivered_cancellations=0,
        paused_runs=0,
    )
