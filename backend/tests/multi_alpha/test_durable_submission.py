from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import multi_alpha as multi_alpha_router
from backend.services.multi_alpha.combine_backtest import (
    MultiAlphaCombineBacktestError,
    MultiAlphaCombineBacktestService,
)
from backend.services.multi_alpha.durable_repository import (
    MultiAlphaDurableRepository,
    MultiAlphaDurableRepositoryError,
)
from backend.services.multi_alpha.durable_submission import (
    DurableCombineSubmissionError,
    DurableCombineSubmissionService,
)
from backend.services.multi_alpha.durable_identity import ExecutionIdentityResolution
from backend.services.multi_alpha.durable_runtime_health import (
    DurableOrchestratorUnavailableError,
)


FIXED_NOW = datetime(2026, 7, 19, 12, 0, tzinfo=timezone.utc)
REPO_ROOT = Path(__file__).resolve().parents[3]


class FakeP0_2SchemaHealth:
    def __init__(self, *, ready: bool, **detail: Any) -> None:
        self.ready = ready
        self._detail = {"ready": ready, **detail}

    def as_dict(self) -> dict[str, Any]:
        return deepcopy(self._detail)


def _payload(*, topk: int = 25, initial_cash: int = 10_000_000) -> dict[str, Any]:
    return {
        "roster": [
            {"leg_id": "leg_a", "seed_run_ids": ["qe_a_L1"], "metadata": {"family": "trend"}},
            {"leg_id": "leg_b", "seed_run_ids": ["qe_b_L1"], "metadata": {"family": "sector"}},
            {"leg_id": "leg_c", "seed_run_ids": ["qe_c_L1"], "metadata": {"family": "risk"}},
        ],
        "oos_start": "2024-07-01",
        "oos_end": "2026-06-29",
        "weighting_schemes": ["equal", "ic_weighted"],
        "normalize_method": "rank",
        "walk_forward": {"enabled": True, "window": 60, "min_periods": 20, "expanding": False},
        "backtest_config": {
            "node_id": "wsl2-5080",
            "node_parallelism": {"wsl2-5080": 2},
            "topk": topk,
            "initial_cash": initial_cash,
            "runtime_template_dir": "unused-by-injected-preflight",
        },
        "baseline_leg_id": "leg_a",
        "topk": topk,
        "run_async": True,
        "scheme_timeout_seconds": 120,
        "run_timeout_seconds": 600,
    }


class FakeDurableRepository:
    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.tasks_by_group: dict[str, str] = {}
        self.runs: dict[str, dict[str, Any]] = {}
        self.preflight_calls = 0

    def preflight_schema(self, *, raise_on_error: bool = False) -> Any:
        self.preflight_calls += 1
        return {"ready": True, "raise_on_error": raise_on_error}

    def preflight_p0_2_schema(self, *, raise_on_error: bool = False) -> Any:
        return FakeP0_2SchemaHealth(ready=True, raise_on_error=raise_on_error, p0_2=True)

    def get_task(self, task_id: str) -> dict[str, Any] | None:
        row = self.tasks.get(task_id)
        return deepcopy(row) if row is not None else None

    def find_task_for_implicit_group(
        self,
        *,
        legacy_group_key: str,
        roster_hash: str,
        roster: list[dict[str, Any]],
        normalize_method: str,
        walk_forward: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        task_id = self.tasks_by_group.get(legacy_group_key)
        if task_id is None:
            return None
        row = self.tasks[task_id]
        MultiAlphaDurableRepository.assert_task_compatible(
            row,
            roster_hash=roster_hash,
            roster=roster,
            normalize_method=normalize_method,
            walk_forward=walk_forward,
            legacy_group_key=legacy_group_key,
        )
        return deepcopy(row)

    def create_task(self, spec: Any) -> dict[str, Any]:
        existing_id = self.tasks_by_group.get(spec.legacy_group_key)
        if existing_id is not None:
            return deepcopy(self.tasks[existing_id])
        row = {
            "task_id": spec.task_id,
            "task_name": spec.task_name,
            "roster_hash": spec.roster_hash,
            "roster_json": [dict(item) for item in spec.roster],
            "default_request_json": deepcopy(dict(spec.default_request)),
            "legacy_group_key": spec.legacy_group_key,
            "source_kind": spec.source_kind,
        }
        self.tasks[spec.task_id] = row
        self.tasks_by_group[spec.legacy_group_key] = spec.task_id
        return deepcopy(row)

    def create_run(self, spec: Any) -> dict[str, Any]:
        row = {
            "id": spec.run_id,
            "task_id": spec.task_id,
            "request_hash": spec.request_hash,
            "roster_hash": spec.roster_hash,
            "roster_json": [dict(item) for item in spec.roster],
            "oos_start": str(spec.oos_start),
            "oos_end": str(spec.oos_end),
            "normalize_method": spec.normalize_method,
            "walk_forward_json": dict(spec.walk_forward),
            "backtest_config_json": deepcopy(dict(spec.backtest_config)),
            "baseline_leg_id": spec.baseline_leg_id,
            "retry_of_run_id": spec.retry_of_run_id,
            "node_parallelism_json": dict(spec.node_parallelism or {}),
            "recovery_kind": spec.recovery_kind,
            "recovery_scope_json": dict(spec.recovery_scope or {}),
            "recovery_scope_hash": spec.recovery_scope_hash,
            "execution_identity_json": (
                deepcopy(dict(spec.execution_identity))
                if spec.execution_identity is not None
                else None
            ),
            "execution_identity_hash": spec.execution_identity_hash,
            "execution_identity_evidence_json": (
                deepcopy(dict(spec.execution_identity_evidence))
                if spec.execution_identity_evidence is not None
                else None
            ),
            "status": "queued",
            "phase": "submitted",
            "progress_json": {},
            "reason": {"phase": "submitted", "progress": {}, "durable": True},
        }
        if spec.run_id in self.runs:
            existing = self.runs[spec.run_id]
            if existing["request_hash"] != spec.request_hash or existing["task_id"] != spec.task_id:
                raise MultiAlphaDurableRepositoryError(
                    "run identity maps to a different request",
                    reason_code="multi_alpha_identity_payload_conflict",
                )
            return deepcopy(existing)
        self.runs[spec.run_id] = row
        return deepcopy(row)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        row = self.runs.get(run_id)
        return deepcopy(row) if row is not None else None

    @staticmethod
    def assert_task_compatible(row: Mapping[str, Any], **kwargs: Any) -> None:
        MultiAlphaDurableRepository.assert_task_compatible(row, **kwargs)


class FakeTime:
    def __init__(self) -> None:
        self.value = 0.0
        self.on_sleep: Any = None

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds
        if self.on_sleep is not None:
            self.on_sleep()


class IncompleteExecutionIdentityResolver:
    def resolve(self, **_kwargs: Any) -> ExecutionIdentityResolution:
        return ExecutionIdentityResolution(
            identity=None,
            evidence={
                "schema_version": "multi_alpha_execution_identity_evidence_v1",
                "complete": False,
                "reason_code": "test_identity_evidence_incomplete",
                "missing": ["test_only"],
                "acquisition_suggestions": ["test-only evidence"],
                "observations": {},
            },
        )


def _service(
    repository: FakeDurableRepository,
    *,
    fake_time: FakeTime | None = None,
    execution_schema_preflight: Any = None,
) -> DurableCombineSubmissionService:
    time_source = fake_time or FakeTime()
    return DurableCombineSubmissionService(
        repository=repository,  # type: ignore[arg-type]
        runtime_preflight=lambda **_kwargs: None,
        execution_schema_preflight=execution_schema_preflight or (lambda: None),
        orchestrator_readiness_preflight=lambda: {"ready": True},
        clock=lambda: FIXED_NOW,
        monotonic=time_source.monotonic,
        sleep=time_source.sleep,
        execution_identity_resolver=IncompleteExecutionIdentityResolver(),
    )


def test_submission_refuses_to_queue_when_process_worker_is_not_ready() -> None:
    repository = FakeDurableRepository()
    service = DurableCombineSubmissionService(
        repository=repository,  # type: ignore[arg-type]
        runtime_preflight=lambda **_kwargs: None,
        execution_schema_preflight=lambda: None,
        orchestrator_readiness_preflight=lambda: (_ for _ in ()).throw(
            DurableOrchestratorUnavailableError(
                {"status": "failed", "ready": False, "last_error": {"reason_code": "boom"}}
            )
        ),
        clock=lambda: FIXED_NOW,
        execution_identity_resolver=IncompleteExecutionIdentityResolver(),
    )

    with pytest.raises(DurableCombineSubmissionError) as caught:
        service.submit(_payload())

    assert caught.value.reason_code == "multi_alpha_durable_orchestrator_unavailable"
    assert caught.value.http_status_code == 503
    assert repository.runs == {}


def test_task_identity_allows_distinct_run_scenarios_and_keeps_original_defaults() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)

    first = service.submit(_payload(topk=25, initial_cash=10_000_000))
    second_payload = _payload(topk=50, initial_cash=100_000_000)
    second_payload["oos_start"] = "2025-01-01"
    second_payload["baseline_leg_id"] = "leg_b"
    second = service.submit(second_payload)

    assert first["task_id"] == second["task_id"]
    assert first["run_id"] != second["run_id"]
    assert len(repository.tasks) == 1
    assert len(repository.runs) == 2
    task = repository.tasks[first["task_id"]]
    assert task["default_request_json"]["topk"] == 25
    assert task["default_request_json"]["backtest_config"]["initial_cash"] == 10_000_000
    assert repository.runs[second["run_id"]]["backtest_config_json"]["topk"] == 50


def test_submission_freezes_explicit_prediction_task_selection_in_request_identity() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)
    payload = _payload()
    payload["prediction_task_selection"] = {
        "include_baseline": True,
        "include_loo": False,
    }

    result = service.submit(payload)
    persisted = repository.runs[result["run_id"]]["backtest_config_json"]

    assert persisted["_combine_request_v1"]["prediction_task_selection"] == {
        "include_baseline": True,
        "include_loo": False,
    }


def test_run_async_override_preserves_explicit_prediction_task_selection() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)
    payload = _payload()
    payload["run_async"] = False
    payload["wait_timeout_seconds"] = 1
    payload["prediction_task_selection"] = {
        "include_baseline": True,
        "include_loo": False,
    }

    result = service.submit(payload, run_async_override=True)
    persisted = repository.runs[result["run_id"]]["backtest_config_json"]

    assert result["status"] == "queued"
    assert persisted["_combine_request_v1"]["prediction_task_selection"] == {
        "include_baseline": True,
        "include_loo": False,
    }


def test_same_payload_at_same_clock_creates_distinct_run_records() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)

    first = service.submit(_payload())
    second = service.submit(_payload())

    assert first["run_id"] != second["run_id"]
    assert first["task_id"] == second["task_id"]


def test_submission_idempotency_key_replays_same_run_and_rejects_changed_payload() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)

    first = service.submit(_payload(), idempotency_key="ui-scenario-1")
    replay = service.submit(_payload(), idempotency_key="ui-scenario-1")

    assert replay["run_id"] == first["run_id"]
    assert len(repository.runs) == 1

    changed = _payload(topk=50)
    with pytest.raises(DurableCombineSubmissionError) as caught:
        service.submit(changed, idempotency_key="ui-scenario-1")
    assert caught.value.http_status_code == 409
    assert caught.value.reason_code == "multi_alpha_identity_payload_conflict"


@pytest.mark.parametrize("key", ["", " ", "x" * 201])
def test_submission_rejects_invalid_idempotency_key_before_run_write(key: str) -> None:
    repository = FakeDurableRepository()

    with pytest.raises(DurableCombineSubmissionError) as caught:
        _service(repository).submit(_payload(), idempotency_key=key)

    assert caught.value.reason_code == "multi_alpha_submission_idempotency_key_invalid"
    assert repository.tasks == {}
    assert repository.runs == {}


def test_explicit_task_identity_mismatch_returns_conflict() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)
    first = service.submit(_payload())
    incompatible = _payload()
    incompatible["task_id"] = first["task_id"]
    incompatible["normalize_method"] = "zscore"

    with pytest.raises(DurableCombineSubmissionError) as caught:
        service.submit(incompatible)

    assert caught.value.http_status_code == 409
    assert caught.value.reason_code == "multi_alpha_identity_payload_conflict"
    assert len(repository.runs) == 1


def test_invalid_wait_timeout_is_rejected_before_task_or_run_write() -> None:
    repository = FakeDurableRepository()
    service = _service(repository)
    payload = _payload()
    payload["run_async"] = False
    payload["wait_timeout_seconds"] = 0

    with pytest.raises(DurableCombineSubmissionError) as caught:
        service.submit(payload)

    assert caught.value.reason_code == "multi_alpha_wait_timeout_invalid"
    assert repository.tasks == {}
    assert repository.runs == {}


def test_sync_wait_timeout_returns_current_state_without_cancelling_run() -> None:
    repository = FakeDurableRepository()
    fake_time = FakeTime()
    service = _service(repository, fake_time=fake_time)
    payload = _payload()
    payload["run_async"] = False
    payload["wait_timeout_seconds"] = 1

    result = service.submit(payload)
    run = repository.runs[result["run_id"]]

    assert result["wait_timed_out"] is True
    assert result["status"] == "queued"
    assert run["status"] == "queued"
    assert run["phase"] == "submitted"


def test_sync_wait_returns_terminal_payload_when_worker_finishes() -> None:
    repository = FakeDurableRepository()
    fake_time = FakeTime()

    def finish_run() -> None:
        if repository.runs:
            row = next(iter(repository.runs.values()))
            row["status"] = "succeeded"
            row["phase"] = "finalized"
            row["progress_json"] = {"completed_children": 9}

    fake_time.on_sleep = finish_run
    service = _service(repository, fake_time=fake_time)
    payload = _payload()
    payload["run_async"] = False
    payload["wait_timeout_seconds"] = 5

    result = service.submit(payload)

    assert result["wait_timed_out"] is False
    assert result["status"] == "succeeded"
    assert result["phase"] == "finalized"
    assert result["progress"] == {"completed_children": 9}


def test_schema_unavailable_is_explicit_and_has_no_legacy_fallback() -> None:
    class UnavailableRepository(FakeDurableRepository):
        def preflight_schema(self, *, raise_on_error: bool = False) -> Any:
            raise MultiAlphaDurableRepositoryError(
                "schema unavailable",
                reason_code="multi_alpha_schema_unavailable",
                context={"raise_on_error": raise_on_error},
            )

    with pytest.raises(DurableCombineSubmissionError) as caught:
        _service(UnavailableRepository()).submit(_payload())

    assert caught.value.http_status_code == 503
    assert caught.value.reason_code == "multi_alpha_durable_schema_unavailable"


def test_p0_2_schema_unavailable_keeps_p0_1b_submission_working_with_explicit_evidence() -> None:
    class P0_2UnavailableRepository(FakeDurableRepository):
        def preflight_p0_2_schema(self, *, raise_on_error: bool = False) -> Any:
            assert raise_on_error is False
            return FakeP0_2SchemaHealth(
                ready=False,
                missing_tables=["multi_alpha_combine_backtest_command"],
            )

    repository = P0_2UnavailableRepository()
    result = _service(repository).submit(_payload())
    stored = repository.runs[result["run_id"]]

    assert result["status"] == "queued"
    assert result["execution_identity_persisted"] is False
    assert result["execution_identity_evidence"]["reason_code"] == "multi_alpha_p0_2_schema_unavailable"
    assert stored["execution_identity_json"] is None
    assert stored["execution_identity_evidence_json"] is None


def test_execution_reservation_schema_unavailable_returns_503_before_run_write() -> None:
    from backend.services.quantevolver.qe_execution_reservation import (
        QEExecutionReservationError,
    )

    repository = FakeDurableRepository()

    def unavailable() -> None:
        raise QEExecutionReservationError(
            "reservation schema unavailable",
            reason_code="qe_execution_reservation_schema_unavailable",
            context={"missing_tables": ["infra.qe_execution_reservation"]},
        )

    with pytest.raises(DurableCombineSubmissionError) as caught:
        _service(
            repository,
            execution_schema_preflight=unavailable,
        ).submit(_payload())

    assert caught.value.http_status_code == 503
    assert caught.value.reason_code == "multi_alpha_durable_execution_schema_unavailable"
    assert repository.tasks == {}
    assert repository.runs == {}


def test_default_facade_delegates_submit_without_starting_legacy_execution() -> None:
    class CapturingDurableSubmission:
        def __init__(self) -> None:
            self.calls: list[tuple[Mapping[str, Any], bool | None, str | None]] = []

        def submit(
            self,
            payload: Mapping[str, Any],
            *,
            run_async_override: bool | None = None,
            idempotency_key: str | None = None,
        ) -> dict[str, Any]:
            self.calls.append((payload, run_async_override, idempotency_key))
            return {
                "task_id": "mact_test",
                "run_id": "macb_test",
                "status": "queued",
                "phase": "submitted",
                "progress": {},
                "durable": True,
            }

    durable = CapturingDurableSubmission()
    facade = MultiAlphaCombineBacktestService(durable_submission_service=durable)

    result = facade.submit_run(_payload(), run_async=True, idempotency_key="ui-scenario")

    assert result["durable"] is True
    assert result["status"] == "queued"
    assert len(durable.calls) == 1
    assert durable.calls[0][1:] == (True, "ui-scenario")


def test_production_facade_requires_explicit_test_flag_for_legacy_mode() -> None:
    source = (REPO_ROOT / "backend/services/multi_alpha/combine_backtest.py").read_text(encoding="utf-8")
    submit_source = source.split("def submit_run", maxsplit=1)[1].split(
        "def _execute_run_thread", maxsplit=1
    )[0]

    assert "legacy_execution_mode_for_tests: bool = False" in source
    assert "if not self._legacy_execution_mode_for_tests" in submit_source
    assert "return self._durable_submission_service.submit" in submit_source
    assert submit_source.index("return self._durable_submission_service.submit") < submit_source.index(
        "threading.Thread"
    )


def test_submit_api_returns_202_for_bounded_wait_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    class TimedOutService:
        def submit_run(self, _payload: Mapping[str, Any], **_kwargs: Any) -> dict[str, Any]:
            return {
                "task_id": "mact_test",
                "run_id": "macb_test",
                "status": "queued",
                "phase": "submitted",
                "progress": {},
                "durable": True,
                "wait_timed_out": True,
            }

    monkeypatch.setattr(multi_alpha_router, "MultiAlphaCombineBacktestService", TimedOutService)
    app = FastAPI()
    app.include_router(multi_alpha_router.router)
    client = TestClient(app)
    payload = _payload()
    payload["run_async"] = False
    payload["wait_timeout_seconds"] = 1

    response = client.post("/multi-alpha/combine-backtest/run", json=payload)

    assert response.status_code == 202
    assert response.json()["data"]["wait_timed_out"] is True
    schema = client.get("/openapi.json").json()
    request_schema = schema["components"]["schemas"]["CombineBacktestRunRequest"]
    assert "task_id" in request_schema["properties"]
    assert "wait_timeout_seconds" in request_schema["properties"]


def test_submit_api_preserves_structured_durable_error_status(monkeypatch: pytest.MonkeyPatch) -> None:
    class UnavailableService:
        def submit_run(self, _payload: Mapping[str, Any], **_kwargs: Any) -> dict[str, Any]:
            raise DurableCombineSubmissionError(
                "schema unavailable",
                reason_code="multi_alpha_durable_schema_unavailable",
                http_status_code=503,
            )

    monkeypatch.setattr(multi_alpha_router, "MultiAlphaCombineBacktestService", UnavailableService)
    app = FastAPI()
    app.include_router(multi_alpha_router.router)
    client = TestClient(app)

    response = client.post("/multi-alpha/combine-backtest/run", json=_payload())

    assert response.status_code == 503
    assert response.json()["detail"]["reason_code"] == "multi_alpha_durable_schema_unavailable"


def test_archive_detail_api_reads_immutable_archive_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    class ArchiveDetailService:
        def get_archive_snapshot(self, run_id: str) -> dict[str, Any]:
            if run_id == "missing":
                raise MultiAlphaCombineBacktestError(
                    "archived run was not found",
                    reason_code="combine_backtest_archive_snapshot_not_found",
                )
            return {
                "run": {"run_id": run_id, "archive_schema_version": "multi_alpha_combine_completed_v2"},
                "recovery_readback_evidence": {
                    "archive_snapshot_authoritative": True,
                    "source_durable_run_required": False,
                },
            }

    monkeypatch.setattr(multi_alpha_router, "MultiAlphaCombineBacktestService", ArchiveDetailService)
    app = FastAPI()
    app.include_router(multi_alpha_router.router)
    client = TestClient(app)

    response = client.get("/multi-alpha/combine-backtest/runs/macb_archive_1/archive-detail")

    assert response.status_code == 200
    assert response.json()["data"]["run"]["run_id"] == "macb_archive_1"
    assert response.json()["data"]["recovery_readback_evidence"]["source_durable_run_required"] is False

    missing = client.get("/multi-alpha/combine-backtest/runs/missing/archive-detail")

    assert missing.status_code == 404
    assert missing.json()["detail"]["reason_code"] == "combine_backtest_archive_snapshot_not_found"
