from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg2
import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeContractError,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOperationStatus,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOutcomeWorkItemV1,
    REASON_DATABASE_UNAVAILABLE,
    REASON_REPOSITORY_CONFLICT,
)
from backend.services.advisory_historical_range.outcome_projection import ExecutablePathOutcomeEngine
from backend.services.advisory_historical_range.outcome_service import (
    HistoricalRangeOutcomeApplicationService,
    HistoricalRangeOutcomeSliceV1,
)
from backend.services.advisory_historical_range.outcome_source import HistoricalRangeOutcomeSourceError
from backend.services.advisory_phase1.label_policy import Projection
from backend.tests.advisory_phase1.test_outcome_engine import _request


def _ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.OUTCOME: "outcomes",
        HistoricalRangeArtifactKind.SUMMARY: "summaries",
    }[kind]
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{namespace}/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _work_item(
    *,
    subject_id: str = "candidate-1",
    subject_ref_char: str = "a",
    range_run_id: str = "run-1",
) -> HistoricalRangeOutcomeWorkItemV1:
    source_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, "b")
    return HistoricalRangeOutcomeWorkItemV1(
        range_run_id=range_run_id,
        subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
        subject_id=subject_id,
        subject_ref=_ref(
            HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT,
            subject_ref_char,
        ),
        policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "c"),
        projection=HistoricalRangeOutcomeProjection.EXECUTABLE,
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=1,
        policy_bundle_hash="c" * 64,
        decision_trade_date=date(2026, 7, 3),
        intended_entry_trade_date=date(2026, 7, 4),
        earliest_sell_trade_date=date(2026, 7, 5),
        exit_trade_date=date(2026, 7, 5),
        label_as_of_trade_date=date(2026, 7, 10),
        source_revision_refs=(source_ref,),
        source_revision_set_hash=canonical_json_sha256([source_ref.model_dump(mode="json")]),
        producer_code_hash="d" * 64,
        outcome_contract_version="r4_v1",
        revision_reason=HistoricalRangeOutcomeRevisionReason.INITIAL,
    )


def _request_contract() -> HistoricalRangeOutcomeRefreshRequestV1:
    return HistoricalRangeOutcomeRefreshRequestV1(
        batch_id="batch-1",
        range_run_ids=("run-1",),
        label_as_of_trade_date=date(2026, 7, 10),
        policy_bundle_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "e"),
        policy_bundle_hash="e" * 64,
        requested_subject_types=(HistoricalRangeOutcomeSubjectType.CANDIDATE,),
        requested_projections=(HistoricalRangeOutcomeProjection.EXECUTABLE,),
        horizons=(1,),
        producer_code_hash="d" * 64,
        outcome_contract_version="r4_v1",
        operation_idempotency_key="refresh-1",
        expected_batch_row_version=1,
        max_items_per_slice=1,
        max_parallel_runs=1,
        lease_seconds=30,
    )


class _Repository:
    def __init__(self) -> None:
        self.operation = None
        self.outcomes = {}
        self.append_count = 0
        self.attempt_receipt_refs = []
        self.expired_attempts = []

    def get_or_create_operation(self, request):
        if self.operation is not None:
            return dict(self.operation), True
        self.operation = {
            "operation_id": request.operation_id,
            "batch_id": request.batch_id,
            "operation_type": request.operation_type.value,
            "operation_idempotency_key": request.operation_idempotency_key,
            "status": "QUEUED",
            "row_version": 1,
            "attempt_no": 0,
            "fencing_token": None,
            "lease_expires_at": None,
            "stable_keyset_cursor_json": None,
            "result_ref": None,
        }
        return dict(self.operation), False

    def transition_operation(self, **kwargs):
        assert kwargs["expected_row_version"] == self.operation["row_version"]
        self.operation["row_version"] += 1
        self.operation["status"] = kwargs["target_status"].value
        self.operation["attempt_no"] = kwargs["attempt_no"]
        self.operation["fencing_token"] = kwargs.get("fencing_token")
        if kwargs.get("replace_stable_keyset_cursor"):
            self.operation["stable_keyset_cursor_json"] = kwargs.get("stable_keyset_cursor_json")
        elif kwargs.get("stable_keyset_cursor_json") is not None:
            self.operation["stable_keyset_cursor_json"] = kwargs["stable_keyset_cursor_json"]
        if kwargs["target_status"] is HistoricalRangeOperationStatus.RUNNING:
            self.operation["worker_id"] = kwargs["worker_id"]
            self.operation["lease_token"] = kwargs["lease_token"]
            self.operation["lease_expires_at"] = kwargs["lease_expires_at"]
            self.operation["result_ref"] = None
        else:
            self.operation["worker_id"] = None
            self.operation["lease_token"] = None
        if kwargs.get("result_ref") is not None:
            self.operation["result_ref"] = kwargs["result_ref"].model_dump(mode="json")
        attempt = kwargs.get("attempt")
        if attempt is not None:
            self.attempt_receipt_refs.append(attempt.attempt_receipt_ref)
        expired_attempt = kwargs.get("expired_attempt")
        if expired_attempt is not None:
            self.expired_attempts.append(expired_attempt)
            self.attempt_receipt_refs.append(expired_attempt.attempt_receipt_ref)
        return dict(self.operation)

    def list_operation_attempt_receipt_refs(self, *, operation_id):
        assert operation_id == self.operation["operation_id"]
        return tuple(self.attempt_receipt_refs)

    def find_outcome_by_input(self, *, outcome_logical_id, outcome_input_hash):
        item = self.outcomes.get(outcome_logical_id)
        return item if item is not None and item.outcome_input_hash == outcome_input_hash else None

    def load_latest_outcome(self, *, outcome_logical_id):
        return self.outcomes.get(outcome_logical_id)

    def append_outcome(self, fact):
        self.outcomes[fact.outcome_logical_id] = fact
        self.append_count += 1
        return False

    def append_outcomes(self, facts):
        return tuple(self.append_outcome(fact) for fact in facts)


class _Planner:
    def __init__(self, item):
        self.items = item if isinstance(item, tuple) else (item,)
        self.calls = 0

    def plan_slice(self, **kwargs):
        self.calls += 1
        return HistoricalRangeOutcomeSliceV1(items=self.items, exhausted=True)


class _Evaluator:
    def evaluate(self, work_item):
        request = _request(Projection.RETURN_GROSS)
        return ExecutablePathOutcomeEngine().calculate(
            requests={Projection.RETURN_GROSS: request},
            timeline=None,
            evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
            horizon_trade_days=1,
        )


class _WaitingEvaluator:
    def evaluate(self, work_item):
        raise HistoricalRangeOutcomeSourceError(
            "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",
            "source is not available yet",
        )


class _DatabaseUnavailableEvaluator:
    def evaluate(self, work_item):
        raise psycopg2.OperationalError("database connection dropped")


class _TypedDatabaseUnavailableEvaluator:
    def evaluate(self, work_item):
        raise HistoricalRangeContractError(
            REASON_DATABASE_UNAVAILABLE,
            "database connection dropped",
        )


class _RecoveringEvaluator:
    def __init__(self) -> None:
        self.calls = 0

    def evaluate(self, work_item):
        self.calls += 1
        if self.calls == 1:
            raise HistoricalRangeOutcomeSourceError(
                "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",
                "source is not available yet",
            )
        return _Evaluator().evaluate(work_item)


class _SecondItemRecoveringEvaluator:
    def __init__(self) -> None:
        self.second_item_calls = 0

    def evaluate(self, work_item):
        if work_item.subject_id == "candidate-2":
            self.second_item_calls += 1
            if self.second_item_calls == 1:
                raise HistoricalRangeOutcomeSourceError(
                    "ADVISORY_HR_OUTCOME_SOURCE_UNAVAILABLE",
                    "second source is not available yet",
                )
        return _Evaluator().evaluate(work_item)


class _PartiallyFailingSummaryCoordinator:
    def __init__(self) -> None:
        self.calls = []

    def refresh(self, *, range_run_id):
        self.calls.append(range_run_id)
        if range_run_id == "run-2":
            raise psycopg2.OperationalError("summary database connection dropped")
        return _ref(HistoricalRangeArtifactKind.SUMMARY, "9")


def test_refresh_persists_outcome_and_exact_retry_returns_existing_receipt(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    repository = _Repository()
    planner = _Planner(_work_item())
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=root),
        planner=planner,
        evaluator=_Evaluator(),
    )
    first, first_ref = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )
    second, second_ref = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-2",
    )
    assert first.status == second.status == "COMPLETED"
    assert first_ref == second_ref
    assert repository.append_count == 1
    assert planner.calls == 1


def test_production_composition_adapts_keyword_only_latest_outcome(tmp_path: Path) -> None:
    from backend.services.advisory_historical_range.composition import (
        build_historical_range_outcome_application_service,
    )

    service = build_historical_range_outcome_application_service(
        conn_factory=lambda: None,
        artifact_root=tmp_path / "artifacts",
        policy_provider=object(),
        producer_code_hash="d" * 64,
        outcome_contract_version="r4_v1",
        subject_input_provider=object(),
    )
    calls = []
    service._repository.load_latest_outcome = lambda *, outcome_logical_id: calls.append(outcome_logical_id) or None

    assert service._planner._latest_outcome("logical-1") is None
    assert calls == ["logical-1"]


def test_source_not_arrived_waits_without_writing_failed_outcome(tmp_path: Path) -> None:
    root = tmp_path / "artifacts"
    root.mkdir()
    repository = _Repository()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=root),
        planner=_Planner(_work_item()),
        evaluator=_WaitingEvaluator(),
    )
    receipt, _ = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )
    assert receipt.status == "WAITING_INPUT"
    assert repository.append_count == 0
    assert repository.operation["status"] == "WAITING_INPUT"


def test_waiting_input_operation_resumes_after_source_arrival(tmp_path: Path) -> None:
    repository = _Repository()
    evaluator = _RecoveringEvaluator()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(_work_item()),
        evaluator=evaluator,
    )

    waiting, _ = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )
    completed, _ = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-2",
    )

    assert waiting.status == "WAITING_INPUT"
    assert waiting.stable_keyset_cursor is None
    assert completed.status == "COMPLETED"
    assert evaluator.calls == 2
    assert repository.append_count == 1
    assert repository.operation["status"] == "COMPLETED"


def test_resume_merges_prior_attempt_outputs_without_double_counting(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    evaluator = _SecondItemRecoveringEvaluator()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(
            (
                _work_item(),
                _work_item(subject_id="candidate-2", subject_ref_char="f"),
            )
        ),
        evaluator=evaluator,
    )

    waiting, waiting_ref = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )
    completed, completed_ref = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-2",
    )

    assert waiting.status == "WAITING_INPUT"
    assert waiting.processed_count == 1
    assert len(waiting.outcome_refs) == 1
    assert completed.status == "COMPLETED"
    assert completed.processed_count == 2
    assert len(completed.outcome_refs) == 2
    assert len({ref.semantic_content_hash for ref in completed.outcome_refs}) == 2
    assert waiting.outcome_refs[0] in completed.outcome_refs
    assert repository.append_count == 2
    assert evaluator.second_item_calls == 2
    assert repository.attempt_receipt_refs == [waiting_ref, completed_ref]


def test_refresh_rejects_takeover_while_running_lease_is_active(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(_work_item()),
        evaluator=_WaitingEvaluator(),
    )
    service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )
    repository.operation.update(
        {
            "status": HistoricalRangeOperationStatus.RUNNING.value,
            "attempt_no": 2,
            "fencing_token": 2,
            "worker_id": "active-worker",
            "lease_token": "active-token",
            "lease_expires_at": datetime.now(UTC) + timedelta(seconds=30),
            "result_ref": None,
        }
    )

    with pytest.raises(HistoricalRangeContractError) as exc_info:
        service.refresh_until_stable_boundary(
            request=_request_contract(),
            resolved_request_hash="f" * 64,
            worker_id="worker-2",
        )

    assert exc_info.value.reason_code == REASON_REPOSITORY_CONFLICT
    assert repository.expired_attempts == []


def test_refresh_takes_over_expired_lease_and_replays_from_last_receipt_cursor(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    evaluator = _SecondItemRecoveringEvaluator()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(
            (
                _work_item(),
                _work_item(subject_id="candidate-2", subject_ref_char="f"),
            )
        ),
        evaluator=evaluator,
    )
    waiting, waiting_ref = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )
    repository.operation.update(
        {
            "status": HistoricalRangeOperationStatus.RUNNING.value,
            "attempt_no": 2,
            "fencing_token": 2,
            "worker_id": "expired-worker",
            "lease_token": "expired-token",
            "lease_expires_at": datetime.now(UTC) - timedelta(seconds=1),
            "stable_keyset_cursor_json": {"position": 99},
            "result_ref": None,
        }
    )

    completed, completed_ref = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-2",
    )

    assert waiting.processed_count == 1
    assert completed.status == "COMPLETED"
    assert completed.processed_count == 2
    assert len(completed.outcome_refs) == 2
    assert len({ref.semantic_content_hash for ref in completed.outcome_refs}) == 2
    assert waiting.outcome_refs[0] in completed.outcome_refs
    assert repository.append_count == 2
    assert len(repository.expired_attempts) == 1
    assert repository.expired_attempts[0].attempt_no == 2
    assert repository.expired_attempts[0].status == "RETRYABLE_FAILED"
    assert repository.expired_attempts[0].result_cursor_json is None
    assert repository.operation["stable_keyset_cursor_json"] is None
    assert repository.attempt_receipt_refs[0] == waiting_ref
    assert repository.attempt_receipt_refs[-1] == completed_ref


def test_database_unavailable_is_retryable_without_failed_outcome(tmp_path: Path) -> None:
    repository = _Repository()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(_work_item()),
        evaluator=_DatabaseUnavailableEvaluator(),
    )

    receipt, _ = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )

    assert receipt.status == "RETRYABLE_FAILED"
    assert receipt.reason_codes == (REASON_DATABASE_UNAVAILABLE,)
    assert repository.append_count == 0
    assert repository.operation["status"] == "RETRYABLE_FAILED"


def test_typed_database_unavailable_is_retryable_without_failed_outcome(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(_work_item()),
        evaluator=_TypedDatabaseUnavailableEvaluator(),
    )

    receipt, _ = service.refresh_until_stable_boundary(
        request=_request_contract(),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )

    assert receipt.status == "RETRYABLE_FAILED"
    assert receipt.reason_codes == (REASON_DATABASE_UNAVAILABLE,)
    assert repository.append_count == 0
    assert repository.operation["status"] == "RETRYABLE_FAILED"


def test_retryable_receipt_preserves_summaries_published_before_failure(
    tmp_path: Path,
) -> None:
    repository = _Repository()
    coordinator = _PartiallyFailingSummaryCoordinator()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=HistoricalRangeArtifactStore(root=tmp_path / "artifacts"),
        planner=_Planner(
            (
                _work_item(),
                _work_item(
                    subject_id="candidate-2",
                    subject_ref_char="f",
                    range_run_id="run-2",
                ),
            )
        ),
        evaluator=_Evaluator(),
        summary_coordinator=coordinator,
    )

    request_payload = _request_contract().model_dump(
        mode="python",
        exclude={"request_hash"},
    )
    request_payload["range_run_ids"] = ("run-1", "run-2")
    receipt, _ = service.refresh_until_stable_boundary(
        request=HistoricalRangeOutcomeRefreshRequestV1.model_validate(request_payload),
        resolved_request_hash="f" * 64,
        worker_id="worker-1",
    )

    assert receipt.status == "RETRYABLE_FAILED"
    assert receipt.processed_count == 2
    assert receipt.summary_refs == (_ref(HistoricalRangeArtifactKind.SUMMARY, "9"),)
    assert coordinator.calls == ["run-1", "run-2"]
