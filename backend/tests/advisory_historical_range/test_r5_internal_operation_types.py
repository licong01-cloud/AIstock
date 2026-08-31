from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeDatasetBridgeApplicationService,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeOperationRequestV1,
    HistoricalRangeOperationType,
    HistoricalRangeOutcomeRefreshRequestV1,
    derive_prefixed_id,
)
from backend.services.advisory_historical_range.outcome_service import (
    HistoricalRangeOutcomeApplicationService,
)
from backend.services.advisory_historical_range.repository import (
    PostgresHistoricalRangeRepository,
)


class _OperationCaptured(RuntimeError):
    pass


class _CapturingRepository:
    def __init__(self) -> None:
        self.request: HistoricalRangeOperationRequestV1 | None = None

    def get_or_create_operation(
        self,
        request: HistoricalRangeOperationRequestV1,
    ) -> tuple[dict[str, object], bool]:
        self.request = request
        raise _OperationCaptured


def _outcome_request() -> HistoricalRangeOutcomeRefreshRequestV1:
    return HistoricalRangeOutcomeRefreshRequestV1.model_construct(
        batch_id="batch-1",
        operation_idempotency_key="refresh-run-1",
        request_hash="a" * 64,
        expected_batch_row_version=1,
        lease_seconds=30,
    )


def _bridge_request() -> HistoricalRangeDatasetBridgeRequestV1:
    return HistoricalRangeDatasetBridgeRequestV1.model_construct(
        batch_id="batch-1",
        operation_idempotency_key="bridge-run-1",
        request_hash="b" * 64,
        expected_batch_row_version=1,
        lease_seconds=30,
    )


def test_outcome_application_uses_distinct_run_level_operation_identity() -> None:
    repository = _CapturingRepository()
    service = HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=object(),
        planner=object(),
        evaluator=object(),
    )
    request = _outcome_request()

    with pytest.raises(_OperationCaptured):
        service.refresh_until_stable_boundary(
            request=request,
            resolved_request_hash="c" * 64,
            worker_id="worker-1",
        )

    assert repository.request is not None
    assert repository.request.operation_type is HistoricalRangeOperationType.REFRESH_OUTCOMES_RUN
    assert repository.request.operation_id == derive_prefixed_id(
        "ahrop",
        {
            "batch_id": request.batch_id,
            "operation_type": HistoricalRangeOperationType.REFRESH_OUTCOMES_RUN.value,
            "idempotency_key": request.operation_idempotency_key,
        },
    )
    assert repository.request.operation_id != derive_prefixed_id(
        "ahrop",
        {
            "batch_id": request.batch_id,
            "operation_type": HistoricalRangeOperationType.REFRESH_OUTCOMES.value,
            "idempotency_key": request.operation_idempotency_key,
        },
    )


def test_bridge_application_uses_distinct_run_level_operation_identity() -> None:
    repository = _CapturingRepository()
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=object(),
        bridge_service=object(),
    )
    request = _bridge_request()

    with pytest.raises(_OperationCaptured):
        service.build_until_stable_boundary(
            request=request,
            resolved_request_hash="d" * 64,
            worker_id="worker-1",
        )

    assert repository.request is not None
    assert repository.request.operation_type is HistoricalRangeOperationType.BUILD_DATASET_BRIDGE_RUN
    assert repository.request.operation_id == derive_prefixed_id(
        "ahrop",
        {
            "batch_id": request.batch_id,
            "operation_type": HistoricalRangeOperationType.BUILD_DATASET_BRIDGE_RUN.value,
            "idempotency_key": request.operation_idempotency_key,
        },
    )
    assert repository.request.operation_id != derive_prefixed_id(
        "ahrop",
        {
            "batch_id": request.batch_id,
            "operation_type": HistoricalRangeOperationType.BUILD_DATASET_BRIDGE.value,
            "idempotency_key": request.operation_idempotency_key,
        },
    )


@pytest.mark.parametrize(
    ("operation_type", "expected_kind"),
    (
        (
            HistoricalRangeOperationType.REFRESH_OUTCOMES,
            HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
        ),
        (
            HistoricalRangeOperationType.REFRESH_OUTCOMES_RUN,
            HistoricalRangeArtifactKind.OUTCOME_REFRESH_RECEIPT,
        ),
        (
            HistoricalRangeOperationType.BUILD_DATASET_BRIDGE,
            HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
        ),
        (
            HistoricalRangeOperationType.BUILD_DATASET_BRIDGE_RUN,
            HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
        ),
    ),
)
def test_parent_and_run_operations_share_their_authoritative_receipt_kind(
    operation_type: HistoricalRangeOperationType,
    expected_kind: HistoricalRangeArtifactKind,
) -> None:
    assert PostgresHistoricalRangeRepository._operation_receipt_kind(operation_type) is expected_kind


def test_migration_extends_operation_contract_without_weakening_active_uniqueness() -> None:
    migration_root = Path(__file__).resolve().parents[3] / "backend" / "db" / "migrations"
    forward = (migration_root / "add_advisory_historical_range_internal_operation_types_20260729.sql").read_text(
        encoding="utf-8"
    )
    rollback = (
        migration_root / "add_advisory_historical_range_internal_operation_types_20260729.rollback.sql"
    ).read_text(encoding="utf-8")

    for operation_type in ("REFRESH_OUTCOMES_RUN", "BUILD_DATASET_BRIDGE_RUN"):
        assert operation_type in forward
        assert operation_type in rollback
    assert "OUTCOME_REFRESH_RECEIPT" in forward
    assert "DATASET_BRIDGE_RECEIPT" in forward
    assert "rollback refused: internal run-level operation facts already exist" in rollback
    assert "uq_advisory_historical_range_operation_running_type" not in forward
    assert "DROP INDEX" not in forward.upper()
