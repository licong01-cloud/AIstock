from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Lock
from types import SimpleNamespace
from uuid import uuid4

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.api_models import (
    HistoricalRangeBuildBridgeRequest,
    HistoricalRangeCommandRequest,
    HistoricalRangeRefreshOutcomesRequest,
)
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeDatasetBridgeArtifactV1,
    HistoricalRangeDatasetBridgeApplicationService,
)
from backend.services.advisory_historical_range.models import (
    REASON_DATASET_BRIDGE_VALID_EMPTY,
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeDatasetBridgeReceiptV1,
    HistoricalRangeOperationAttemptV1,
    HistoricalRangeOperationStatus,
    HistoricalRangeOperationType,
    HistoricalRangeOutcomeStatus,
)
from backend.services.advisory_historical_range.service import (
    HistoricalRangeApplicationService,
    HistoricalRangeRuntime,
)
from backend.services.advisory_historical_range.repository import (
    PostgresHistoricalRangeRepository,
)
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
)


class _Query:
    def get_batch(self, batch_id: str):
        return {"batch_id": batch_id, "row_version": 7, "request_payload_sha256": "a" * 64}

    def get_operation(self, operation_id: str):
        return {
            "operation_id": operation_id,
            "batch_id": "ahrb_1",
            "status": HistoricalRangeOperationStatus.QUEUED.value,
            "row_version": 1,
            "lease_expired": False,
        }


class _Repository:
    def __init__(self, *, exact_retry: bool = False) -> None:
        self.requests = []
        self.exact_retry = exact_retry

    def get_or_create_operation(self, request):
        self.requests.append(request)
        return ({"operation_id": request.operation_id}, self.exact_retry)


class _Background:
    def __init__(self) -> None:
        self.tasks = []

    def add_task(self, func, *args, **kwargs):
        self.tasks.append((func, args, kwargs))


def _runtime(repository: _Repository) -> HistoricalRangeRuntime:
    return HistoricalRangeRuntime(
        query=_Query(),  # type: ignore[arg-type]
        repository=repository,  # type: ignore[arg-type]
        planning=SimpleNamespace(),
        execution=SimpleNamespace(),
        outcome=SimpleNamespace(),
        bridge=SimpleNamespace(),
        outcome_requests=SimpleNamespace(build=lambda *_args: SimpleNamespace(request_hash="b" * 64)),
        bridge_requests=SimpleNamespace(build=lambda *_args: SimpleNamespace(request_hash="c" * 64)),
        options_projector=lambda: {},
    )


def test_resume_persists_exact_operation_before_background_dispatch() -> None:
    repository = _Repository()
    service = HistoricalRangeApplicationService(runtime_factory=lambda: _runtime(repository))
    background = _Background()
    response = service.resume_batch(
        "ahrb_1",
        HistoricalRangeCommandRequest(operation_idempotency_key="resume-key", expected_row_version=7),
        background_tasks=background,
    )
    assert len(repository.requests) == 1
    persisted = repository.requests[0]
    assert persisted.operation_type.value == "RESUME"
    assert persisted.expected_row_version == 7
    assert response["data"]["operation_id"] == persisted.operation_id
    assert response["data"]["links"]["operation"].endswith(persisted.operation_id)
    assert len(background.tasks) == 1


def test_resume_returns_repository_exact_retry_without_duplicate_dispatch_identity() -> None:
    repository = _Repository(exact_retry=True)
    service = HistoricalRangeApplicationService(runtime_factory=lambda: _runtime(repository))
    response = service.resume_batch(
        "ahrb_1",
        HistoricalRangeCommandRequest(operation_idempotency_key="resume-key", expected_row_version=7),
        background_tasks=_Background(),
    )
    assert response["data"]["exact_retry"] is True
    assert len(repository.requests) == 1


def test_planning_resume_enforces_expected_row_version_before_dispatch() -> None:
    repository = _Repository()
    runtime = _runtime(repository)
    runtime.query.get_batch = lambda batch_id: {  # type: ignore[method-assign]
        "batch_id": batch_id,
        "row_version": 8,
        "request_payload_sha256": None,
        "catalog_operation_id": "ahrop_catalog",
    }
    service = HistoricalRangeApplicationService(runtime_factory=lambda: runtime)
    try:
        service.resume_batch(
            "ahrb_1",
            HistoricalRangeCommandRequest(operation_idempotency_key="resume-key", expected_row_version=7),
            background_tasks=_Background(),
        )
    except Exception as exc:
        assert getattr(exc, "reason_code", None) == "ADVISORY_HR_OPERATION_BATCH_VERSION_CONFLICT"
        assert getattr(exc, "http_status", None) == 409
    else:
        raise AssertionError("stale PLANNING resume must fail")


def test_cancel_refresh_and_bridge_return_repository_exact_retry() -> None:
    repository = _Repository(exact_retry=True)
    service = HistoricalRangeApplicationService(runtime_factory=lambda: _runtime(repository))
    cancel = service.cancel_batch(
        "ahrb_1",
        HistoricalRangeCommandRequest(operation_idempotency_key="cancel-key", expected_row_version=7),
        background_tasks=_Background(),
    )
    refresh = service.refresh_outcomes(
        "ahrb_1",
        HistoricalRangeRefreshOutcomesRequest(
            operation_idempotency_key="refresh-key",
            expected_row_version=7,
            label_as_of_trade_date="2026-07-24",
            horizons=[1],
        ),
        background_tasks=_Background(),
    )
    bridge = service.build_dataset_bridge(
        "ahrb_1",
        HistoricalRangeBuildBridgeRequest(
            operation_idempotency_key="bridge-key",
            expected_row_version=7,
            requested_horizons=[1],
            requested_maturity_statuses=["COMPLETE"],
        ),
        background_tasks=_Background(),
    )
    assert [cancel["data"]["exact_retry"], refresh["data"]["exact_retry"], bridge["data"]["exact_retry"]] == [True, True, True]


class _ConcurrentRepository(_Repository):
    def __init__(self) -> None:
        super().__init__()
        self._lock = Lock()
        self._operation_ids: set[str] = set()

    def get_or_create_operation(self, request):
        with self._lock:
            exact_retry = request.operation_id in self._operation_ids
            self._operation_ids.add(request.operation_id)
            self.requests.append(request)
        return ({"operation_id": request.operation_id}, exact_retry)


def test_concurrent_resume_retries_share_one_operation_identity() -> None:
    repository = _ConcurrentRepository()
    service = HistoricalRangeApplicationService(runtime_factory=lambda: _runtime(repository))

    def invoke(_index: int):
        return service.resume_batch(
            "ahrb_1",
            HistoricalRangeCommandRequest(
                operation_idempotency_key="concurrent-resume-key",
                expected_row_version=7,
            ),
            background_tasks=_Background(),
        )["data"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(invoke, range(8)))
    assert len({item["operation_id"] for item in results}) == 1
    assert sum(item["exact_retry"] is False for item in results) == 1
    assert sum(item["exact_retry"] is True for item in results) == 7


def _bridge_ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    return HistoricalRangeArtifactRefV1(
        artifact_kind=kind,
        relative_path=f"{kind.value.lower()}/{digest}.json",
        producer_contract_version="test_v1",
        payload_schema_version="test_v1",
        semantic_content_hash=digest,
        payload_sha256=digest,
        file_sha256=digest,
    )


def _bridge_request(
    *,
    day_ref: HistoricalRangeArtifactRefV1,
    policy_ref: HistoricalRangeArtifactRefV1,
) -> HistoricalRangeDatasetBridgeRequestV1:
    components = {
        role: char * 64
        for role, char in zip(
            (
                "BARRIER",
                "BENCHMARK",
                "CALENDAR",
                "CASH_RETURN",
                "CORPORATE_ACTION",
                "COST",
                "EXECUTION",
                "MARKET_DATA",
                "TERMINAL",
            ),
            "123456789",
            strict=True,
        )
    }
    return HistoricalRangeDatasetBridgeRequestV1(
        batch_id="ahrb_1",
        range_run_ids=("ahrr_1",),
        successful_day_refs=(day_ref,),
        candidate_refs=(),
        outcome_refs=(),
        requested_horizons=(1,),
        requested_maturity_statuses=(HistoricalRangeOutcomeStatus.COMPLETE,),
        policy_bundle_refs=(policy_ref,),
        policy_component_hashes={policy_ref.payload_sha256: components},
        canonical_signal_dedup_policy_hash="3" * 64,
        retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        dataset_schema_hash="4" * 64,
        builder_hash="5" * 64,
        writer_hash="6" * 64,
        partition_policy_hash="7" * 64,
        compression_config_hash="8" * 64,
        artifact_root_identity_hash="9" * 64,
        operation_idempotency_key="bridge-command-key",
        expected_batch_row_version=7,
    )


def test_bridge_parent_receipt_preserves_child_and_artifact_lineage(tmp_path) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "bridge-command-artifacts")
    day_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DAY_RECEIPT,
        producer_contract_version="test_v1",
        payload_schema_version="test_day_receipt_v1",
        resolved_request_hash="a" * 64,
        payload={"schema_version": "test_day_receipt_v1", "ordinal": 1},
        range_run_id="ahrr_1",
        day_run_id="ahrd_1",
    ).ref
    policy_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="test_v1",
        payload_schema_version="test_policy_bundle_v1",
        resolved_request_hash="a" * 64,
        payload={"schema_version": "test_policy_bundle_v1", "policy": "test"},
    ).ref
    request = _bridge_request(day_ref=day_ref, policy_ref=policy_ref)
    child_bridge = HistoricalRangeDatasetBridgeArtifactV1(
        operation_id="ahrop_child",
        request=request,
        request_hash=str(request.request_hash),
        result_status=HistoricalRangeBridgeResultStatus.VALID_EMPTY,
        selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        producer_code_hash="a" * 64,
    )
    bridge_upstream = tuple(
        (
            *request.successful_day_refs,
            *request.candidate_refs,
            *request.outcome_refs,
            *request.summary_refs,
            *request.policy_bundle_refs,
        )
    )
    bridge_artifact = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE,
        producer_contract_version="advisory_phase1r_r4_dataset_bridge_v1",
        payload_schema_version="advisory_historical_range_dataset_bridge_artifact_v1",
        resolved_request_hash="a" * 64,
        payload=child_bridge.model_dump(mode="json"),
        upstream_refs=bridge_upstream,
    ).ref
    child_receipt = HistoricalRangeDatasetBridgeReceiptV1(
        operation_id="ahrop_child",
        request_hash=str(request.request_hash),
        result_status=HistoricalRangeBridgeResultStatus.VALID_EMPTY,
        observation_count=0,
        label_count=0,
        canonical_signal_count=0,
        range_lineage_count=0,
        retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        bridge_artifact_ref=bridge_artifact,
        reason_codes=(REASON_DATASET_BRIDGE_VALID_EMPTY,),
    )
    child_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
        producer_contract_version="advisory_phase1r_r4_dataset_bridge_v1",
        payload_schema_version=child_receipt.schema_version,
        resolved_request_hash="a" * 64,
        payload=child_receipt.model_dump(mode="json"),
        upstream_refs=(bridge_artifact,),
    ).ref
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=SimpleNamespace(),
        artifact_store=store,
        bridge_service=SimpleNamespace(),
    )

    parent_receipt, parent_ref = service.publish_parent_receipt(
        operation_id="ahrop_parent",
        child_receipt=child_receipt,
        child_receipt_ref=child_ref,
        resolved_request_hash="a" * 64,
    )

    assert child_receipt.operation_id == "ahrop_child"
    assert parent_receipt.operation_id == "ahrop_parent"
    assert parent_receipt.result_status is HistoricalRangeBridgeResultStatus.VALID_EMPTY
    envelope = store.load(parent_ref)
    assert HistoricalRangeDatasetBridgeReceiptV1.model_validate(envelope.payload) == parent_receipt
    assert envelope.upstream_refs == (parent_receipt.bridge_artifact_ref,)
    parent_bridge_envelope = store.load(parent_receipt.bridge_artifact_ref)
    parent_bridge = HistoricalRangeDatasetBridgeArtifactV1.model_validate(parent_bridge_envelope.payload)
    assert parent_bridge.operation_id == "ahrop_parent"
    assert parent_bridge.model_dump(mode="json", exclude={"operation_id"}) == (
        child_bridge.model_dump(mode="json", exclude={"operation_id"})
    )
    assert parent_bridge_envelope.upstream_refs == bridge_upstream
    attempt = HistoricalRangeOperationAttemptV1(
        attempt_id="ahroba_parent_1",
        operation_id="ahrop_parent",
        attempt_no=1,
        worker_id="bridge-parent-worker",
        lease_token=uuid4().hex,
        fencing_token=1,
        status=HistoricalRangeOperationStatus.COMPLETED.value,
        input_hash=str(request.request_hash),
        result_hash=parent_ref.semantic_content_hash,
        attempt_receipt_ref=parent_ref,
        reason_codes=parent_receipt.reason_codes,
        started_at=datetime.now(UTC),
        finished_at=datetime.now(UTC),
    )
    repository = PostgresHistoricalRangeRepository(
        conn_factory=lambda: None,
        artifact_store=store,
    )
    repository._validate_operation_attempt_artifacts(
        attempt=attempt,
        resolved_request_hash="a" * 64,
        operation_type=HistoricalRangeOperationType.BUILD_DATASET_BRIDGE,
    )
