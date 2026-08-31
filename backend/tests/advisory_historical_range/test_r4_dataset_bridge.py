from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg2
import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonicalize,
)
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeBridgeCandidateV1,
    HistoricalRangeDatasetBridgeApplicationService,
    HistoricalRangeDatasetBridgeError,
    HistoricalRangeDatasetBridgeService,
    PostgresHistoricalRangeBridgeInputLoader,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeContractError,
    HistoricalRangeDatasetBridgeReceiptV1,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeLineageIdentity,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeOperationStatus,
    HistoricalRangePolicyComponentV1,
    REASON_DATABASE_CAPACITY_EXHAUSTED,
    REASON_DATABASE_UNAVAILABLE,
    REASON_DATASET_BRIDGE_VALID_EMPTY,
    derive_outcome_logical_id,
    derive_prefixed_id,
)
from backend.services.advisory_phase1.capture_foundation import (
    RetrospectiveObservationCapturePlan,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase1.observation_capture import (
    materialize_retrospective_observation_row_bundle,
    retrospective_observation_payload,
)
from backend.services.advisory_phase1.outcome_engine import (
    OutcomeCalculationRequest,
    OutcomeEngine,
    OutcomeOwner,
    OwnerType,
)
from backend.services.advisory_phase1.retrospective_contracts import (
    HistoricalRangeArtifactReference,
    HistoricalRangeCaptureScope,
    HistoricalRangeLineageProjection,
)
from backend.services.advisory_phase1.retrospective_selector import RETROSPECTIVE_SELECTOR_POLICY_HASH
from backend.tests.advisory_phase1.test_outcome_engine import _request as _outcome_request


def _ref(kind: HistoricalRangeArtifactKind, char: str) -> HistoricalRangeArtifactRefV1:
    digest = char * 64
    namespace = {
        HistoricalRangeArtifactKind.REQUEST: "requests",
        HistoricalRangeArtifactKind.FROZEN_PROGRAM: "frozen-programs",
        HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT: "candidate-artifacts",
        HistoricalRangeArtifactKind.OUTCOME: "outcomes",
        HistoricalRangeArtifactKind.SUMMARY: "summaries",
        HistoricalRangeArtifactKind.DAY_RECEIPT: "day-receipts",
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


def _request(
    *,
    candidate_refs=(),
    outcome_refs=(),
    policy_ref=None,
    policy_components=None,
) -> HistoricalRangeDatasetBridgeRequestV1:
    policy_ref = policy_ref or _ref(HistoricalRangeArtifactKind.REQUEST, "a")
    components = policy_components or {
        role: character * 64
        for role, character in zip(
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
            "abcdefabc",
            strict=True,
        )
    }
    return HistoricalRangeDatasetBridgeRequestV1(
        batch_id="batch-1",
        range_run_ids=("run-1",),
        successful_day_refs=(_ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "d"),),
        candidate_refs=candidate_refs,
        outcome_refs=outcome_refs,
        requested_horizons=(5,),
        requested_maturity_statuses=(HistoricalRangeOutcomeStatus.COMPLETE,),
        policy_bundle_refs=(policy_ref,),
        policy_component_hashes={
            policy_ref.payload_sha256: components
        },
        canonical_signal_dedup_policy_hash="1" * 64,
        retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        dataset_schema_hash="2" * 64,
        builder_hash="3" * 64,
        writer_hash="4" * 64,
        partition_policy_hash="5" * 64,
        compression_config_hash="6" * 64,
        artifact_root_identity_hash="7" * 64,
        operation_idempotency_key="bridge-1",
        expected_batch_row_version=1,
    )


def test_bridge_request_rejects_noncanonical_or_duplicate_exact_refs() -> None:
    day_a = _ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "c")
    day_b = _ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "d")
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload["successful_day_refs"] = (day_b, day_a)
    with pytest.raises(ValueError, match="sorted and duplicate-free"):
        HistoricalRangeDatasetBridgeRequestV1.model_validate(payload)

    payload["successful_day_refs"] = (day_a, day_a)
    with pytest.raises(ValueError, match="sorted and duplicate-free"):
        HistoricalRangeDatasetBridgeRequestV1.model_validate(payload)

    policy_ref = payload["policy_bundle_refs"][0]
    payload = _request().model_dump(mode="python", exclude={"request_hash"})
    payload["policy_bundle_refs"] = (policy_ref, policy_ref)
    with pytest.raises(ValueError, match="duplicate-free|payload hashes"):
        HistoricalRangeDatasetBridgeRequestV1.model_validate(payload)


class _CaptureWriter:
    calls = 0
    last_kwargs = None

    def capture(self, **kwargs):
        self.calls += 1
        self.last_kwargs = kwargs
        return ("capture-observation", "capture-label")


class _Builder:
    calls = 0

    def build(self, **kwargs):
        self.calls += 1
        return "build-1"


class _SnapshotWriter:
    calls = 0

    def seal(self, *, build_id: str, expected_selector_policy_hash: str):
        self.calls += 1
        return "snapshot-1", expected_selector_policy_hash


class _BridgeInputRepository:
    def __init__(self, *, days=(), outcomes=()) -> None:
        self.days = tuple(days)
        self.outcomes = tuple(outcomes)

    def list_bridge_successful_days(self, **_kwargs):
        return self.days

    def list_bridge_candidate_outcomes(self, **_kwargs):
        return self.outcomes


class _UnusedProjectionLoader:
    def load(self, **_kwargs):
        raise AssertionError("projection loader must not run for rejected or empty input")


def _bridge_day(
    *,
    candidate_ref: HistoricalRangeArtifactRefV1,
    terminal_status: str,
    included_candidate_ids=(),
):
    return {
        "range_run_id": "run-1",
        "day_run_id": "day-1",
        "terminal_status": terminal_status,
        "day_receipt_ref": _ref(HistoricalRangeArtifactKind.DAY_RECEIPT, "d"),
        "candidate_artifact_ref": candidate_ref,
        "included_candidate_ids": tuple(included_candidate_ids),
    }


class _OperationRepository:
    def __init__(self, *, initial=None) -> None:
        self.operation = dict(initial) if initial is not None else None
        self.transitions = []
        self.attempts = []
        self.expired_attempts = []

    def get_or_create_operation(self, request):
        if self.operation is not None:
            return dict(self.operation), True
        self.operation = {
            "operation_id": request.operation_id,
            "batch_id": request.batch_id,
            "operation_type": request.operation_type.value,
            "operation_idempotency_key": request.operation_idempotency_key,
            "request_payload_sha256": request.request_payload_sha256,
            "expected_row_version": request.expected_row_version,
            "status": HistoricalRangeOperationStatus.QUEUED.value,
            "row_version": 1,
            "attempt_no": 0,
            "fencing_token": 0,
            "stable_keyset_cursor_json": None,
            "result_ref": None,
            "result_status": None,
        }
        return dict(self.operation), False

    def transition_operation(self, **kwargs):
        assert kwargs["expected_row_version"] == self.operation["row_version"]
        target = kwargs["target_status"]
        expired = kwargs.get("expired_attempt")
        attempt = kwargs.get("attempt")
        if expired is not None:
            self.expired_attempts.append(expired)
        if attempt is not None:
            self.attempts.append(attempt)
        self.transitions.append(dict(kwargs))
        self.operation.update(
            {
                "status": target.value,
                "row_version": self.operation["row_version"] + 1,
                "attempt_no": kwargs["attempt_no"],
                "worker_id": kwargs.get("worker_id"),
                "lease_token": kwargs.get("lease_token"),
                "lease_expires_at": kwargs.get("lease_expires_at"),
                "fencing_token": kwargs.get(
                    "fencing_token", self.operation.get("fencing_token")
                ),
                "stable_keyset_cursor_json": kwargs.get(
                    "stable_keyset_cursor_json",
                    self.operation.get("stable_keyset_cursor_json"),
                ),
                "result_ref": (
                    kwargs["result_ref"].model_dump(mode="json")
                    if kwargs.get("result_ref") is not None
                    else self.operation.get("result_ref")
                ),
                "result_status": kwargs.get(
                    "result_status", self.operation.get("result_status")
                ),
                "started_at": self.operation.get("started_at")
                or kwargs.get("started_at"),
            }
        )
        return dict(self.operation)


class _OperationBridge:
    def __init__(self, *, store: HistoricalRangeArtifactStore, effects=()) -> None:
        self.store = store
        self.effects = list(effects)
        self.calls = 0

    def build(self, *, operation_id, request, resolved_request_hash, heartbeat):
        self.calls += 1
        heartbeat("INPUT_RESOLVED")
        if self.effects:
            effect = self.effects.pop(0)
            if isinstance(effect, Exception):
                raise effect
        return self._publish(
            operation_id=operation_id,
            request=request,
            resolved_request_hash=resolved_request_hash,
            status=HistoricalRangeBridgeResultStatus.VALID_EMPTY,
            reason_codes=(REASON_DATASET_BRIDGE_VALID_EMPTY,),
        )

    def publish_failed_receipt(
        self,
        *,
        operation_id,
        request,
        resolved_request_hash,
        reason_code,
        result_status=HistoricalRangeBridgeResultStatus.FAILED,
    ):
        return self._publish(
            operation_id=operation_id,
            request=request,
            resolved_request_hash=resolved_request_hash,
            status=result_status,
            reason_codes=(reason_code,),
        )

    def _publish(
        self,
        *,
        operation_id,
        request,
        resolved_request_hash,
        status,
        reason_codes,
    ):
        bridge_ref = self.store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE,
            producer_contract_version="test_bridge_v1",
            payload_schema_version="test_bridge_v1",
            resolved_request_hash=resolved_request_hash,
            payload={
                "operation_id": operation_id,
                "request_hash": request.request_hash,
                "result_status": status.value,
            },
        ).ref
        receipt = HistoricalRangeDatasetBridgeReceiptV1(
            operation_id=operation_id,
            request_hash=str(request.request_hash),
            result_status=status,
            observation_count=0,
            label_count=0,
            canonical_signal_count=0,
            range_lineage_count=0,
            retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            bridge_artifact_ref=bridge_ref,
            reason_codes=reason_codes,
        )
        receipt_ref = self.store.publish_payload(
            artifact_kind=HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT,
            producer_contract_version="test_bridge_v1",
            payload_schema_version=receipt.schema_version,
            resolved_request_hash=resolved_request_hash,
            payload=receipt.model_dump(mode="json"),
            upstream_refs=(bridge_ref,),
        ).ref
        return receipt, receipt_ref


def _service(root: Path):
    capture = _CaptureWriter()
    builder = _Builder()
    snapshot = _SnapshotWriter()
    store = HistoricalRangeArtifactStore(root=root)
    return (
        HistoricalRangeDatasetBridgeService(
            artifact_store=store,
            capture_writer=capture,
            dataset_builder=builder,
            snapshot_writer=snapshot,
            producer_code_hash="f" * 64,
        ),
        capture,
        builder,
        snapshot,
        store,
    )


def _bridge_projection(
    *,
    lineage: HistoricalRangeLineageIdentity,
    policy_ref: HistoricalRangeArtifactRefV1,
    policy: HistoricalRangeOutcomePolicyBundleV1,
):
    phase1_lineage = HistoricalRangeLineageProjection.model_validate(
        lineage.model_dump(mode="json")
    )
    scope = HistoricalRangeCaptureScope(
        historical_range_request_ref=phase1_lineage.historical_range_request_ref,
        historical_range_frozen_program_ref=(
            phase1_lineage.historical_range_frozen_program_ref
        ),
        range_run_id=lineage.range_run_id,
        historical_range_policy_bundle_ref=(
            HistoricalRangeArtifactReference.model_validate(
                policy_ref.model_dump(mode="json")
            )
        ),
        historical_range_policy_bundle_hash=str(policy.policy_bundle_hash),
        selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
        signal_source_revision_set_id="source-set-1",
        signal_source_revision_set_hash=lineage.signal_source_revision_set_hash,
        oos_interval_hash=lineage.oos_interval_hash,
    )
    signal_scope_hash = canonical_json_sha256(
        {"signal": "candidate-1", "policy": policy.policy_bundle_hash}
    )
    stage_payload = {
        name: {
            "stage": name,
            "status": "COMPLETE",
            "input_count": 1,
            "output_count": 1,
            "excluded_count": 0,
            "reason_codes": (),
        }
        for name in (
            "alpha_raw",
            "hmm_adjusted",
            "risk_policy_adjusted",
            "selection_effective",
        )
    }
    candidate_fact = {
        "candidate_id": "candidate-1",
        "day_run_id": lineage.range_day_run_id,
        "symbol": "000001.SZ",
        "membership_status": "INCLUDED",
        "alpha_raw_rank": 1,
        "alpha_raw_score": "0.9",
        "hmm_adjusted_rank": 1,
        "hmm_adjusted_score": "0.9",
        "risk_policy_adjusted_rank": 1,
        "risk_policy_adjusted_score": "0.9",
        "selection_effective_rank": 1,
        "selection_effective_score": "0.9",
        "advisory_model_rank": None,
        "advisory_model_score": None,
        "component_lineage_json": {"component": "alpha-1"},
        "component_lineage_hash": canonical_json_sha256(
            {"component": "alpha-1"}
        ),
    }
    plan = RetrospectiveObservationCapturePlan(
        canonical_signal_id=f"acs_{signal_scope_hash[:20]}",
        symbol="000001.SZ",
        decision_as_of_trade_date=date(2026, 7, 3),
        selection_as_of_trade_date=date(2026, 7, 3),
        target_trade_date=date(2026, 7, 6),
        decision_cutoff_ts=datetime(2026, 7, 3, 7, tzinfo=UTC),
        alpha_mode="single_alpha",
        selection_runtime_semantics_hash="5" * 64,
        package_effective_config_hash="6" * 64,
        calendar_version=policy.calendar_version,
        calendar_hash=policy.calendar_hash,
        stable_signal_semantics_hash="7" * 64,
        canonical_signal_scope_hash=signal_scope_hash,
        lineage=phase1_lineage,
        range_scope=scope,
        signal_source_revision_set_id=scope.signal_source_revision_set_id,
        signal_source_revision_set_hash=scope.signal_source_revision_set_hash,
        range_signal_context_hash="8" * 64,
        evidence_bundle_hash="9" * 64,
        stage_payload_hash=canonical_json_sha256(stage_payload),
        runtime_profile_version_id="runtime-1",
        runtime_profile_version_hash="a" * 64,
        hmm_snapshot_status="NOT_APPLICABLE",
        risk_policy_hash="b" * 64,
        universe_policy_hash="c" * 64,
        symbol_normalization_policy_hash="d" * 64,
        evidence_available_at=datetime(2026, 7, 3, 8, tzinfo=UTC),
        selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
    )
    rows = materialize_retrospective_observation_row_bundle(
        plan=plan,
        stage_payload=stage_payload,
        candidate_fact=candidate_fact,
        created_by_capture_batch_id="test-capture",
    )
    selection_stage = next(
        item
        for item in rows.stage_evidence_rows
        if item["stage"] == "selection_effective"
    )
    owner = OutcomeOwner(
        owner_type=OwnerType.CANDIDATE,
        owner_key="candidate-1",
        canonical_signal_id=plan.canonical_signal_id,
        observation_version_id=rows.observation_version["observation_version_id"],
        candidate_stage_evidence_id=selection_stage["stage_evidence_id"],
        symbol="000001.SZ",
        decision_as_of_trade_date=date(2026, 7, 3),
    )
    observation_payload = retrospective_observation_payload(
        plan=plan,
        candidate_fact=candidate_fact,
        stage_evidence_bundle_hash=canonical_json_sha256(
            [item["content_hash"] for item in rows.stage_evidence_rows]
        ),
    )
    return plan, stage_payload, candidate_fact, owner, observation_payload


def test_production_input_loader_accepts_only_genuinely_candidate_empty_range() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    loader = PostgresHistoricalRangeBridgeInputLoader(
        repository=_BridgeInputRepository(
            days=(
                _bridge_day(
                    candidate_ref=candidate_ref,
                    terminal_status="VALID_NO_CANDIDATE",
                ),
            )
        ),
        projection_loader=_UnusedProjectionLoader(),
    )

    assert loader.load(request=_request(candidate_refs=(candidate_ref,))) == ()

    nonempty_loader = PostgresHistoricalRangeBridgeInputLoader(
        repository=_BridgeInputRepository(
            days=(
                _bridge_day(
                    candidate_ref=candidate_ref,
                    terminal_status="COMPLETE",
                    included_candidate_ids=("candidate-1",),
                ),
            )
        ),
        projection_loader=_UnusedProjectionLoader(),
    )
    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="genuinely candidate-empty",
    ):
        nonempty_loader.load(request=_request(candidate_refs=(candidate_ref,)))


def test_production_input_loader_rejects_duplicate_or_incomplete_exact_refs() -> None:
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    day = _bridge_day(
        candidate_ref=candidate_ref,
        terminal_status="COMPLETE",
        included_candidate_ids=("candidate-1",),
    )
    loader = PostgresHistoricalRangeBridgeInputLoader(
        repository=_BridgeInputRepository(days=(day,)),
        projection_loader=_UnusedProjectionLoader(),
    )
    with pytest.raises(ValueError, match="duplicate-free"):
        _request(candidate_refs=(candidate_ref, candidate_ref))

    outcome_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, "e")
    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="outcome set is incomplete",
    ):
        loader.load(
            request=_request(
                candidate_refs=(candidate_ref,),
                outcome_refs=(outcome_ref,),
            )
        )

    missing_day_loader = PostgresHistoricalRangeBridgeInputLoader(
        repository=_BridgeInputRepository(),
        projection_loader=_UnusedProjectionLoader(),
    )
    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="coverage is incomplete",
    ):
        missing_day_loader.load(request=_request(candidate_refs=(candidate_ref,)))


def test_bridge_operation_exact_retry_returns_durable_terminal_receipt(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "operation-artifacts")
    repository = _OperationRepository()
    bridge = _OperationBridge(store=store)
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=store,
        bridge_service=bridge,
    )
    request = _request()

    first = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="worker-1",
    )
    retry = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="worker-2",
    )

    assert first == retry
    assert bridge.calls == 1
    assert repository.operation["request_payload_sha256"] == request.request_hash
    assert repository.operation["status"] == HistoricalRangeOperationStatus.COMPLETED.value
    assert [item.status for item in repository.attempts] == ["COMPLETED"]
    assert any(
        item.get("stable_keyset_cursor_json") == {"phase": "INPUT_RESOLVED"}
        for item in repository.transitions
    )


def test_bridge_operation_retryable_failure_resumes_with_new_fencing_attempt(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "retry-artifacts")
    repository = _OperationRepository()
    bridge = _OperationBridge(
        store=store,
        effects=(
            HistoricalRangeContractError(
                REASON_DATABASE_CAPACITY_EXHAUSTED,
                "injected capacity failure",
            ),
        ),
    )
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=store,
        bridge_service=bridge,
    )
    request = _request()

    failed, _ = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="worker-1",
    )
    resumed, _ = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="worker-2",
    )

    assert failed.result_status is HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED
    assert failed.reason_codes == (REASON_DATABASE_CAPACITY_EXHAUSTED,)
    assert resumed.result_status is HistoricalRangeBridgeResultStatus.VALID_EMPTY
    assert bridge.calls == 2
    assert [item.status for item in repository.attempts] == [
        "RETRYABLE_FAILED",
        "COMPLETED",
    ]
    claims = [
        item for item in repository.transitions
        if item["target_status"] is HistoricalRangeOperationStatus.RUNNING
        and item["attempt_no"] in {1, 2}
    ]
    assert [item["fencing_token"] for item in claims if item.get("started_at")] == [1, 2]


def test_bridge_database_unavailable_is_retryable_without_terminal_operation(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "database-unavailable-artifacts")
    repository = _OperationRepository()
    bridge = _OperationBridge(
        store=store,
        effects=(psycopg2.OperationalError("database connection dropped"),),
    )
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=store,
        bridge_service=bridge,
    )

    receipt, _ = service.build_until_stable_boundary(
        request=_request(),
        resolved_request_hash="8" * 64,
        worker_id="worker-1",
    )

    assert receipt.result_status is HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED
    assert receipt.reason_codes == (REASON_DATABASE_UNAVAILABLE,)
    assert repository.operation["status"] == HistoricalRangeOperationStatus.RETRYABLE_FAILED.value
    assert repository.attempts[-1].status == HistoricalRangeOperationStatus.RETRYABLE_FAILED.value


def test_bridge_typed_database_unavailable_contract_error_is_retryable(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "typed-database-unavailable-artifacts")
    repository = _OperationRepository()
    bridge = _OperationBridge(
        store=store,
        effects=(
            HistoricalRangeContractError(
                REASON_DATABASE_UNAVAILABLE,
                "injected typed database-unavailable failure",
            ),
        ),
    )
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=store,
        bridge_service=bridge,
    )

    receipt, _ = service.build_until_stable_boundary(
        request=_request(),
        resolved_request_hash="8" * 64,
        worker_id="worker-1",
    )

    assert receipt.result_status is HistoricalRangeBridgeResultStatus.RETRYABLE_FAILED
    assert receipt.reason_codes == (REASON_DATABASE_UNAVAILABLE,)
    assert repository.operation["status"] == HistoricalRangeOperationStatus.RETRYABLE_FAILED.value
    assert repository.attempts[-1].status == HistoricalRangeOperationStatus.RETRYABLE_FAILED.value


def test_bridge_operation_takes_over_only_an_expired_running_lease(
    tmp_path: Path,
) -> None:
    store = HistoricalRangeArtifactStore(root=tmp_path / "takeover-artifacts")
    request = _request()
    operation_id = derive_prefixed_id(
        "ahrop",
        {
            "batch_id": request.batch_id,
            "operation_type": "BUILD_DATASET_BRIDGE",
            "idempotency_key": request.operation_idempotency_key,
        },
    )
    repository = _OperationRepository(
        initial={
            "operation_id": operation_id,
            "batch_id": request.batch_id,
            "operation_type": "BUILD_DATASET_BRIDGE",
            "operation_idempotency_key": request.operation_idempotency_key,
            "request_payload_sha256": "8" * 64,
            "expected_row_version": request.expected_batch_row_version,
            "status": HistoricalRangeOperationStatus.RUNNING.value,
            "row_version": 2,
            "attempt_no": 1,
            "fencing_token": 1,
            "worker_id": "expired-worker",
            "lease_token": "expired-token",
            "lease_expires_at": datetime.now(UTC) - timedelta(seconds=1),
            "stable_keyset_cursor_json": {"phase": "CAPTURED"},
            "started_at": datetime.now(UTC) - timedelta(minutes=5),
            "result_ref": None,
            "result_status": None,
        }
    )
    bridge = _OperationBridge(store=store)
    service = HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=store,
        bridge_service=bridge,
    )

    receipt, _ = service.build_until_stable_boundary(
        request=request,
        resolved_request_hash="8" * 64,
        worker_id="takeover-worker",
    )

    assert receipt.result_status is HistoricalRangeBridgeResultStatus.VALID_EMPTY
    assert len(repository.expired_attempts) == 1
    assert repository.expired_attempts[0].status == "RETRYABLE_FAILED"
    assert repository.expired_attempts[0].result_cursor_json == {
        "phase": "LEASE_EXPIRED"
    }
    assert repository.attempts[-1].fencing_token == 2


def test_valid_empty_completes_without_capture_build_or_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "range-artifacts"
    root.mkdir()
    service, capture, builder, snapshot, _ = _service(root)
    receipt, receipt_ref = service.build(
        operation_id="operation-1",
        request=_request(),
        candidates=(),
        resolved_request_hash="8" * 64,
    )
    assert receipt.result_status is HistoricalRangeBridgeResultStatus.VALID_EMPTY
    assert receipt.reason_codes == (REASON_DATASET_BRIDGE_VALID_EMPTY,)
    assert receipt.dataset_build_id is None and receipt.sealed_snapshot_id is None
    assert receipt_ref.artifact_kind is HistoricalRangeArtifactKind.DATASET_BRIDGE_RECEIPT
    assert capture.calls == builder.calls == snapshot.calls == 0


def test_nonempty_bridge_projects_only_candidate_fixed_executable_and_seals(tmp_path: Path) -> None:
    root = tmp_path / "range-artifacts"
    root.mkdir()
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    service, capture, builder, snapshot, store = _service(root)
    component_hashes = {
        role: character * 64
        for role, character in zip(
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
            "abcdefabc",
            strict=True,
        )
    }
    policy = HistoricalRangeOutcomePolicyBundleV1(
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        alpha_mode="single_alpha",
        style_family="TREND",
        style_resolution_reason="FROZEN_TEST_POLICY",
        calendar_version="calendar-v1",
        calendar_hash=component_hashes["CALENDAR"],
        components=tuple(
                HistoricalRangePolicyComponentV1(
                    component_role=role,
                    component_ref=f"components/{role.lower()}-v1",
                    component_hash=component_hashes[role],
            )
            for role in sorted(component_hashes)
        ),
        horizons=(1,),
        projections_by_horizon={
            1: (
                "EXECUTABLE_MFE",
                "RETURN_GROSS",
                "RETURN_NET_EXCESS",
            )
        },
        candidate_reference_notional="100000",
        benchmark_portfolio_notional="100000",
    )
    policy_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="r4_policy_v1",
        payload_schema_version=policy.schema_version,
        resolved_request_hash="8" * 64,
        payload=policy.model_dump(
            mode="json",
            exclude={"policy_bundle_id", "policy_bundle_hash"},
        ),
    ).ref
    assert policy_ref.payload_sha256 == policy.policy_bundle_hash
    lineage = HistoricalRangeLineageIdentity(
        historical_range_request_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "a"),
        historical_range_frozen_program_ref=_ref(HistoricalRangeArtifactKind.FROZEN_PROGRAM, "f"),
        range_run_id="run-1",
        range_day_run_id="day-1",
        candidate_artifact_ref=candidate_ref,
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        code_release_hash="2" * 64,
        signal_source_revision_set_hash="3" * 64,
        oos_interval_hash="4" * 64,
    )
    plan, stages, candidate_fact, owner, _observation_payload = _bridge_projection(
        lineage=lineage,
        policy_ref=policy_ref,
        policy=policy,
    )

    def _owned_request(projection: Projection) -> OutcomeCalculationRequest:
        base = _outcome_request(projection, horizon=1)
        payload = base.model_dump(mode="python", exclude={"calculation_request_hash"})
        payload["owner"] = owner.model_dump(mode="python")
        return OutcomeCalculationRequest.model_validate(payload)

    results = tuple(
        OutcomeEngine().calculate(_owned_request(projection))
        for projection in (
            Projection.RETURN_GROSS,
            Projection.EXECUTABLE_MFE,
            Projection.RETURN_NET_EXCESS,
        )
    )
    outcome_logical_id = derive_outcome_logical_id(
        HistoricalRangeOutcomeSubjectType.CANDIDATE,
        "candidate-1",
        HistoricalRangeOutcomeProjection.EXECUTABLE,
        HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        1,
        str(policy.policy_bundle_hash),
    )
    result_payloads = tuple(
        canonicalize(item.model_dump(mode="python")) for item in results
    )
    artifact = HistoricalRangeOutcomeArtifactV2(
        outcome_logical_id=outcome_logical_id,
        outcome_version_id="outcome-version-1",
        outcome_input_hash="9" * 64,
        subject_ref=candidate_ref,
        direct_upstream_refs=(candidate_ref, policy_ref),
        projection_group=HistoricalRangeOutcomeProjection.EXECUTABLE,
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=1,
        policy_bundle_ref=policy_ref,
        policy_bundle_hash=str(policy.policy_bundle_hash),
        label_as_of_trade_date=date(2026, 7, 10),
        source_revision_set_hash="e" * 64,
        maturity_status=HistoricalRangeOutcomeStatus.NOT_DUE,
        calculation_results=result_payloads,
        calculation_result_set_hash=canonical_json_sha256(list(result_payloads)),
        producer_code_hash="f" * 64,
    )
    outcome_ref = store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.OUTCOME,
        producer_contract_version="r4_v1",
        payload_schema_version=artifact.schema_version,
        resolved_request_hash="8" * 64,
        payload=artifact.model_dump(mode="json"),
        range_run_id="run-1",
        upstream_refs=(candidate_ref, policy_ref),
    ).ref
    outcome = HistoricalRangeOutcomeFactV1(
        outcome_version_id="outcome-version-1",
        outcome_logical_id=outcome_logical_id,
        outcome_version=1,
        subject_type=HistoricalRangeOutcomeSubjectType.CANDIDATE,
        subject_id="candidate-1",
        projection=HistoricalRangeOutcomeProjection.EXECUTABLE,
        evaluation_window_type=HistoricalRangeEvaluationWindowType.FIXED_HORIZON,
        horizon_trade_days=1,
        historical_range_policy_bundle_hash=str(policy.policy_bundle_hash),
        outcome_input_hash="9" * 64,
        revision_reason=HistoricalRangeOutcomeRevisionReason.INITIAL,
        producer_code_hash="f" * 64,
        outcome_contract_version="r4_v1",
        source_revision_set_hash="e" * 64,
        maturity_status=HistoricalRangeOutcomeStatus.NOT_DUE,
        label_as_of_trade_date=date(2026, 7, 10),
        outcome_artifact_ref=outcome_ref,
        outcome_json=artifact.model_dump(mode="json"),
    )
    mismatched = outcome.model_dump(
        mode="python",
        exclude={"outcome_content_hash"},
    )
    mismatched["producer_code_hash"] = "1" * 64
    with pytest.raises(
        ValueError,
        match="columns differ from the embedded V2 artifact",
    ):
        HistoricalRangeOutcomeFactV1.model_validate(mismatched)
    candidate = HistoricalRangeBridgeCandidateV1(
        canonical_signal_id=plan.canonical_signal_id,
        symbol="000001.SZ",
        lineage=lineage,
        candidate_artifact_ref=candidate_ref,
        capture_plan=plan,
        candidate_fact=candidate_fact,
        owner=owner,
        stage_payload=stages,
        stage_payload_hash=canonical_json_sha256(stages),
        outcome=outcome,
        outcome_ref=outcome_ref,
    )
    receipt, _ = service.build(
        operation_id="operation-1",
        request=_request(
            candidate_refs=(candidate_ref,),
            outcome_refs=(outcome_ref,),
            policy_ref=policy_ref,
            policy_components=component_hashes,
        ),
        candidates=(candidate,),
        resolved_request_hash="8" * 64,
    )
    assert receipt.result_status is HistoricalRangeBridgeResultStatus.SEALED
    assert receipt.observation_count == 1
    assert receipt.label_count == 2
    assert {
        item.projection
        for item in capture.last_kwargs["labels"]
    } == {Projection.RETURN_GROSS, Projection.EXECUTABLE_MFE}
    assert receipt.dataset_build_id == "build-1"
    assert receipt.sealed_snapshot_id == "snapshot-1"
    assert capture.calls == builder.calls == snapshot.calls == 1
    bridge_envelope = store.load(receipt.bridge_artifact_ref)
    assert policy_ref in bridge_envelope.upstream_refs
    assert candidate_ref in bridge_envelope.upstream_refs
    assert outcome_ref in bridge_envelope.upstream_refs
