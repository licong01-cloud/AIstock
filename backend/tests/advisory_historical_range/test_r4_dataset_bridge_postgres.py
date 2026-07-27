from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeBridgeObservationV1,
    HistoricalRangeDatasetBridgeError,
)
from backend.services.advisory_historical_range.dataset_bridge_postgres import (
    PostgresHistoricalRangeBridgeAdapters,
    _label_capture_id,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeContractError,
    HistoricalRangeLineageIdentity,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangePolicyComponentV1,
    REASON_DATABASE_CAPACITY_EXHAUSTED,
)
from backend.services.advisory_phase1.capture_foundation import (
    CaptureBatchStatus,
    InMemoryCaptureBatchRepository,
    RetrospectiveObservationCaptureBatchRequestV1,
    RetrospectiveObservationCaptureBinding,
)
from backend.services.advisory_phase1.observation_capture import (
    materialize_retrospective_observation_row_bundle,
)
from backend.services.advisory_phase1.label_capture import (
    PlannedLabelDescriptor,
    RetrospectiveLabelCaptureBatchRequestV1,
    RetrospectiveLabelCaptureBinding,
    RetrospectiveSelectedObservationMappingReference,
)
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _bridge_projection,
    _ref,
    _request,
)


def _projection_fixture():
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
        projections_by_horizon={1: ("RETURN_GROSS",)},
        candidate_reference_notional="100000",
        benchmark_portfolio_notional="100000",
    )
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    policy_hash = str(policy.policy_bundle_hash)
    policy_ref = _ref(HistoricalRangeArtifactKind.REQUEST, "a").model_copy(
        update={
            "relative_path": f"requests/{policy_hash}.json",
            "payload_sha256": policy_hash,
            "semantic_content_hash": policy_hash,
            "file_sha256": policy_hash,
        }
    )
    lineage = HistoricalRangeLineageIdentity(
        historical_range_request_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "c"),
        historical_range_frozen_program_ref=_ref(
            HistoricalRangeArtifactKind.FROZEN_PROGRAM, "f"
        ),
        range_run_id="run-1",
        range_day_run_id="day-1",
        candidate_artifact_ref=candidate_ref,
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        code_release_hash="2" * 64,
        signal_source_revision_set_hash="3" * 64,
        oos_interval_hash="4" * 64,
    )
    plan, stages, candidate_fact, owner, observation_payload = _bridge_projection(
        lineage=lineage,
        policy_ref=policy_ref,
        policy=policy,
    )
    rows = materialize_retrospective_observation_row_bundle(
        plan=plan,
        stage_payload=stages,
        candidate_fact=candidate_fact,
        created_by_capture_batch_id="preflight",
    )
    observation = HistoricalRangeBridgeObservationV1(
        canonical_signal_id=plan.canonical_signal_id,
        observation_version_id=str(rows.observation_version["observation_version_id"]),
        observation_content_hash=str(
            rows.observation_version["observation_content_hash"]
        ),
        lineage=lineage,
        capture_plan=plan,
        candidate_fact=candidate_fact,
        owner=owner,
        observation_payload=observation_payload,
        stage_payload=stages,
        lineage_variants=(lineage,),
        capture_plan_variants=(plan,),
        accepted_outcome_refs=(_ref(HistoricalRangeArtifactKind.OUTCOME, "e"),),
    )
    return plan, observation


def _capture_request() -> RetrospectiveObservationCaptureBatchRequestV1:
    plan, _ = _projection_fixture()
    capture_id = "capture-observation-a1"
    return RetrospectiveObservationCaptureBatchRequestV1(
        capture_batch_id=capture_id,
        binding=RetrospectiveObservationCaptureBinding(
            capture_batch_id=capture_id,
            capture_fencing_token=1,
            range_scope=plan.range_scope,
        ),
        plans=(plan,),
    )


def _label_capture_request() -> RetrospectiveLabelCaptureBatchRequestV1:
    plan, observation = _projection_fixture()
    capture_id = "capture-label-a1"
    mapping = RetrospectiveSelectedObservationMappingReference(
        selected_mapping_id="mapping-1",
        selected_mapping_hash="8" * 64,
        canonical_signal_id=plan.canonical_signal_id,
        terminal_observation_version_id=observation.observation_version_id,
        terminal_observation_content_hash=observation.observation_content_hash,
        candidate_stage_evidence_id=str(
            observation.owner.candidate_stage_evidence_id
        ),
        selected_lineage_refs=(
            str(observation.lineage.range_lineage_identity_hash),
        ),
        selection_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
    )
    planned = PlannedLabelDescriptor(
        canonical_signal_id=plan.canonical_signal_id,
        observation_version_id=observation.observation_version_id,
        candidate_stage_evidence_id=str(
            observation.owner.candidate_stage_evidence_id
        ),
        symbol=observation.owner.symbol,
        decision_as_of_trade_date=observation.owner.decision_as_of_trade_date,
        horizon_trading_days=1,
        projection="RETURN_GROSS",
        label_key_hash="9" * 64,
    )
    mapping_hash = canonical_json_sha256([mapping.canonical_identity()])
    planned_hash = canonical_json_sha256([planned.canonical_identity()])
    binding = RetrospectiveLabelCaptureBinding(
        capture_batch_id=capture_id,
        current_fencing_token=1,
        source_observation_capture_batch_id="capture-observation-a1",
        source_capture_request_hash="1" * 64,
        source_capture_receipt_hash="2" * 64,
        source_capture_membership_count=2,
        source_capture_membership_hash="3" * 64,
        source_capture_plan_set_count=1,
        source_capture_plan_set_hash="4" * 64,
        range_scope=plan.range_scope,
        selected_observation_mapping_set_count=1,
        selected_observation_mapping_set_hash=mapping_hash,
        policy_component_set_hash="5" * 64,
        label_source_revision_set_id="label-source-1",
        label_source_revision_set_hash="6" * 64,
        label_as_of_ts=datetime(2026, 7, 10, tzinfo=UTC),
    )
    return RetrospectiveLabelCaptureBatchRequestV1(
        capture_batch_id=capture_id,
        binding=binding,
        source_observation_capture_batch_id=binding.source_observation_capture_batch_id,
        source_capture_receipt_hash=binding.source_capture_receipt_hash,
        source_capture_membership_hash=binding.source_capture_membership_hash,
        source_capture_plan_set_count=binding.source_capture_plan_set_count,
        source_capture_plan_set_hash=binding.source_capture_plan_set_hash,
        selected_observation_mappings=(mapping,),
        label_policy_bundle_id="range-policy-1",
        label_policy_bundle_hash=plan.range_scope.historical_range_policy_bundle_hash,
        historical_range_policy_bundle_ref=(
            plan.range_scope.historical_range_policy_bundle_ref
        ),
        policy_component_set_hash=binding.policy_component_set_hash,
        label_source_revision_set_id=binding.label_source_revision_set_id,
        label_source_revision_set_hash=binding.label_source_revision_set_hash,
        label_as_of_ts=binding.label_as_of_ts,
        planned_labels=(planned,),
        planned_label_count=1,
        planned_label_hash=planned_hash,
        selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
    )


def _adapter_with_repository(repository) -> PostgresHistoricalRangeBridgeAdapters:
    adapter = object.__new__(PostgresHistoricalRangeBridgeAdapters)
    adapter._capture_repository = repository
    return adapter


def test_capture_recovery_preserves_request_hash_and_is_exactly_retryable() -> None:
    now = datetime.now(UTC) - timedelta(seconds=10)
    clock = {"now": now}
    repository = InMemoryCaptureBatchRepository(now_provider=lambda: clock["now"])
    adapter = _adapter_with_repository(repository)
    request = _capture_request()
    planned = repository.create(request)
    running = repository.acquire(
        capture_batch_id=request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=1,
    )
    clock["now"] = now + timedelta(seconds=2)
    assert running.status is CaptureBatchStatus.RUNNING

    recovered = adapter._prepare_capture_batch(
        request=request,
        id_prefix="ahr_obs_cap",
    )
    retry = adapter._prepare_capture_batch(
        request=request,
        id_prefix="ahr_obs_cap",
    )

    assert recovered == retry
    assert recovered.status is CaptureBatchStatus.PLANNED
    assert recovered.capture_attempt_no == 2
    assert recovered.predecessor_capture_batch_id == request.capture_batch_id
    assert recovered.request.capture_request_hash == request.capture_request_hash
    assert recovered.request.capture_batch_id != request.capture_batch_id


def test_capture_recovery_does_not_take_over_an_active_lease() -> None:
    repository = InMemoryCaptureBatchRepository()
    adapter = _adapter_with_repository(repository)
    request = _capture_request()
    planned = repository.create(request)
    repository.acquire(
        capture_batch_id=request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=3600,
    )

    with pytest.raises(HistoricalRangeContractError) as error:
        adapter._prepare_capture_batch(
            request=request,
            id_prefix="ahr_obs_cap",
        )
    assert error.value.reason_code == REASON_DATABASE_CAPACITY_EXHAUSTED


def test_label_capture_recovery_preserves_range_policy_and_selector() -> None:
    repository = InMemoryCaptureBatchRepository()
    adapter = _adapter_with_repository(repository)
    request = _label_capture_request()
    planned = repository.create(request)
    running = repository.acquire(
        capture_batch_id=request.capture_batch_id,
        expected_row_version=planned.row_version,
        lease_seconds=60,
    )
    repository.fail(
        capture_batch_id=request.capture_batch_id,
        expected_row_version=running.row_version,
        fencing_token=running.fencing_token,
        reason_codes=("INJECTED_FAILURE",),
    )

    recovered = adapter._prepare_capture_batch(
        request=request,
        id_prefix="ahr_lbl_cap",
    )

    assert isinstance(recovered.request, RetrospectiveLabelCaptureBatchRequestV1)
    assert recovered.capture_attempt_no == 2
    assert recovered.request.capture_request_hash == request.capture_request_hash
    assert recovered.request.selector_policy_hash == RETROSPECTIVE_SELECTOR_POLICY_HASH
    assert (
        recovered.request.historical_range_policy_bundle_ref
        == request.historical_range_policy_bundle_ref
    )
    assert recovered.request.selected_observation_mappings[0].terminal_revision_no == 1


def test_label_capture_rejects_source_without_terminal_closure() -> None:
    repository = InMemoryCaptureBatchRepository()
    adapter = _adapter_with_repository(repository)
    source_batch = repository.create(_capture_request())

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="terminal receipt or membership closure",
    ):
        adapter._capture_labels(
            request=_request(),
            source_batch=source_batch,
            observations=(),
            labels=(),
            label_source_revision_id="label-source-1",
            label_source_revision_hash="6" * 64,
        )


def test_capture_preflight_rejects_mixed_policy_before_repository_mutation() -> None:
    outcome_ref = _ref(HistoricalRangeArtifactKind.OUTCOME, "e")
    policy_a = _ref(HistoricalRangeArtifactKind.REQUEST, "a")
    policy_f = _ref(HistoricalRangeArtifactKind.REQUEST, "f")
    base = _request(outcome_refs=(outcome_ref,), policy_ref=policy_a)
    components = base.policy_component_hashes[policy_a.payload_sha256]
    payload = base.model_dump(mode="python", exclude={"request_hash"})
    payload["policy_bundle_refs"] = (policy_a, policy_f)
    payload["policy_component_hashes"] = {
        policy_a.payload_sha256: components,
        policy_f.payload_sha256: components,
    }
    request = type(base).model_validate(payload)
    observation = SimpleNamespace(canonical_signal_id="signal-1")
    label_a = SimpleNamespace(
        canonical_signal_id="signal-1",
        historical_range_policy_bundle_ref=policy_a,
        historical_range_policy_bundle_hash=policy_a.payload_sha256,
        policy_component_set_hash="1" * 64,
        accepted_outcome_refs=(outcome_ref,),
    )
    label_f = SimpleNamespace(
        canonical_signal_id="signal-1",
        historical_range_policy_bundle_ref=policy_f,
        historical_range_policy_bundle_hash=policy_f.payload_sha256,
        policy_component_set_hash="1" * 64,
        accepted_outcome_refs=(outcome_ref,),
    )

    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="one exact policy/component set",
    ):
        PostgresHistoricalRangeBridgeAdapters._validate_capture_inputs(
            request=request,
            observations=(observation,),
            labels=(label_a, label_f),
        )


def test_label_capture_identity_includes_source_revision_id_and_hash() -> None:
    common = {
        "bridge_request_hash": "1" * 64,
        "scope_hash": "2" * 64,
        "label_source_revision_hash": "3" * 64,
    }
    first = _label_capture_id(
        **common,
        label_source_revision_id="source-a",
    )
    second = _label_capture_id(
        **common,
        label_source_revision_id="source-b",
    )

    assert first != second


class _RollbackConnection:
    def __init__(self) -> None:
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, _exc, _tb):
        self.rolled_back = exc_type is not None
        return False

    def cursor(self, **_kwargs):
        return _CursorContext()


class _CursorContext:
    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        return False


class _FailingTransactionCaptureRepository(InMemoryCaptureBatchRepository):
    def add_membership_in_transaction(self, *_args, **_kwargs):
        raise RuntimeError("injected membership failure")

    def complete_in_transaction(self, *_args, **_kwargs):
        raise AssertionError("failed capture transaction must not complete")


def test_observation_capture_rolls_back_its_short_write_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, observation = _projection_fixture()
    repository = _FailingTransactionCaptureRepository()
    connection = _RollbackConnection()
    adapter = _adapter_with_repository(repository)
    adapter._capture_lease_seconds = 60
    adapter._conn_factory = lambda: connection
    monkeypatch.setattr(
        "backend.services.advisory_historical_range.dataset_bridge_postgres."
        "PostgresObservationCaptureRepository.append_materialized_bundle_in_transaction",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(RuntimeError, match="injected membership failure"):
        adapter._capture_observations(
            request=_request(),
            scope_hash=str(plan.range_scope.range_lineage_scope_hash),
            observations=(observation,),
        )
    assert connection.rolled_back is True


def test_snapshot_seal_returns_persisted_retrospective_selector_hash() -> None:
    adapter = object.__new__(PostgresHistoricalRangeBridgeAdapters)
    adapter._actor = "test-actor"
    adapter._pipeline = SimpleNamespace(
        run=lambda **_kwargs: SimpleNamespace(
            request=SimpleNamespace(
                selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH
            ),
            sealed_snapshot_id="snapshot-1",
        )
    )

    assert adapter.seal(
        build_id="build-1",
        expected_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
    ) == ("snapshot-1", RETROSPECTIVE_SELECTOR_POLICY_HASH)

    with pytest.raises(Exception, match="retrospective selector"):
        adapter.seal(
            build_id="build-1",
            expected_selector_policy_hash="0" * 64,
        )
