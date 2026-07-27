"""R4 bridge fine-grained maturity and duplicate-signal regressions.

Covers the production non-empty-bridge defect family:
- an outer NOT_DUE outcome still contributes its individually MATURED
  projections, and only projections satisfying the requested maturity are
  emitted as labels;
- a missing benchmark degrades RETURN_NET_EXCESS to a typed unavailable
  result without touching gross/absolute/path projections;
- duplicate canonical-signal range-lineage variants share one economic
  observation while every real lineage ref is preserved.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from backend.services.advisory_historical_range.artifact_store import HistoricalRangeArtifactStore
from backend.services.advisory_historical_range.canonical import (
    canonical_json_sha256,
    canonicalize,
)
from backend.services.advisory_historical_range.dataset_bridge import (
    HistoricalRangeBridgeCandidateV1,
    HistoricalRangeDatasetBridgeError,
    _eligible_executable_results,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeBridgeResultStatus,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeLineageIdentity,
    HistoricalRangeOutcomeArtifactV2,
    HistoricalRangeOutcomeFactV1,
    HistoricalRangeOutcomePolicyBundleV1,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangePolicyComponentV1,
    derive_outcome_logical_id,
)
from backend.services.advisory_phase1.capture_foundation import (
    RetrospectiveObservationCapturePlan,
)
from backend.services.advisory_phase1.label_policy import Projection
from backend.services.advisory_phase1.outcome_engine import (
    MaturityStatus,
    OutcomeCalculationRequest,
    OutcomeEngine,
    OutcomeOwner,
    REASON_BENCHMARK_UNAVAILABLE,
)
from backend.tests.advisory_historical_range.test_r4_dataset_bridge import (
    _bridge_projection,
    _ref,
    _request,
    _service,
)


_COMPONENT_HASHES = {
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


def _policy_with_projections(
    projections: tuple[str, ...],
) -> HistoricalRangeOutcomePolicyBundleV1:
    return HistoricalRangeOutcomePolicyBundleV1(
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        alpha_mode="single_alpha",
        style_family="TREND",
        style_resolution_reason="FROZEN_TEST_POLICY",
        calendar_version="calendar-v1",
        calendar_hash=_COMPONENT_HASHES["CALENDAR"],
        components=tuple(
            HistoricalRangePolicyComponentV1(
                component_role=role,
                component_ref=f"components/{role.lower()}-v1",
                component_hash=_COMPONENT_HASHES[role],
            )
            for role in sorted(_COMPONENT_HASHES)
        ),
        horizons=(1,),
        projections_by_horizon={1: projections},
        candidate_reference_notional="100000",
        benchmark_portfolio_notional="100000",
    )


def _publish_policy(
    store: HistoricalRangeArtifactStore,
    policy: HistoricalRangeOutcomePolicyBundleV1,
) -> HistoricalRangeArtifactRefV1:
    return store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="r4_policy_v1",
        payload_schema_version=policy.schema_version,
        resolved_request_hash="8" * 64,
        payload=policy.model_dump(
            mode="json",
            exclude={"policy_bundle_id", "policy_bundle_hash"},
        ),
    ).ref


def _lineage_for(
    candidate_ref: HistoricalRangeArtifactRefV1,
    *,
    day_run_id: str,
) -> HistoricalRangeLineageIdentity:
    return HistoricalRangeLineageIdentity(
        historical_range_request_ref=_ref(HistoricalRangeArtifactKind.REQUEST, "a"),
        historical_range_frozen_program_ref=_ref(
            HistoricalRangeArtifactKind.FROZEN_PROGRAM, "f"
        ),
        range_run_id="run-1",
        range_day_run_id=day_run_id,
        candidate_artifact_ref=candidate_ref,
        package_id="pkg-1",
        manifest_sha256="1" * 64,
        code_release_hash="2" * 64,
        signal_source_revision_set_hash="3" * 64,
        oos_interval_hash="4" * 64,
    )


def _owned_outcome_results(
    owner: OutcomeOwner,
    projections: tuple[Projection, ...],
) -> dict[Projection, Any]:
    from backend.tests.advisory_phase1.test_outcome_engine import (
        _request as _outcome_request,
    )

    def _request_for(projection: Projection) -> OutcomeCalculationRequest:
        base = _outcome_request(projection, horizon=1)
        payload = base.model_dump(mode="python", exclude={"calculation_request_hash"})
        payload["owner"] = owner.model_dump(mode="python")
        return OutcomeCalculationRequest.model_validate(payload)

    return {
        projection: OutcomeEngine().calculate(_request_for(projection))
        for projection in projections
    }


def _outcome_fact_and_ref(
    store: HistoricalRangeArtifactStore,
    *,
    candidate_ref: HistoricalRangeArtifactRefV1,
    policy_ref: HistoricalRangeArtifactRefV1,
    policy: HistoricalRangeOutcomePolicyBundleV1,
    results: tuple[Any, ...],
    outcome_version_id: str,
    outer_status: HistoricalRangeOutcomeStatus,
) -> tuple[HistoricalRangeOutcomeFactV1, HistoricalRangeArtifactRefV1, HistoricalRangeOutcomeArtifactV2]:
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
        outcome_version_id=outcome_version_id,
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
        maturity_status=outer_status,
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
    fact = HistoricalRangeOutcomeFactV1(
        outcome_version_id=outcome_version_id,
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
        maturity_status=outer_status,
        label_as_of_trade_date=date(2026, 7, 10),
        outcome_artifact_ref=outcome_ref,
        outcome_json=artifact.model_dump(mode="json"),
    )
    return fact, outcome_ref, artifact


def _sorted_refs(
    refs: tuple[HistoricalRangeArtifactRefV1, ...],
) -> tuple[HistoricalRangeArtifactRefV1, ...]:
    return tuple(
        sorted(
            refs,
            key=lambda item: (
                item.artifact_kind.value,
                item.semantic_content_hash,
                item.relative_path,
            ),
        )
    )


def test_bridge_labels_only_fine_grained_matured_projections_when_outer_not_due(
    tmp_path: Path,
) -> None:
    root = tmp_path / "range-artifacts"
    root.mkdir()
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    service, capture, builder, snapshot, store = _service(root)
    policy = _policy_with_projections(
        (
            "EXECUTABLE_MFE",
            "RETURN_GROSS",
            "RETURN_NET_ABSOLUTE",
            "RETURN_NET_EXCESS",
        ),
    )
    policy_ref = _publish_policy(store, policy)
    lineage = _lineage_for(candidate_ref, day_run_id="day-1")
    plan, stages, candidate_fact, owner, _ = _bridge_projection(
        lineage=lineage,
        policy_ref=policy_ref,
        policy=policy,
    )
    results = _owned_outcome_results(
        owner,
        (
            Projection.RETURN_GROSS,
            Projection.RETURN_NET_ABSOLUTE,
            Projection.EXECUTABLE_MFE,
            Projection.RETURN_NET_EXCESS,
        ),
    )
    # The benchmark-less request must keep gross/absolute/path projections
    # MATURED while RETURN_NET_EXCESS degrades to a typed benchmark-unavailable
    # result.  Without an immutable benchmark failure receipt the engine keeps
    # the source PENDING (never a fake zero return), which the fine-grained
    # projection maps to NOT_DUE -- excluded from COMPLETE-requested labels.
    assert results[Projection.RETURN_GROSS].maturity_status is MaturityStatus.MATURED
    assert (
        results[Projection.RETURN_NET_ABSOLUTE].maturity_status
        is MaturityStatus.MATURED
    )
    assert results[Projection.EXECUTABLE_MFE].maturity_status is MaturityStatus.MATURED
    excess = results[Projection.RETURN_NET_EXCESS]
    assert excess.maturity_status is MaturityStatus.PENDING
    assert REASON_BENCHMARK_UNAVAILABLE in excess.reason_codes

    fact, outcome_ref, artifact = _outcome_fact_and_ref(
        store,
        candidate_ref=candidate_ref,
        policy_ref=policy_ref,
        policy=policy,
        results=tuple(results.values()),
        outcome_version_id="outcome-version-1",
        outer_status=HistoricalRangeOutcomeStatus.NOT_DUE,
    )
    # Fine-grained filtering: the outer outcome is NOT_DUE, yet only the
    # projections that actually satisfy the requested maturity may enter.
    complete_only = _eligible_executable_results(
        artifact.model_dump(mode="python"),
        requested_maturity_statuses=(HistoricalRangeOutcomeStatus.COMPLETE,),
    )
    assert {item.projection for item in complete_only} == {
        Projection.RETURN_GROSS,
        Projection.RETURN_NET_ABSOLUTE,
        Projection.EXECUTABLE_MFE,
    }
    with_maturing = _eligible_executable_results(
        artifact.model_dump(mode="python"),
        requested_maturity_statuses=(
            HistoricalRangeOutcomeStatus.COMPLETE,
            HistoricalRangeOutcomeStatus.MATURING,
        ),
    )
    # PENDING maps to NOT_DUE, so the benchmark-less excess projection stays
    # out of COMPLETE/MATURING requests and only re-enters when NOT_DUE is
    # explicitly requested.
    assert {item.projection for item in with_maturing} == {
        Projection.RETURN_GROSS,
        Projection.RETURN_NET_ABSOLUTE,
        Projection.EXECUTABLE_MFE,
    }
    with_not_due = _eligible_executable_results(
        artifact.model_dump(mode="python"),
        requested_maturity_statuses=(
            HistoricalRangeOutcomeStatus.COMPLETE,
            HistoricalRangeOutcomeStatus.NOT_DUE,
        ),
    )
    assert {item.projection for item in with_not_due} == set(results)

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
        outcome=fact,
        outcome_ref=outcome_ref,
    )
    receipt, _ = service.build(
        operation_id="operation-1",
        request=_request(
            candidate_refs=(candidate_ref,),
            outcome_refs=(outcome_ref,),
            policy_ref=policy_ref,
            policy_components=_COMPONENT_HASHES,
        ),
        candidates=(candidate,),
        resolved_request_hash="8" * 64,
    )
    assert receipt.result_status is HistoricalRangeBridgeResultStatus.SEALED
    assert receipt.observation_count == 1
    assert receipt.label_count == 3
    assert {item.projection for item in capture.last_kwargs["labels"]} == {
        Projection.RETURN_GROSS,
        Projection.RETURN_NET_ABSOLUTE,
        Projection.EXECUTABLE_MFE,
    }
    assert capture.calls == builder.calls == snapshot.calls == 1


def test_bridge_deduplicates_canonical_signal_variants_and_preserves_lineage(
    tmp_path: Path,
) -> None:
    root = tmp_path / "range-artifacts"
    root.mkdir()
    # Both variants reference the exact same candidate artifact; only the
    # day-run lineage and the economically excluded candidate identity keys
    # (candidate_id / day_run_id / candidate_content_hash) differ.  The
    # economic observation identity must therefore materialize exactly once.
    candidate_ref = _ref(HistoricalRangeArtifactKind.CANDIDATE_ARTIFACT, "b")
    service, capture, builder, snapshot, store = _service(root)
    policy = _policy_with_projections(("RETURN_GROSS",))
    policy_ref = _publish_policy(store, policy)

    lineage = _lineage_for(candidate_ref, day_run_id="day-1")
    plan, stages, candidate_fact, owner, _ = _bridge_projection(
        lineage=lineage,
        policy_ref=policy_ref,
        policy=policy,
    )
    variant_lineage = _lineage_for(candidate_ref, day_run_id="day-2")
    variant_plan, _, _, variant_owner, _ = _bridge_projection(
        lineage=variant_lineage,
        policy_ref=policy_ref,
        policy=policy,
    )
    assert variant_plan.canonical_signal_id == plan.canonical_signal_id
    assert (
        variant_lineage.range_lineage_identity_hash
        != lineage.range_lineage_identity_hash
    )
    variant_fact = dict(candidate_fact)
    variant_fact["candidate_id"] = "candidate-1b"
    variant_fact["day_run_id"] = "day-2"

    results = _owned_outcome_results(owner, (Projection.RETURN_GROSS,))
    variant_results = _owned_outcome_results(variant_owner, (Projection.RETURN_GROSS,))
    fact, outcome_ref, _ = _outcome_fact_and_ref(
        store,
        candidate_ref=candidate_ref,
        policy_ref=policy_ref,
        policy=policy,
        results=(results[Projection.RETURN_GROSS],),
        outcome_version_id="outcome-version-1",
        outer_status=HistoricalRangeOutcomeStatus.COMPLETE,
    )
    variant_outcome, variant_outcome_ref, _ = _outcome_fact_and_ref(
        store,
        candidate_ref=candidate_ref,
        policy_ref=policy_ref,
        policy=policy,
        results=(variant_results[Projection.RETURN_GROSS],),
        outcome_version_id="outcome-version-2",
        outer_status=HistoricalRangeOutcomeStatus.COMPLETE,
    )

    def _candidate(
        *,
        signal_lineage: HistoricalRangeLineageIdentity,
        signal_plan: RetrospectiveObservationCapturePlan,
        signal_candidate_fact: dict,
        signal_owner: OutcomeOwner,
        signal_ref: HistoricalRangeArtifactRefV1,
        signal_outcome: HistoricalRangeOutcomeFactV1,
        signal_outcome_ref: HistoricalRangeArtifactRefV1,
    ) -> HistoricalRangeBridgeCandidateV1:
        return HistoricalRangeBridgeCandidateV1(
            canonical_signal_id=signal_plan.canonical_signal_id,
            symbol="000001.SZ",
            lineage=signal_lineage,
            candidate_artifact_ref=signal_ref,
            capture_plan=signal_plan,
            candidate_fact=signal_candidate_fact,
            owner=signal_owner,
            stage_payload=stages,
            stage_payload_hash=canonical_json_sha256(stages),
            outcome=signal_outcome,
            outcome_ref=signal_outcome_ref,
        )

    primary = _candidate(
        signal_lineage=lineage,
        signal_plan=plan,
        signal_candidate_fact=candidate_fact,
        signal_owner=owner,
        signal_ref=candidate_ref,
        signal_outcome=fact,
        signal_outcome_ref=outcome_ref,
    )
    variant = _candidate(
        signal_lineage=variant_lineage,
        signal_plan=variant_plan,
        signal_candidate_fact=variant_fact,
        signal_owner=variant_owner,
        signal_ref=candidate_ref,
        signal_outcome=variant_outcome,
        signal_outcome_ref=variant_outcome_ref,
    )
    receipt, _ = service.build(
        operation_id="operation-1",
        request=_request(
            candidate_refs=(candidate_ref,),
            outcome_refs=_sorted_refs((outcome_ref, variant_outcome_ref)),
            policy_ref=policy_ref,
            policy_components=_COMPONENT_HASHES,
        ),
        candidates=(primary, variant),
        resolved_request_hash="8" * 64,
    )
    assert receipt.result_status is HistoricalRangeBridgeResultStatus.SEALED
    assert receipt.observation_count == 1
    assert receipt.label_count == 1
    assert receipt.canonical_signal_count == 1
    assert receipt.range_lineage_count == 2
    assert capture.calls == builder.calls == snapshot.calls == 1
    observation = capture.last_kwargs["observations"][0]
    assert {
        item.range_lineage_identity_hash for item in observation.lineage_variants
    } == {
        lineage.range_lineage_identity_hash,
        variant_lineage.range_lineage_identity_hash,
    }
    assert len(observation.capture_plan_variants) == 2
    assert {
        item.semantic_content_hash for item in observation.accepted_outcome_refs
    } == {
        outcome_ref.semantic_content_hash,
        variant_outcome_ref.semantic_content_hash,
    }

    conflicting_stages = {
        name: dict(receipt_row) for name, receipt_row in stages.items()
    }
    conflicting_stages["alpha_raw"]["output_count"] = 2
    conflicting = HistoricalRangeBridgeCandidateV1(
        canonical_signal_id=plan.canonical_signal_id,
        symbol="000001.SZ",
        lineage=variant_lineage,
        candidate_artifact_ref=candidate_ref,
        capture_plan=variant_plan,
        candidate_fact=variant_fact,
        owner=variant_owner,
        stage_payload=conflicting_stages,
        stage_payload_hash=canonical_json_sha256(conflicting_stages),
        outcome=variant_outcome,
        outcome_ref=variant_outcome_ref,
    )
    with pytest.raises(
        HistoricalRangeDatasetBridgeError,
        match="conflicting observation or label content",
    ):
        service.build(
            operation_id="operation-2",
            request=_request(
                candidate_refs=(candidate_ref,),
                outcome_refs=_sorted_refs((outcome_ref, variant_outcome_ref)),
                policy_ref=policy_ref,
                policy_components=_COMPONENT_HASHES,
            ),
            candidates=(primary, conflicting),
            resolved_request_hash="8" * 64,
        )
