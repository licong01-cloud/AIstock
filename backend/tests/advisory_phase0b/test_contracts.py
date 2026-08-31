from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.summary_service import (
    Phase1WinnerDefinitionV1,
)
from backend.services.advisory_phase0b.contracts import (
    AuditStyleHypothesis,
    Phase0BAuditTargetV1,
    Phase0BByFdrPolicyV1,
    Phase0BCandidateQualityAuditRequestV1,
    Phase0BDatasetStoreIdentityV1,
    Phase0BEconomicSignificancePolicyV1,
    Phase0BEconomicThresholdV1,
    Phase0BMetricDefinitionV1,
    Phase0BMetricRegistryV1,
    Phase0BMultipleTestingRegistryV1,
    Phase0BNumericKernelV1,
    Phase0BRandomPolicyV1,
    Phase0BSpaPolicyV1,
    Phase0BStationaryBootstrapPolicyV1,
    Phase0BStyleHorizonBindingV1,
    Phase0BTargetRuntimeVariantBindingV1,
    Phase0BTargetStyleBindingV1,
)


_HASH = "a" * 64
_CODE_HASH = "b" * 64


def _winner() -> Phase1WinnerDefinitionV1:
    return Phase1WinnerDefinitionV1(
        winner_definition_id="positive-net-excess-h5-v1",
        projection="RETURN_NET_EXCESS",
        comparison_operator="GT",
        threshold=Decimal("0"),
        ranking_direction="DESC",
        horizon_trade_days=5,
        label_policy_hash=_HASH,
        denominator_universe_layer="PIT_ELIGIBLE",
    )


def _target(*, snapshot_id: str = "snapshot-1") -> Phase0BAuditTargetV1:
    return Phase0BAuditTargetV1(
        snapshot_id=snapshot_id,
        program_id="program-1",
        package_id="package-1",
        manifest_sha256=_HASH,
        alpha_mode="multi_alpha",
        style_hypothesis=AuditStyleHypothesis.SHORT_REBOUND,
    )


def _request() -> Phase0BCandidateQualityAuditRequestV1:
    target = _target()
    winner = _winner()
    store = Phase0BDatasetStoreIdentityV1.from_authoritative_factory()
    numeric = Phase0BNumericKernelV1()
    metric = Phase0BMetricDefinitionV1(
        metric_id="selection-effective-top5-net-return-v1",
        family="PRIMARY",
        projection="RETURN_NET_EXCESS",
        horizon_source="LABEL_POLICY",
        horizons=(5,),
        stages=("selection_effective",),
        depths=(5,),
        cash_policy="NET_EXCESS_NEGATIVE_BENCHMARK",
        maturity_eligibility=("MATURED",),
        event_eligibility=("TERMINAL",),
        winner_definition_ids=(),
        benchmark_policy_ref="snapshot-label-policy-benchmark",
        cost_policy_ref="snapshot-label-policy-cost",
        numeric_kernel_ref=str(numeric.kernel_hash),
        output_unit="DECIMAL_RETURN",
    )
    coverage_metric = Phase0BMetricDefinitionV1(
        metric_id="label-coverage-v1",
        family="COVERAGE",
        projection="RETURN_NET_EXCESS",
        horizon_source="LABEL_POLICY",
        horizons=(5,),
        stages=("selection_effective",),
        depths=(5,),
        cash_policy="NOT_APPLICABLE",
        maturity_eligibility=("MATURED", "PENDING", "RIGHT_CENSORED", "UNAVAILABLE"),
        event_eligibility=("TERMINAL", "NON_TERMINAL"),
        winner_definition_ids=(),
        benchmark_policy_ref="snapshot-label-policy-benchmark",
        cost_policy_ref="snapshot-label-policy-cost",
        numeric_kernel_ref=str(numeric.kernel_hash),
        output_unit="COUNT",
    )
    metric_registry = Phase0BMetricRegistryV1(metrics=(metric, coverage_metric))
    target_set_hash = canonical_json_sha256((target.target_hash,))
    winner_set_hash = canonical_json_sha256((winner.winner_definition_hash,))
    testing = Phase0BMultipleTestingRegistryV1(
        audit_target_identity_set_hash=target_set_hash,
        style_hypothesis_by_target=(
            Phase0BTargetStyleBindingV1(
                target_hash=str(target.target_hash),
                style_hypothesis=target.style_hypothesis,
            ),
        ),
        manifest_runtime_variant_by_target=(
            Phase0BTargetRuntimeVariantBindingV1(
                target_hash=str(target.target_hash),
                manifest_sha256=target.manifest_sha256,
                runtime_variant_id="runtime-variant-1",
            ),
        ),
        winner_definition_set_hash=winner_set_hash,
        horizons_by_style=(
            Phase0BStyleHorizonBindingV1(
                style_hypothesis="SHORT_REBOUND",
                horizons=(1, 3, 5, 10, 20),
                winner_definition_ids=(winner.winner_definition_id,),
            ),
            Phase0BStyleHorizonBindingV1(
                style_hypothesis="LONG_TREND",
                horizons=(20, 40, 60, 120, 180),
                winner_definition_ids=(),
            ),
            Phase0BStyleHorizonBindingV1(
                style_hypothesis="UNCLASSIFIED",
                horizons=(),
                winner_definition_ids=(),
            ),
        ),
        market_regime_definitions=(),
        baseline_policy_hash=_HASH,
        primary_metric_family=(metric.metric_id,),
        diagnostic_metric_families=(coverage_metric.metric_id,),
        stationary_bootstrap_policy=Phase0BStationaryBootstrapPolicyV1(),
        spa_policy=Phase0BSpaPolicyV1(),
        by_fdr_policy=Phase0BByFdrPolicyV1(),
        economic_significance_policy=Phase0BEconomicSignificancePolicyV1(
            policy_id="phase0b-economic-significance-v1",
            thresholds=(
                Phase0BEconomicThresholdV1(
                    metric_family=metric.metric_id,
                    minimum_absolute_effect=Decimal("0.01"),
                    output_unit="DECIMAL_RETURN",
                ),
            ),
        ),
        random_policy=Phase0BRandomPolicyV1(),
        numeric_kernel=numeric,
        metric_registry_hash=str(metric_registry.registry_hash),
    )
    return Phase0BCandidateQualityAuditRequestV1(
        snapshot_ids=(target.snapshot_id,),
        audit_targets=(target,),
        dataset_store_identity=store,
        dataset_store_identity_hash=str(store.identity_hash),
        metric_registry=metric_registry,
        metric_registry_hash=str(metric_registry.registry_hash),
        winner_definitions=(winner,),
        winner_definition_set_hash=winner_set_hash,
        numeric_kernel=numeric,
        multiple_testing_registry=testing,
        multiple_testing_registry_hash=str(testing.registry_hash),
        producer_code_closure_hash=_CODE_HASH,
    )


def test_request_closes_full_payloads_without_self_referential_hashes() -> None:
    request = _request()

    assert request.request_hash == canonical_json_sha256(
        request.model_dump(mode="python", exclude={"request_hash"})
    )
    assert request.metric_registry_hash == request.metric_registry.registry_hash
    assert request.dataset_store_identity_hash == request.dataset_store_identity.identity_hash
    assert request.multiple_testing_registry_hash == request.multiple_testing_registry.registry_hash
    assert Phase0BCandidateQualityAuditRequestV1.model_validate(
        request.model_dump(mode="json")
    ) == request


def test_request_rejects_hash_only_or_conflicting_registry_identity() -> None:
    payload = _request().model_dump(mode="python")
    payload["metric_registry_hash"] = "c" * 64
    payload["request_hash"] = None

    with pytest.raises(ValidationError, match="metric_registry_hash differs"):
        Phase0BCandidateQualityAuditRequestV1.model_validate(payload)


def test_request_rejects_snapshot_without_explicit_target() -> None:
    payload = _request().model_dump(mode="python")
    payload["snapshot_ids"] = ("snapshot-1", "snapshot-2")
    payload["request_hash"] = None

    with pytest.raises(ValidationError, match="cover every requested snapshot"):
        Phase0BCandidateQualityAuditRequestV1.model_validate(payload)


def test_request_rejects_duplicate_lineage_with_different_style() -> None:
    request = _request()
    base = request.audit_targets[0]
    duplicate = Phase0BAuditTargetV1(
        snapshot_id=base.snapshot_id,
        program_id=base.program_id,
        package_id=base.package_id,
        manifest_sha256=base.manifest_sha256,
        alpha_mode=base.alpha_mode,
        style_hypothesis="LONG_TREND",
    )
    payload = request.model_dump(mode="python")
    payload["audit_targets"] = (base, duplicate)
    payload["request_hash"] = None

    with pytest.raises(ValidationError, match="cannot bind one snapshot lineage more than once"):
        Phase0BCandidateQualityAuditRequestV1.model_validate(payload)


@pytest.mark.parametrize(
    ("depths", "buckets"),
    [
        ((), ()),
        ((100,), ((1, 100),)),
        ((-1,), ((1, 5),)),
    ],
)
def test_registry_rejects_non_frozen_depths_and_buckets(
    depths: tuple[int, ...],
    buckets: tuple[tuple[int, int], ...],
) -> None:
    payload = _request().multiple_testing_registry.model_dump(mode="python")
    payload["candidate_depths"] = depths
    payload["rank_buckets"] = buckets
    payload["registry_hash"] = None

    with pytest.raises(ValidationError):
        Phase0BMultipleTestingRegistryV1.model_validate(payload)


def test_metric_contract_rejects_missing_semantic_reference() -> None:
    metric = next(item for item in _request().metric_registry.metrics if item.family == "PRIMARY")
    payload = metric.model_dump(mode="python")
    payload.pop("benchmark_policy_ref")
    payload["metric_hash"] = None

    with pytest.raises(ValidationError, match="benchmark_policy_ref"):
        Phase0BMetricDefinitionV1.model_validate(payload)


def test_request_rejects_metric_kernel_reference_drift() -> None:
    request = _request()
    primary = next(item for item in request.metric_registry.metrics if item.family == "PRIMARY")
    metric_payload = primary.model_dump(mode="python")
    metric_payload["numeric_kernel_ref"] = "c" * 64
    metric_payload["metric_hash"] = None
    metric = Phase0BMetricDefinitionV1.model_validate(metric_payload)
    metric_registry = Phase0BMetricRegistryV1(
        metrics=tuple(
            metric if item.metric_id == primary.metric_id else item
            for item in request.metric_registry.metrics
        )
    )
    payload = request.model_dump(mode="python")
    payload["metric_registry"] = metric_registry
    payload["metric_registry_hash"] = metric_registry.registry_hash
    registry_payload = request.multiple_testing_registry.model_dump(mode="python")
    registry_payload["metric_registry_hash"] = metric_registry.registry_hash
    registry_payload["registry_hash"] = None
    registry = Phase0BMultipleTestingRegistryV1.model_validate(registry_payload)
    payload["multiple_testing_registry"] = registry
    payload["multiple_testing_registry_hash"] = registry.registry_hash
    payload["request_hash"] = None

    with pytest.raises(ValidationError, match="numeric kernel reference"):
        Phase0BCandidateQualityAuditRequestV1.model_validate(payload)


def test_metric_registry_preserves_multiple_formula_variants_without_hash_collision() -> None:
    metric = next(item for item in _request().metric_registry.metrics if item.family == "PRIMARY")
    payload = metric.model_dump(mode="python")
    payload.update(
        {
            "projection": "RETURN_GROSS",
            "cash_policy": "RETURN_ZERO",
            "metric_hash": None,
        }
    )
    variant = Phase0BMetricDefinitionV1.model_validate(payload)

    registry = Phase0BMetricRegistryV1(metrics=(metric, variant))

    assert len(registry.metrics) == 2
    assert len({item.metric_hash for item in registry.metrics}) == 2
