from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import require_sha256
from backend.services.advisory_historical_range.summary_service import (
    Phase1WinnerDefinitionV1,
)
from backend.services.advisory_phase1.dataset_store import (
    LOCAL_DATASET_STORE_SCHEMA_VERSION,
    LocalContentAddressedStore,
)
from backend.services.advisory_phase1.label_policy import Projection


AUDIT_REQUEST_SCHEMA_VERSION = "advisory_phase0b_candidate_quality_audit_request_v1"
AUDIT_TARGET_SCHEMA_VERSION = "advisory_phase0b_audit_target_v1"
DATASET_STORE_IDENTITY_SCHEMA_VERSION = "advisory_phase0b_dataset_store_identity_v1"
METRIC_DEFINITION_SCHEMA_VERSION = "phase0b_metric_definition_v1"
METRIC_REGISTRY_SCHEMA_VERSION = "phase0b_metric_registry_v1"
MULTIPLE_TESTING_SCHEMA_VERSION = "phase0b_multiple_testing_registry_v1"
NUMERIC_KERNEL_SCHEMA_VERSION = "phase0b_numeric_kernel_v1"
OUTPUT_SCHEMA_VERSION = "advisory_phase0b_candidate_quality_report_v1"
SAMPLE_POLICY_VERSION = "phase0b_sample_policy_v1"

CANDIDATE_DEPTHS = (5, 10, 20)
RANK_BUCKETS = ((1, 5), (6, 10), (11, 20))
STAGE_ABLATIONS = (
    "alpha_raw",
    "hmm_adjusted",
    "risk_policy_adjusted",
    "selection_effective",
)

StageName = Literal[
    "alpha_raw",
    "hmm_adjusted",
    "risk_policy_adjusted",
    "selection_effective",
]
CandidateDepths = tuple[Literal[5], Literal[10], Literal[20]]
RankBuckets = tuple[
    tuple[Literal[1], Literal[5]],
    tuple[Literal[6], Literal[10]],
    tuple[Literal[11], Literal[20]],
]
StageAblations = tuple[
    Literal["alpha_raw"],
    Literal["hmm_adjusted"],
    Literal["risk_policy_adjusted"],
    Literal["selection_effective"],
]


class _FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuditStyleHypothesis(str, Enum):
    SHORT_REBOUND = "SHORT_REBOUND"
    LONG_TREND = "LONG_TREND"
    UNCLASSIFIED = "UNCLASSIFIED"


class MetricStatus(str, Enum):
    AVAILABLE = "AVAILABLE"
    INSUFFICIENT_SAMPLE = "INSUFFICIENT_SAMPLE"
    INPUT_CAPABILITY_NOT_AVAILABLE = "INPUT_CAPABILITY_NOT_AVAILABLE"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class Phase0BAuditTargetV1(_FrozenModel):
    schema_version: Literal[AUDIT_TARGET_SCHEMA_VERSION] = AUDIT_TARGET_SCHEMA_VERSION
    snapshot_id: str = Field(min_length=1, max_length=160)
    program_id: str = Field(min_length=1, max_length=160)
    package_id: str = Field(min_length=1, max_length=160)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    alpha_mode: Literal["single_alpha", "multi_alpha"]
    style_hypothesis: AuditStyleHypothesis
    target_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("snapshot_id", "program_id", "package_id")
    @classmethod
    def _identifiers(cls, value: str, info: Any) -> str:
        if value != value.strip():
            raise ValueError(f"{info.field_name} must not contain surrounding whitespace")
        return value

    @field_validator("manifest_sha256", "target_hash")
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BAuditTargetV1":
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"target_hash"}))
        if self.target_hash is not None and self.target_hash != digest:
            raise ValueError("target_hash differs from canonical audit target")
        object.__setattr__(self, "target_hash", digest)
        return self


class Phase0BDatasetStoreIdentityV1(_FrozenModel):
    schema_version: Literal[DATASET_STORE_IDENTITY_SCHEMA_VERSION] = (
        DATASET_STORE_IDENTITY_SCHEMA_VERSION
    )
    dataset_store_schema_version: Literal[LOCAL_DATASET_STORE_SCHEMA_VERSION] = (
        LOCAL_DATASET_STORE_SCHEMA_VERSION
    )
    durability_mode: str = Field(min_length=1, max_length=160)
    atomic_publish_mode: Literal["HARDLINK_CREATE_IF_ABSENT_V1"] = (
        "HARDLINK_CREATE_IF_ABSENT_V1"
    )
    identity_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @classmethod
    def from_authoritative_factory(cls) -> "Phase0BDatasetStoreIdentityV1":
        from backend.services.advisory_historical_range.runtime_factories import (
            historical_range_store_identity,
        )

        return cls(**historical_range_store_identity())

    @field_validator("identity_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="identity_hash") if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BDatasetStoreIdentityV1":
        expected_durability = LocalContentAddressedStore.expected_durability_mode()
        if self.durability_mode != expected_durability:
            raise ValueError(
                "dataset store durability mode differs from the authoritative runtime factory"
            )
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"identity_hash"}))
        if self.identity_hash is not None and self.identity_hash != digest:
            raise ValueError("dataset store identity hash differs from canonical content")
        object.__setattr__(self, "identity_hash", digest)
        return self


class Phase0BMetricDefinitionV1(_FrozenModel):
    schema_version: Literal[METRIC_DEFINITION_SCHEMA_VERSION] = METRIC_DEFINITION_SCHEMA_VERSION
    metric_id: str = Field(min_length=1, max_length=160)
    family: Literal["PRIMARY", "DIAGNOSTIC", "COVERAGE"]
    projection: Projection
    horizon_source: Literal["LABEL_POLICY", "WINNER_DEFINITION"]
    horizons: tuple[int, ...] = Field(min_length=1)
    stages: tuple[StageName, ...] = Field(min_length=1)
    depths: tuple[int, ...] = Field(min_length=1)
    aggregation_unit: Literal["DECISION_DATE_EQUAL_WEIGHT"] = "DECISION_DATE_EQUAL_WEIGHT"
    cash_policy: Literal[
        "RETURN_ZERO",
        "NET_EXCESS_NEGATIVE_BENCHMARK",
        "PATH_ZERO_DIAGNOSTIC",
        "PRECISION_EMPTY_FAILURE",
        "NDCG_EMPTY_ZERO_GAIN",
        "NOT_APPLICABLE",
    ]
    maturity_eligibility: tuple[str, ...] = Field(min_length=1)
    event_eligibility: tuple[str, ...] = Field(min_length=1)
    winner_definition_ids: tuple[str, ...] = ()
    benchmark_policy_ref: str = Field(min_length=1, max_length=160)
    cost_policy_ref: str = Field(min_length=1, max_length=160)
    sample_policy_ref: Literal[SAMPLE_POLICY_VERSION] = SAMPLE_POLICY_VERSION
    numeric_kernel_ref: str = Field(min_length=64, max_length=64)
    output_unit: Literal["DECIMAL_RETURN", "RATIO", "COUNT", "CORRELATION"]
    metric_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "metric_id",
        "benchmark_policy_ref",
        "cost_policy_ref",
    )
    @classmethod
    def _semantic_refs(cls, value: str, info: Any) -> str:
        if not value.strip() or value != value.strip():
            raise ValueError(f"{info.field_name} must be non-empty without surrounding whitespace")
        return value

    @field_validator("horizons", "depths")
    @classmethod
    def _positive_unique(cls, value: tuple[int, ...], info: Any) -> tuple[int, ...]:
        normalized = tuple(sorted(set(value)))
        if len(normalized) != len(value) or any(item <= 0 for item in normalized):
            raise ValueError(f"{info.field_name} must contain positive unique integers")
        return normalized

    @field_validator(
        "stages",
        "maturity_eligibility",
        "event_eligibility",
        "winner_definition_ids",
    )
    @classmethod
    def _unique_strings(cls, value: tuple[Any, ...], info: Any) -> tuple[Any, ...]:
        if len(value) != len(set(value)) or any(
            isinstance(item, str) and (not item.strip() or item != item.strip())
            for item in value
        ):
            raise ValueError(f"{info.field_name} values must be unique")
        return tuple(sorted(value))

    @field_validator("numeric_kernel_ref", "metric_hash")
    @classmethod
    def _hash(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BMetricDefinitionV1":
        if self.family == "COVERAGE":
            if set(self.maturity_eligibility) != {
                "MATURED",
                "PENDING",
                "RIGHT_CENSORED",
                "UNAVAILABLE",
            }:
                raise ValueError("coverage metrics must retain every frozen maturity state")
            if set(self.event_eligibility) != {"TERMINAL", "NON_TERMINAL"}:
                raise ValueError("coverage metrics must retain terminal and non-terminal events")
        else:
            if self.maturity_eligibility != ("MATURED",):
                raise ValueError("evaluable metrics require exactly MATURED eligibility")
            if self.event_eligibility != ("TERMINAL",):
                raise ValueError("evaluable metrics require exactly TERMINAL eligibility")
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"metric_hash"}))
        if self.metric_hash is not None and self.metric_hash != digest:
            raise ValueError("metric_hash differs from canonical metric definition")
        object.__setattr__(self, "metric_hash", digest)
        return self


class Phase0BMetricRegistryV1(_FrozenModel):
    schema_version: Literal[METRIC_REGISTRY_SCHEMA_VERSION] = METRIC_REGISTRY_SCHEMA_VERSION
    metrics: tuple[Phase0BMetricDefinitionV1, ...] = Field(min_length=1)
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("registry_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="registry_hash") if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BMetricRegistryV1":
        metrics = tuple(
            sorted(self.metrics, key=lambda item: (item.metric_id, str(item.metric_hash)))
        )
        if len({item.metric_hash for item in metrics}) != len(metrics):
            raise ValueError("metric registry definitions must have unique semantic identities")
        object.__setattr__(self, "metrics", metrics)
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"registry_hash"}))
        if self.registry_hash is not None and self.registry_hash != digest:
            raise ValueError("metric registry hash differs from canonical content")
        object.__setattr__(self, "registry_hash", digest)
        return self


class Phase0BNumericKernelV1(_FrozenModel):
    schema_version: Literal[NUMERIC_KERNEL_SCHEMA_VERSION] = NUMERIC_KERNEL_SCHEMA_VERSION
    canonical_serializer: Literal["AISTOCK_CANONICAL_JSON_V1"] = "AISTOCK_CANONICAL_JSON_V1"
    decimal_scale: Literal[12] = 12
    price_scale: Literal[6] = 6
    decimal_rounding: Literal["ROUND_HALF_EVEN"] = "ROUND_HALF_EVEN"
    float_kernel: Literal["IEEE754_FLOAT64"] = "IEEE754_FLOAT64"
    random_policy: Literal["SHA256_COUNTER_SORT_V1"] = "SHA256_COUNTER_SORT_V1"
    bootstrap_policy: Literal["POLITIS_ROMANO_CIRCULAR_V1"] = (
        "POLITIS_ROMANO_CIRCULAR_V1"
    )
    quantile_policy: Literal["NEAREST_RANK_V1"] = "NEAREST_RANK_V1"
    tie_policy: Literal["AVERAGE_RANK_V1"] = "AVERAGE_RANK_V1"
    kernel_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator("kernel_hash")
    @classmethod
    def _hash(cls, value: str | None) -> str | None:
        return require_sha256(value, field_name="kernel_hash") if value is not None else None

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BNumericKernelV1":
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"kernel_hash"}))
        if self.kernel_hash is not None and self.kernel_hash != digest:
            raise ValueError("numeric kernel hash differs from canonical content")
        object.__setattr__(self, "kernel_hash", digest)
        return self


class Phase0BTargetStyleBindingV1(_FrozenModel):
    target_hash: str = Field(min_length=64, max_length=64)
    style_hypothesis: AuditStyleHypothesis

    @field_validator("target_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return require_sha256(value, field_name="target_hash")


class Phase0BTargetRuntimeVariantBindingV1(_FrozenModel):
    target_hash: str = Field(min_length=64, max_length=64)
    manifest_sha256: str = Field(min_length=64, max_length=64)
    runtime_variant_id: str = Field(min_length=1, max_length=160)

    @field_validator("target_hash", "manifest_sha256")
    @classmethod
    def _hashes(cls, value: str, info: Any) -> str:
        return require_sha256(value, field_name=info.field_name)

    @field_validator("runtime_variant_id")
    @classmethod
    def _runtime_variant_id(cls, value: str) -> str:
        if value != value.strip():
            raise ValueError("runtime_variant_id must not contain surrounding whitespace")
        return value


class Phase0BStyleHorizonBindingV1(_FrozenModel):
    style_hypothesis: AuditStyleHypothesis
    horizons: tuple[int, ...]
    winner_definition_ids: tuple[str, ...]

    @model_validator(mode="after")
    def _validate_style_family(self) -> "Phase0BStyleHorizonBindingV1":
        expected_horizons = {
            AuditStyleHypothesis.SHORT_REBOUND: (1, 3, 5, 10, 20),
            AuditStyleHypothesis.LONG_TREND: (20, 40, 60, 120, 180),
            AuditStyleHypothesis.UNCLASSIFIED: (),
        }[self.style_hypothesis]
        if self.horizons != expected_horizons:
            raise ValueError("style horizons differ from the frozen Phase 0B search family")
        if len(self.winner_definition_ids) != len(set(self.winner_definition_ids)):
            raise ValueError("style winner definition ids must be unique")
        if self.style_hypothesis is AuditStyleHypothesis.UNCLASSIFIED and self.winner_definition_ids:
            raise ValueError("UNCLASSIFIED style cannot select a winner family")
        object.__setattr__(self, "winner_definition_ids", tuple(sorted(self.winner_definition_ids)))
        return self


class Phase0BMarketRegimeDefinitionV1(_FrozenModel):
    regime_definition_id: str = Field(min_length=1, max_length=160)
    regime_value: str = Field(min_length=1, max_length=160)
    evidence_value_field: Literal["market_regime_at_t"] = "market_regime_at_t"
    evidence_hash_field: Literal["market_regime_evidence_hash"] = "market_regime_evidence_hash"
    missing_evidence_policy: Literal["INPUT_CAPABILITY_NOT_AVAILABLE"] = (
        "INPUT_CAPABILITY_NOT_AVAILABLE"
    )

    @field_validator("regime_definition_id", "regime_value")
    @classmethod
    def _identifiers(cls, value: str, info: Any) -> str:
        if not value.strip() or value != value.strip():
            raise ValueError(f"{info.field_name} must be non-empty without surrounding whitespace")
        return value


class Phase0BStationaryBootstrapPolicyV1(_FrozenModel):
    policy_id: Literal["POLITIS_ROMANO_CIRCULAR_V1"] = "POLITIS_ROMANO_CIRCULAR_V1"
    replicates: Literal[5000] = 5000
    seed_policy: Literal["MULTIPLE_TESTING_REGISTRY_HASH_UINT32_V1"] = (
        "MULTIPLE_TESTING_REGISTRY_HASH_UINT32_V1"
    )
    expected_block_length_policy: Literal["CLAMP_ROUND_N_CUBERT_5_60_V1"] = (
        "CLAMP_ROUND_N_CUBERT_5_60_V1"
    )
    draw_policy: Literal["SHA256_POSITIONAL_UINT64_V1"] = "SHA256_POSITIONAL_UINT64_V1"
    quantile_policy: Literal["NEAREST_RANK_025_975_V1"] = "NEAREST_RANK_025_975_V1"


class Phase0BSpaPolicyV1(_FrozenModel):
    policy_id: Literal["HANSEN_SPA_ONE_SIDED_V1"] = "HANSEN_SPA_ONE_SIDED_V1"
    alpha: Literal["0.05"] = "0.05"
    minimum_decision_dates: Literal[252] = 252


class Phase0BByFdrPolicyV1(_FrozenModel):
    policy_id: Literal["BENJAMINI_YEKUTIELI_V1"] = "BENJAMINI_YEKUTIELI_V1"
    q: Literal["0.10"] = "0.10"
    minimum_decision_dates: Literal[252] = 252


class Phase0BEconomicThresholdV1(_FrozenModel):
    metric_family: str = Field(min_length=1, max_length=160)
    minimum_absolute_effect: Decimal = Field(ge=0)
    output_unit: Literal["DECIMAL_RETURN", "RATIO", "COUNT", "CORRELATION"]


class Phase0BEconomicSignificancePolicyV1(_FrozenModel):
    policy_id: str = Field(min_length=1, max_length=160)
    thresholds: tuple[Phase0BEconomicThresholdV1, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_families(self) -> "Phase0BEconomicSignificancePolicyV1":
        thresholds = tuple(sorted(self.thresholds, key=lambda item: item.metric_family))
        if len({item.metric_family for item in thresholds}) != len(thresholds):
            raise ValueError("economic significance metric families must be unique")
        object.__setattr__(self, "thresholds", thresholds)
        return self


class Phase0BRandomPolicyV1(_FrozenModel):
    policy_id: Literal["SHA256_COUNTER_SORT_V1"] = "SHA256_COUNTER_SORT_V1"
    portfolio_size: Literal[5] = 5
    replicates: Literal[1000] = 1000
    seed_material_policy: Literal[
        "REQUEST_SNAPSHOT_SIGNAL_PROJECTION_HORIZON_V1"
    ] = "REQUEST_SNAPSHOT_SIGNAL_PROJECTION_HORIZON_V1"
    without_replacement: Literal[True] = True


class Phase0BMultipleTestingRegistryV1(_FrozenModel):
    schema_version: Literal[MULTIPLE_TESTING_SCHEMA_VERSION] = MULTIPLE_TESTING_SCHEMA_VERSION
    audit_target_identity_set_hash: str = Field(min_length=64, max_length=64)
    style_hypothesis_by_target: tuple[Phase0BTargetStyleBindingV1, ...] = Field(min_length=1)
    manifest_runtime_variant_by_target: tuple[
        Phase0BTargetRuntimeVariantBindingV1, ...
    ] = Field(min_length=1)
    winner_definition_set_hash: str = Field(min_length=64, max_length=64)
    horizons_by_style: tuple[Phase0BStyleHorizonBindingV1, ...] = Field(min_length=3)
    candidate_depths: CandidateDepths = CANDIDATE_DEPTHS
    stage_ablations: StageAblations = STAGE_ABLATIONS
    rank_buckets: RankBuckets = RANK_BUCKETS
    market_regime_definitions: tuple[Phase0BMarketRegimeDefinitionV1, ...]
    primary_baseline_id: Literal["SELECTION_EFFECTIVE_TOP5_CASH_PADDED_V1"] = (
        "SELECTION_EFFECTIVE_TOP5_CASH_PADDED_V1"
    )
    baseline_policy_hash: str = Field(min_length=64, max_length=64)
    primary_metric_family: tuple[str, ...] = Field(min_length=1)
    diagnostic_metric_families: tuple[str, ...] = Field(min_length=1)
    stationary_bootstrap_policy: Phase0BStationaryBootstrapPolicyV1
    spa_policy: Phase0BSpaPolicyV1
    by_fdr_policy: Phase0BByFdrPolicyV1
    economic_significance_policy: Phase0BEconomicSignificancePolicyV1
    random_policy: Phase0BRandomPolicyV1
    numeric_kernel: Phase0BNumericKernelV1
    metric_registry_hash: str = Field(min_length=64, max_length=64)
    minimum_descriptive_dates: Literal[60] = 60
    minimum_inferential_dates: Literal[252] = 252
    minimum_recall_winner_events: Literal[50] = 50
    registry_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "audit_target_identity_set_hash",
        "winner_definition_set_hash",
        "baseline_policy_hash",
        "metric_registry_hash",
        "registry_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @field_validator("primary_metric_family", "diagnostic_metric_families")
    @classmethod
    def _metric_families(cls, value: tuple[str, ...], info: Any) -> tuple[str, ...]:
        normalized = tuple(sorted(value))
        if len(normalized) != len(set(normalized)) or any(not item.strip() for item in normalized):
            raise ValueError(f"{info.field_name} must contain unique non-empty ids")
        return normalized

    @model_validator(mode="after")
    def _identity(self) -> "Phase0BMultipleTestingRegistryV1":
        style_bindings = tuple(sorted(self.style_hypothesis_by_target, key=lambda item: item.target_hash))
        runtime_bindings = tuple(
            sorted(self.manifest_runtime_variant_by_target, key=lambda item: item.target_hash)
        )
        style_horizons = tuple(sorted(self.horizons_by_style, key=lambda item: item.style_hypothesis.value))
        regimes = tuple(sorted(self.market_regime_definitions, key=lambda item: item.regime_definition_id))
        for label, values in (
            ("style_hypothesis_by_target", style_bindings),
            ("manifest_runtime_variant_by_target", runtime_bindings),
        ):
            if len({item.target_hash for item in values}) != len(values):
                raise ValueError(f"{label} target identities must be unique")
        if {item.style_hypothesis for item in style_horizons} != set(AuditStyleHypothesis):
            raise ValueError("horizons_by_style must contain every frozen style family exactly once")
        if len({item.style_hypothesis for item in style_horizons}) != len(style_horizons):
            raise ValueError("horizons_by_style contains duplicate style families")
        if len({item.regime_definition_id for item in regimes}) != len(regimes):
            raise ValueError("market regime definition ids must be unique")
        if len({item.regime_value for item in regimes}) != len(regimes):
            raise ValueError("market regime values must be unique")
        if set(self.primary_metric_family) & set(self.diagnostic_metric_families):
            raise ValueError("primary and diagnostic metric families must be disjoint")
        object.__setattr__(self, "style_hypothesis_by_target", style_bindings)
        object.__setattr__(self, "manifest_runtime_variant_by_target", runtime_bindings)
        object.__setattr__(self, "horizons_by_style", style_horizons)
        object.__setattr__(self, "market_regime_definitions", regimes)
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"registry_hash"}))
        if self.registry_hash is not None and self.registry_hash != digest:
            raise ValueError("multiple-testing registry hash differs from canonical content")
        object.__setattr__(self, "registry_hash", digest)
        return self


def _identity_set_hash(values: tuple[str, ...]) -> str:
    return canonical_json_sha256(tuple(sorted(values)))


def _winner_matches_style(
    winner: Phase1WinnerDefinitionV1,
    style: AuditStyleHypothesis,
) -> bool:
    if style is AuditStyleHypothesis.UNCLASSIFIED:
        return False
    if style is AuditStyleHypothesis.SHORT_REBOUND:
        if winner.horizon_trade_days not in {1, 3, 5, 10, 20}:
            return False
        return (
            winner.projection == Projection.RETURN_NET_EXCESS.value
            and winner.comparison_operator == "GT"
            and winner.threshold == Decimal("0")
        ) or (
            winner.projection == Projection.EXECUTABLE_MFE.value
            and winner.comparison_operator == "GTE"
            and winner.threshold in {Decimal("0.05"), Decimal("0.10")}
        )
    return (
        winner.horizon_trade_days in {20, 40, 60, 120, 180}
        and winner.projection == Projection.EXECUTABLE_MFE.value
        and winner.comparison_operator == "GTE"
        and winner.threshold in {Decimal("0.30"), Decimal("0.50"), Decimal("0.70")}
    )


class Phase0BCandidateQualityAuditRequestV1(_FrozenModel):
    schema_version: Literal[AUDIT_REQUEST_SCHEMA_VERSION] = AUDIT_REQUEST_SCHEMA_VERSION
    snapshot_ids: tuple[str, ...] = Field(min_length=1)
    audit_targets: tuple[Phase0BAuditTargetV1, ...] = Field(min_length=1)
    dataset_store_identity: Phase0BDatasetStoreIdentityV1
    dataset_store_identity_hash: str = Field(min_length=64, max_length=64)
    metric_registry: Phase0BMetricRegistryV1
    metric_registry_hash: str = Field(min_length=64, max_length=64)
    winner_definitions: tuple[Phase1WinnerDefinitionV1, ...] = ()
    winner_definition_set_hash: str = Field(min_length=64, max_length=64)
    candidate_depths: CandidateDepths = CANDIDATE_DEPTHS
    rank_buckets: RankBuckets = RANK_BUCKETS
    random_portfolio_size: Literal[5] = 5
    random_replicates: Literal[1000] = 1000
    random_seed_policy: Literal["SHA256_COUNTER_SORT_V1"] = "SHA256_COUNTER_SORT_V1"
    multiple_testing_registry: Phase0BMultipleTestingRegistryV1
    multiple_testing_registry_hash: str = Field(min_length=64, max_length=64)
    stationary_bootstrap_replicates: Literal[5000] = 5000
    stationary_bootstrap_seed_policy: Literal[
        "PHASE0A_MULTIPLE_TESTING_REGISTRY_HASH_V1"
    ] = "PHASE0A_MULTIPLE_TESTING_REGISTRY_HASH_V1"
    stationary_bootstrap_block_policy: Literal["POLITIS_ROMANO_CIRCULAR_V1"] = (
        "POLITIS_ROMANO_CIRCULAR_V1"
    )
    numeric_kernel: Phase0BNumericKernelV1
    sample_policy_version: Literal[SAMPLE_POLICY_VERSION] = SAMPLE_POLICY_VERSION
    output_schema_version: Literal[OUTPUT_SCHEMA_VERSION] = OUTPUT_SCHEMA_VERSION
    producer_code_closure_hash: str = Field(min_length=64, max_length=64)
    request_hash: str | None = Field(default=None, min_length=64, max_length=64)

    @field_validator(
        "dataset_store_identity_hash",
        "metric_registry_hash",
        "winner_definition_set_hash",
        "multiple_testing_registry_hash",
        "producer_code_closure_hash",
        "request_hash",
    )
    @classmethod
    def _hashes(cls, value: str | None, info: Any) -> str | None:
        return require_sha256(value, field_name=info.field_name) if value is not None else None

    @model_validator(mode="after")
    def _closure(self) -> "Phase0BCandidateQualityAuditRequestV1":
        snapshot_ids = tuple(sorted(set(self.snapshot_ids)))
        if len(snapshot_ids) != len(self.snapshot_ids) or any(
            not item.strip() or item != item.strip() for item in snapshot_ids
        ):
            raise ValueError("snapshot_ids must be non-empty and unique without surrounding whitespace")
        targets = tuple(sorted(self.audit_targets, key=lambda item: str(item.target_hash)))
        if len({item.target_hash for item in targets}) != len(targets):
            raise ValueError("audit_targets must have unique identities")
        lineage_keys = {
            (
                item.snapshot_id,
                item.program_id,
                item.package_id,
                item.manifest_sha256,
                item.alpha_mode,
            )
            for item in targets
        }
        if len(lineage_keys) != len(targets):
            raise ValueError("audit_targets cannot bind one snapshot lineage more than once")
        if {item.snapshot_id for item in targets} != set(snapshot_ids):
            raise ValueError("audit targets must cover every requested snapshot")
        winners = tuple(
            sorted(self.winner_definitions, key=lambda item: str(item.winner_definition_hash))
        )
        if len({item.winner_definition_hash for item in winners}) != len(winners):
            raise ValueError("winner definitions must have unique identities")
        winner_by_id = {item.winner_definition_id: item for item in winners}
        if len(winner_by_id) != len(winners):
            raise ValueError("winner definition ids must be unique")
        expected_target_set_hash = _identity_set_hash(
            tuple(str(item.target_hash) for item in targets)
        )
        expected_winner_set_hash = _identity_set_hash(
            tuple(str(item.winner_definition_hash) for item in winners)
        )
        expected = {
            "dataset_store_identity_hash": self.dataset_store_identity.identity_hash,
            "metric_registry_hash": self.metric_registry.registry_hash,
            "winner_definition_set_hash": expected_winner_set_hash,
            "multiple_testing_registry_hash": self.multiple_testing_registry.registry_hash,
        }
        for field_name, expected_value in expected.items():
            if getattr(self, field_name) != expected_value:
                raise ValueError(f"{field_name} differs from its canonical payload")
        registry = self.multiple_testing_registry
        if (
            registry.audit_target_identity_set_hash != expected_target_set_hash
            or registry.winner_definition_set_hash != expected_winner_set_hash
            or registry.metric_registry_hash != self.metric_registry.registry_hash
            or registry.numeric_kernel != self.numeric_kernel
            or registry.candidate_depths != self.candidate_depths
            or registry.rank_buckets != self.rank_buckets
            or registry.random_policy.portfolio_size != self.random_portfolio_size
            or registry.random_policy.replicates != self.random_replicates
            or registry.random_policy.policy_id != self.random_seed_policy
            or registry.stationary_bootstrap_policy.replicates
            != self.stationary_bootstrap_replicates
            or registry.stationary_bootstrap_policy.policy_id
            != self.stationary_bootstrap_block_policy
        ):
            raise ValueError("multiple-testing registry does not close request identities and policies")
        target_by_hash = {str(item.target_hash): item for item in targets}
        style_by_target = {item.target_hash: item.style_hypothesis for item in registry.style_hypothesis_by_target}
        runtime_by_target = {
            item.target_hash: item for item in registry.manifest_runtime_variant_by_target
        }
        if set(style_by_target) != set(target_by_hash) or set(runtime_by_target) != set(target_by_hash):
            raise ValueError("multiple-testing target bindings must exactly cover audit targets")
        for target_hash, target in target_by_hash.items():
            if style_by_target[target_hash] is not target.style_hypothesis:
                raise ValueError("multiple-testing style binding differs from audit target")
            if runtime_by_target[target_hash].manifest_sha256 != target.manifest_sha256:
                raise ValueError("runtime variant binding differs from audit target manifest")
        style_horizons = {item.style_hypothesis: item for item in registry.horizons_by_style}
        active_styles = {item.style_hypothesis for item in targets}
        for style in active_styles - {AuditStyleHypothesis.UNCLASSIFIED}:
            if not style_horizons[style].winner_definition_ids:
                raise ValueError("classified audit targets require a frozen winner family")
        for style, binding in style_horizons.items():
            for winner_id in binding.winner_definition_ids:
                winner = winner_by_id.get(winner_id)
                if winner is None or not _winner_matches_style(winner, style):
                    raise ValueError("style winner family is missing or violates the frozen definition")
        referenced_winner_ids = {
            item for binding in style_horizons.values() for item in binding.winner_definition_ids
        }
        if referenced_winner_ids != set(winner_by_id):
            raise ValueError("winner definitions must be referenced by exactly one frozen style family")
        allowed_horizons = {
            horizon
            for target in targets
            for horizon in style_horizons[target.style_hypothesis].horizons
        }
        metrics = self.metric_registry.metrics
        expected_primary_metrics = {item.metric_id for item in metrics if item.family == "PRIMARY"}
        expected_diagnostic_metrics = {
            item.metric_id for item in metrics if item.family in {"DIAGNOSTIC", "COVERAGE"}
        }
        if set(registry.primary_metric_family) != expected_primary_metrics:
            raise ValueError("primary metric family does not exactly close the metric registry")
        if set(registry.diagnostic_metric_families) != expected_diagnostic_metrics:
            raise ValueError("diagnostic metric families do not exactly close the metric registry")
        economic_threshold_by_family = {
            item.metric_family: item for item in registry.economic_significance_policy.thresholds
        }
        economic_threshold_families = set(economic_threshold_by_family)
        if not expected_primary_metrics.issubset(economic_threshold_families):
            raise ValueError("economic significance policy must cover every primary metric")
        for metric in metrics:
            threshold = economic_threshold_by_family.get(metric.metric_id)
            if threshold is not None and threshold.output_unit != metric.output_unit:
                raise ValueError("economic significance output unit differs from metric definition")
            if metric.numeric_kernel_ref != self.numeric_kernel.kernel_hash:
                raise ValueError("metric numeric kernel reference differs from request kernel")
            if not set(metric.depths).issubset(set(self.candidate_depths)):
                raise ValueError("metric depth is outside the frozen candidate depths")
            if not set(metric.winner_definition_ids).issubset(set(winner_by_id)):
                raise ValueError("metric references an unknown winner definition")
            if metric.horizon_source == "WINNER_DEFINITION":
                if len(metric.winner_definition_ids) != 1:
                    raise ValueError("each winner-sourced metric variant requires exactly one winner")
                if any(
                    winner_by_id[winner_id].projection != metric.projection.value
                    for winner_id in metric.winner_definition_ids
                ):
                    raise ValueError("winner metric projection differs from its winner definition")
                referenced_horizons = {
                    winner_by_id[winner_id].horizon_trade_days
                    for winner_id in metric.winner_definition_ids
                }
                if set(metric.horizons) != referenced_horizons:
                    raise ValueError("winner-sourced metric horizons differ from winner definitions")
                if not set(metric.horizons).issubset(allowed_horizons):
                    raise ValueError("winner metric horizon is outside active frozen style families")
        object.__setattr__(self, "snapshot_ids", snapshot_ids)
        object.__setattr__(self, "audit_targets", targets)
        object.__setattr__(self, "winner_definitions", winners)
        digest = canonical_json_sha256(self.model_dump(mode="python", exclude={"request_hash"}))
        if self.request_hash is not None and self.request_hash != digest:
            raise ValueError("request_hash differs from canonical request")
        object.__setattr__(self, "request_hash", digest)
        return self
