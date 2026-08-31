"""Pure O4 builders for pre-observation scopes and exact Program source requirements."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AdvisorySourceMappingRegistry,
    AdvisorySourceObservationScopeRequest,
    AdvisoryStrategyPackageInputProjectionV1,
    AggregateInputReadiness,
    AlphaMode,
    ExpectedLogicalInput,
    O4ArtifactKind,
    PartitionGranularity,
    Phase1EProgramDateInput,
    Phase1EProgramCompilerDependency,
    Phase1EProgramInputUnit,
    Phase1ERealInputBundle,
    ProgramCapacityStatus,
    ProgramIdentityReadiness,
    ProgramPlanReadiness,
    ProgramSourceReadiness,
    ProgramSourceRequirementSet,
    REASON_SOURCE_MAPPING_CONFLICT,
    REASON_SOURCE_MAPPING_MISSING,
    RealDevOnboardingError,
    STRATEGY_PACKAGE_PIT_UNIVERSE_KEY,
    STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_PAYLOAD,
    SourcePartitionRequirement,
    validate_sha256,
)
from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.source_capacity import Phase1ECapacityProgramCoverageV1
from backend.services.advisory_phase1.readiness_plan import (
    Phase1EProgramDateRequest,
    Phase1ERevalidationBatchRequest,
)


_WINDOWED_ROLES = frozenset({"market_history", "fundamental_moneyflow", "trading_calendar"})


class ProgramWindowLineage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    alpha_component_id: str | None = Field(default=None, min_length=1, max_length=160)
    window_start_date: date
    effective_trade_date: date
    required_window: int = Field(ge=1)
    window_resolution: str = Field(min_length=1, max_length=80)
    window_lineage_hash: str = Field(min_length=64, max_length=64)
    trading_dates: tuple[date, ...] = Field(min_length=1)

    @field_validator("window_lineage_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return validate_sha256(value, field_name="window_lineage_hash")

    @model_validator(mode="after")
    def _coherent(self) -> "ProgramWindowLineage":
        if self.window_start_date > self.effective_trade_date:
            raise ValueError("window_start_date must not follow effective_trade_date")
        if tuple(sorted(set(self.trading_dates))) != self.trading_dates:
            raise ValueError("trading_dates must be sorted and duplicate-free")
        if self.trading_dates[0] != self.window_start_date or self.trading_dates[-1] != self.effective_trade_date:
            raise ValueError("trading_dates must exactly cover the declared window endpoints")
        if len(self.trading_dates) < self.required_window:
            raise ValueError("trading_dates do not satisfy required_window")
        return self


class PersistedDseSourceReadReceipt(BaseModel):
    """Advisory-owned DTO for the persisted DSE source receipt contract."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_role: str = Field(min_length=1, max_length=120)
    dataset_id: str = Field(min_length=1, max_length=240)
    partition_ref: str = Field(min_length=1, max_length=400)
    query_template_id: str = Field(min_length=1, max_length=160)
    query_template_version: str = Field(min_length=1, max_length=80)
    parameter_hash: str = Field(min_length=64, max_length=64)
    row_count: int = Field(ge=0)
    content_hash: str = Field(min_length=64, max_length=64)
    available_at: datetime | None = None
    first_observed_at: datetime | None = None
    admissibility: str = Field(min_length=1, max_length=120)
    leg_id: str | None = Field(default=None, min_length=1, max_length=160)

    @field_validator("parameter_hash", "content_hash")
    @classmethod
    def _hash(cls, value: str) -> str:
        return validate_sha256(value, field_name="content_hash")

    @field_validator("available_at", "first_observed_at")
    @classmethod
    def _time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("DSE source receipt timestamps must be timezone-aware")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def _availability(self) -> "PersistedDseSourceReadReceipt":
        if self.available_at is None and self.first_observed_at is None:
            raise ValueError("DSE source receipt requires available_at or first_observed_at")
        return self


def build_pre_observation_scope(
    *,
    target_database_identity_hash: str,
    program_id: str,
    decision_trade_date: date,
    pit_universe_key: str,
    style_family: str,
    binding_version_id: str,
    binding_payload_hash: str,
    selection_normalized_config_hash: str,
    projection: AdvisoryStrategyPackageInputProjectionV1,
    projection_ref: AdvisoryImmutableArtifactRef,
    mapping_registry: AdvisorySourceMappingRegistry,
    mapping_registry_ref: AdvisoryImmutableArtifactRef,
    source_query_registry_ref: AdvisoryImmutableArtifactRef,
    source_query_registry_hash: str,
    window_policy_ref: AdvisoryImmutableArtifactRef,
    window_policy_hash: str,
    decision_cutoff_ts: datetime,
    window_lineages: tuple[ProgramWindowLineage, ...],
) -> AdvisorySourceObservationScopeRequest:
    """Build the complete pre-Selection observation scope from admitted metadata only."""

    if pit_universe_key != STRATEGY_PACKAGE_PIT_UNIVERSE_KEY:
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "PIT universe key differs from the frozen Selection provider contract",
            context={"pit_universe_key": pit_universe_key},
        )

    _require_ref(
        ref=projection_ref,
        digest=str(projection.projection_hash),
        artifact_kind=O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION,
        field_name="strategy_package_input_projection",
    )
    _require_ref(
        ref=mapping_registry_ref,
        digest=str(mapping_registry.registry_hash),
        artifact_kind=O4ArtifactKind.SOURCE_MAPPING_REGISTRY,
        field_name="source_mapping_registry",
    )
    lineages = _lineage_by_component(projection=projection, window_lineages=window_lineages)
    mapping = {
        (
            item.dse_source_role,
            item.dse_dataset_id,
            item.dse_query_template_id,
            item.dse_query_template_version,
        ): item
        for item in mapping_registry.entries
    }
    expected_inputs: list[ExpectedLogicalInput] = []
    for contract_item in STRATEGY_PACKAGE_SELECTION_QUERY_CONTRACT_PAYLOAD["logical_inputs"]:
        role = str(contract_item["source_role"])
        identity = (
            role,
            str(contract_item["dataset_id"]),
            str(contract_item["query_template_id"]),
            str(contract_item["query_template_version"]),
        )
        mapping_entry = mapping.get(identity)
        if mapping_entry is None:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_MISSING,
                "compiled source mapping registry does not cover a persisted Selection input",
                context={"logical_identity": list(identity)},
            )
        component_ids = tuple(lineages) if role != "reference_price" else (None,)
        for component_id in component_ids:
            lineage = lineages[component_id] if component_id is not None else _single_effective_lineage(lineages)
            if role in _WINDOWED_ROLES:
                window_start = lineage.window_start_date
                required_window = lineage.required_window
                lineage_hash = lineage.window_lineage_hash
                resolution = lineage.window_resolution
            else:
                window_start = lineage.effective_trade_date
                required_window = 1
                resolution = "trading_day"
                lineage_hash = canonical_json_sha256(
                    {
                        "source_role": role,
                        "effective_trade_date": lineage.effective_trade_date,
                        "projection_hash": projection.projection_hash,
                    }
                )
            expected_inputs.append(
                ExpectedLogicalInput(
                    alpha_component_id=component_id,
                    source_role=identity[0],
                    dataset_id=identity[1],
                    query_template_id=identity[2],
                    query_template_version=identity[3],
                    expected_window_start_date=window_start,
                    effective_trade_date=lineage.effective_trade_date,
                    required_window=required_window,
                    window_resolution=resolution,
                    expected_window_lineage_hash=lineage_hash,
                    physical_requirement_templates=mapping_entry.physical_requirements,
                )
            )
    return AdvisorySourceObservationScopeRequest(
        target_database_identity_hash=target_database_identity_hash,
        program_id=program_id,
        decision_trade_date=decision_trade_date,
        pit_universe_key=pit_universe_key,
        package_id=projection.package_id,
        manifest_sha256=projection.manifest_sha256,
        alpha_mode=projection.alpha_mode,
        style_family=style_family,
        binding_version_id=binding_version_id,
        binding_payload_hash=binding_payload_hash,
        selection_normalized_config_hash=selection_normalized_config_hash,
        strategy_package_input_projection_ref=projection_ref,
        strategy_package_input_projection_hash=str(projection.projection_hash),
        source_mapping_registry_ref=mapping_registry_ref,
        source_mapping_registry_hash=str(mapping_registry.registry_hash),
        source_query_registry_ref=source_query_registry_ref,
        source_query_registry_hash=source_query_registry_hash,
        window_policy_ref=window_policy_ref,
        window_policy_hash=window_policy_hash,
        decision_cutoff_ts=decision_cutoff_ts,
        expected_logical_inputs=tuple(expected_inputs),
    )


def reconcile_dse_and_build_requirement_set(
    *,
    observation_scope: AdvisorySourceObservationScopeRequest,
    observation_scope_ref: AdvisoryImmutableArtifactRef,
    dse_evidence_hash: str,
    selection_artifact_hash: str,
    source_receipts: tuple[PersistedDseSourceReadReceipt, ...],
    window_lineages: tuple[ProgramWindowLineage, ...],
) -> ProgramSourceRequirementSet:
    """Require exact DSE parity, then expand every physical daily/as-of requirement."""

    _require_ref(
        ref=observation_scope_ref,
        digest=str(observation_scope.observation_scope_hash),
        artifact_kind=O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST,
        field_name="source_observation_scope",
    )
    lineages = {item.alpha_component_id: item for item in window_lineages}
    expected = {
        (item.alpha_component_id, item.source_role, item.dataset_id, item.query_template_id, item.query_template_version): item
        for item in observation_scope.expected_logical_inputs
    }
    actual = {
        (item.leg_id, item.source_role, item.dataset_id, item.query_template_id, item.query_template_version): item
        for item in source_receipts
    }
    if len(actual) != len(source_receipts):
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "persisted DSE source receipts contain duplicate logical identities",
        )
    if set(actual) != set(expected):
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "persisted DSE source receipts differ from the pre-observation logical scope",
            context={
                "missing": [list(item) for item in sorted(set(expected) - set(actual), key=str)],
                "unexpected": [list(item) for item in sorted(set(actual) - set(expected), key=str)],
            },
        )

    physical: list[SourcePartitionRequirement] = []
    for identity, expected_input in expected.items():
        receipt = actual[identity]
        lineage = lineages.get(expected_input.alpha_component_id)
        if lineage is None:
            lineage = _single_effective_lineage(lineages)
        if expected_input.source_role in _WINDOWED_ROLES:
            if (
                lineage.window_start_date != expected_input.expected_window_start_date
                or lineage.effective_trade_date != expected_input.effective_trade_date
                or lineage.required_window != expected_input.required_window
                or lineage.window_resolution != expected_input.window_resolution
                or lineage.window_lineage_hash != expected_input.expected_window_lineage_hash
            ):
                raise RealDevOnboardingError(
                    REASON_SOURCE_MAPPING_CONFLICT,
                    "persisted DSE window lineage differs from the pre-observation scope",
                    context={"logical_identity": list(identity)},
                )
        expected_partition_ref = _expected_partition_ref(expected_input)
        if receipt.partition_ref != expected_partition_ref:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "persisted DSE partition ref differs from the pre-observation scope",
                context={
                    "logical_identity": list(identity),
                    "expected_partition_ref": expected_partition_ref,
                    "actual_partition_ref": receipt.partition_ref,
                },
            )
        if expected_input.source_role == "pit_universe":
            expected_parameter_hash = canonical_json_sha256(
                {
                    "trade_date": expected_input.effective_trade_date.isoformat(),
                    "ensure": True,
                }
            )
            if receipt.parameter_hash != expected_parameter_hash:
                raise RealDevOnboardingError(
                    REASON_SOURCE_MAPPING_CONFLICT,
                    "persisted DSE PIT parameter hash differs from the frozen provider contract",
                    context={"logical_identity": list(identity)},
                )
        for template in expected_input.physical_requirement_templates:
            partition_dates = (
                lineage.trading_dates
                if template.partition_granularity is PartitionGranularity.DAILY
                and expected_input.source_role in _WINDOWED_ROLES
                else (expected_input.effective_trade_date,)
            )
            for partition_date in partition_dates:
                partition_key = {
                    template.bind_parameter_schema[-1].name: partition_date.isoformat(),
                }
                if any(parameter.name == "universe_key" for parameter in template.bind_parameter_schema):
                    partition_key["universe_key"] = observation_scope.pit_universe_key
                physical.append(
                    SourcePartitionRequirement(
                        alpha_component_id=expected_input.alpha_component_id,
                        source_role=template.source_role,
                        dataset_name=template.dataset_name,
                        query_template_id=template.observer_query_template_id,
                        query_template_version=template.observer_query_template_version,
                        partition_granularity=template.partition_granularity,
                        partition_key=partition_key,
                    )
                )
    return ProgramSourceRequirementSet(
        program_id=observation_scope.program_id,
        decision_trade_date=observation_scope.decision_trade_date,
        observation_scope_ref=observation_scope_ref,
        observation_scope_hash=str(observation_scope.observation_scope_hash),
        dse_evidence_hash=dse_evidence_hash,
        selection_artifact_hash=selection_artifact_hash,
        physical_requirements=tuple(physical),
    )


def build_program_input_unit(
    *,
    program_date: Phase1EProgramDateInput,
    compiler_dependency_ref: AdvisoryImmutableArtifactRef | None,
    compiler_dependency_hash: str | None,
    source_requirement_set_ref: AdvisoryImmutableArtifactRef | None,
    source_requirement_set_hash: str | None,
    source_resolution_receipt_ref: AdvisoryImmutableArtifactRef | None,
    source_resolution_receipt_hash: str | None,
    source_readiness: ProgramSourceReadiness,
    capacity_program_workload_ref: AdvisoryImmutableArtifactRef | None,
    capacity_program_workload_hash: str | None,
    capacity_coverage_ref: AdvisoryImmutableArtifactRef | None,
    capacity_coverage: Phase1ECapacityProgramCoverageV1 | None,
    phase1e_program_date_request_ref: AdvisoryImmutableArtifactRef | None,
    phase1e_program_date_request_hash: str | None,
    phase1e_batch_request_ref: AdvisoryImmutableArtifactRef | None = None,
    phase1e_batch_request_hash: str | None = None,
    identity_blocked: bool = False,
    reason_codes: tuple[str, ...] = (),
) -> Phase1EProgramInputUnit:
    """Derive one Program readiness state from exact evidence presence and typed outcomes."""

    identity_pairs = {
        "compiler_dependency": (compiler_dependency_ref, compiler_dependency_hash),
    }
    identity_missing = tuple(name for name, pair in identity_pairs.items() if pair[0] is None or pair[1] is None)
    if program_date.historical_status.value == "FAILED" or identity_blocked:
        identity_readiness = ProgramIdentityReadiness.BLOCKED
    elif program_date.historical_status.value == "COMPLETE" and not identity_missing:
        identity_readiness = ProgramIdentityReadiness.COMPLETE
    else:
        identity_readiness = ProgramIdentityReadiness.PENDING

    if identity_readiness is not ProgramIdentityReadiness.COMPLETE:
        if source_readiness is not ProgramSourceReadiness.NOT_EVALUATED:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "source readiness cannot be evaluated before Program identity is complete",
                context={"program_id": program_date.program_id},
            )
        effective_source_readiness = ProgramSourceReadiness.NOT_EVALUATED
    else:
        effective_source_readiness = source_readiness

    source_missing = tuple(
        name
        for name, pair in {
            "source_requirement_set": (source_requirement_set_ref, source_requirement_set_hash),
            "source_resolution_receipt": (source_resolution_receipt_ref, source_resolution_receipt_hash),
        }.items()
        if pair[0] is None or pair[1] is None
    )
    if effective_source_readiness is ProgramSourceReadiness.READY and source_missing:
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "READY source status requires the exact requirement and resolution artifacts",
            context={"program_id": program_date.program_id, "missing": list(source_missing)},
        )
    source_state_missing = (
        ("source_resolution_pending",)
        if effective_source_readiness is ProgramSourceReadiness.PENDING
        else ("source_resolution_blocked",)
        if effective_source_readiness is ProgramSourceReadiness.BLOCKED
        else ()
    )

    if capacity_coverage is None:
        if capacity_coverage_ref is not None:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "capacity coverage ref cannot exist without the typed coverage artifact",
            )
        capacity_status = ProgramCapacityStatus.NOT_MEASURED
        capacity_coverage_hash = None
    else:
        if capacity_coverage_ref is None:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "typed capacity coverage requires its immutable ref",
            )
        if capacity_coverage.program_id != program_date.program_id or capacity_coverage.decision_trade_date != program_date.decision_trade_date:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "capacity coverage identity differs from the Program/date",
            )
        capacity_status = capacity_coverage.status
        capacity_coverage_hash = str(capacity_coverage.coverage_hash)
        _require_ref(
            ref=capacity_coverage_ref,
            digest=capacity_coverage_hash,
            artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE,
            field_name="capacity_program_coverage",
        )
    if capacity_program_workload_ref is not None or capacity_program_workload_hash is not None:
        if capacity_program_workload_ref is None or capacity_program_workload_hash is None:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "capacity Program workload ref and hash must be present together",
            )
        _require_ref(
            ref=capacity_program_workload_ref,
            digest=capacity_program_workload_hash,
            artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD,
            field_name="capacity_program_workload",
        )

    capacity_missing = tuple(
        name
        for name, value in (
            (
                "capacity_program_workload",
                capacity_program_workload_ref if capacity_program_workload_hash is not None else None,
            ),
            ("capacity_coverage", capacity_coverage_ref if capacity_coverage is not None else None),
        )
        if value is None
    )
    capacity_measurement_missing = (
        tuple(f"capacity_measurement:{item}" for item in capacity_coverage.missing_measurements)
        if capacity_coverage is not None and capacity_status is not ProgramCapacityStatus.MEASURED
        else ()
    )
    request_missing = (
        ("phase1e_program_date_request",)
        if phase1e_program_date_request_ref is None or phase1e_program_date_request_hash is None
        else ()
    )
    missing_slots = tuple(
        sorted(
            set(
                (
                    *identity_missing,
                    *source_missing,
                    *source_state_missing,
                    *capacity_missing,
                    *capacity_measurement_missing,
                    *request_missing,
                )
            )
        )
    )
    blocked = (
        identity_readiness is ProgramIdentityReadiness.BLOCKED
        or effective_source_readiness is ProgramSourceReadiness.BLOCKED
        or capacity_status is ProgramCapacityStatus.INSUFFICIENT
    )
    plan_readiness = (
        ProgramPlanReadiness.BLOCKED
        if blocked
        else ProgramPlanReadiness.IDENTITY_PENDING
        if identity_readiness is not ProgramIdentityReadiness.COMPLETE
        else ProgramPlanReadiness.IDENTITY_COMPLETE_SOURCE_PENDING
        if effective_source_readiness is not ProgramSourceReadiness.READY
        else ProgramPlanReadiness.SOURCE_READY_CAPACITY_PARTIAL
        if capacity_status is not ProgramCapacityStatus.MEASURED
        else ProgramPlanReadiness.FULL_READY
    )
    combined_reasons = tuple(
        sorted(
            set(
                (
                    *program_date.historical_reason_codes,
                    *(capacity_coverage.reason_codes if capacity_coverage is not None else ()),
                    *reason_codes,
                )
            )
        )
    )
    if blocked and not combined_reasons:
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "BLOCKED Program readiness requires an explicit reason code",
        )
    return Phase1EProgramInputUnit(
        program_id=program_date.program_id,
        decision_trade_date=program_date.decision_trade_date,
        package_id=program_date.package_id,
        manifest_sha256=program_date.manifest_sha256,
        alpha_mode=program_date.alpha_mode,
        style_family=program_date.style_family,
        compiler_dependency_ref=compiler_dependency_ref,
        compiler_dependency_hash=compiler_dependency_hash,
        source_requirement_set_ref=source_requirement_set_ref,
        source_requirement_set_hash=source_requirement_set_hash,
        source_resolution_receipt_ref=source_resolution_receipt_ref,
        source_resolution_receipt_hash=source_resolution_receipt_hash,
        capacity_program_workload_ref=capacity_program_workload_ref,
        capacity_program_workload_hash=capacity_program_workload_hash,
        capacity_coverage_ref=capacity_coverage_ref,
        capacity_coverage_hash=capacity_coverage_hash,
        phase1e_program_date_request_ref=phase1e_program_date_request_ref,
        phase1e_program_date_request_hash=phase1e_program_date_request_hash,
        phase1e_batch_request_ref=phase1e_batch_request_ref,
        phase1e_batch_request_hash=phase1e_batch_request_hash,
        identity_readiness=identity_readiness,
        source_readiness=effective_source_readiness,
        capacity_status=capacity_status,
        plan_readiness=plan_readiness,
        missing_slots=missing_slots,
        reason_codes=combined_reasons,
    )


def build_real_input_bundle(
    *,
    build_request_ref: AdvisoryImmutableArtifactRef,
    build_request_hash: str,
    target_database_identity_hash: str,
    capacity_policy_ref: AdvisoryImmutableArtifactRef,
    capacity_policy_hash: str,
    source_mapping_registry_ref: AdvisoryImmutableArtifactRef,
    source_mapping_registry_hash: str,
    source_requirement_registry_ref: AdvisoryImmutableArtifactRef | None,
    source_requirement_registry_hash: str | None,
    capacity_request_ref: AdvisoryImmutableArtifactRef | None,
    capacity_request_hash: str | None,
    capacity_receipt_ref: AdvisoryImmutableArtifactRef | None,
    capacity_receipt_hash: str | None,
    program_inputs: tuple[Phase1EProgramInputUnit, ...],
) -> Phase1ERealInputBundle:
    """Build batch statistics without changing any independent Program readiness fact."""

    def counts(field_name: str) -> dict[str, int]:
        values: dict[str, int] = {}
        for item in program_inputs:
            value = getattr(item, field_name).value
            values[value] = values.get(value, 0) + 1
        return dict(sorted(values.items()))

    plan_states = {item.plan_readiness for item in program_inputs}
    aggregate = (
        AggregateInputReadiness.ALL_FULL_READY
        if plan_states == {ProgramPlanReadiness.FULL_READY}
        else AggregateInputReadiness.BLOCKED
        if plan_states == {ProgramPlanReadiness.BLOCKED}
        else AggregateInputReadiness.ALL_PENDING
        if plan_states <= {
            ProgramPlanReadiness.IDENTITY_PENDING,
            ProgramPlanReadiness.IDENTITY_COMPLETE_SOURCE_PENDING,
        }
        else AggregateInputReadiness.MIXED
    )
    return Phase1ERealInputBundle(
        build_request_ref=build_request_ref,
        build_request_hash=build_request_hash,
        target_database_identity_hash=target_database_identity_hash,
        capacity_policy_ref=capacity_policy_ref,
        capacity_policy_hash=capacity_policy_hash,
        source_mapping_registry_ref=source_mapping_registry_ref,
        source_mapping_registry_hash=source_mapping_registry_hash,
        source_requirement_registry_ref=source_requirement_registry_ref,
        source_requirement_registry_hash=source_requirement_registry_hash,
        capacity_request_ref=capacity_request_ref,
        capacity_request_hash=capacity_request_hash,
        capacity_receipt_ref=capacity_receipt_ref,
        capacity_receipt_hash=capacity_receipt_hash,
        program_inputs=program_inputs,
        counts_by_identity_readiness=counts("identity_readiness"),
        counts_by_source_readiness=counts("source_readiness"),
        counts_by_capacity_status=counts("capacity_status"),
        counts_by_plan_readiness=counts("plan_readiness"),
        aggregate_readiness=aggregate,
    )


def build_phase1e_batch_request(
    *,
    program_input: Phase1EProgramInputUnit,
    program_date_request: Phase1EProgramDateRequest,
    compiler_dependency: Phase1EProgramCompilerDependency,
    source_requirement_registry_hash: str,
    capacity_request_ref: AdvisoryImmutableArtifactRef,
    capacity_receipt_ref: AdvisoryImmutableArtifactRef,
    capacity_coverage: Phase1ECapacityProgramCoverageV1,
) -> Phase1ERevalidationBatchRequest | None:
    """Build one exact single-Program compiler request from its immutable O4 closure."""

    if program_input.plan_readiness is not ProgramPlanReadiness.FULL_READY:
        return None
    identity = (program_input.program_id, program_input.decision_trade_date)
    if identity != (program_date_request.program_id, program_date_request.decision_trade_date):
        raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "Phase 1E Program request identity differs")
    if identity != (compiler_dependency.program_id, compiler_dependency.decision_trade_date):
        raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "compiler dependency identity differs")
    expected = (
        program_date_request.expected_package_id,
        program_date_request.expected_manifest_sha256,
        program_date_request.expected_alpha_mode,
        program_date_request.expected_style_family,
    )
    actual = (
        program_input.package_id,
        program_input.manifest_sha256,
        program_input.alpha_mode.value,
        program_input.style_family,
    )
    if expected != actual:
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "Phase 1E request expected identity differs from the FULL_READY Program input",
            context={"program_id": program_input.program_id},
        )
    if program_input.capacity_program_workload_hash != capacity_coverage.program_workload_hash:
        raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "capacity coverage differs from Program workload")
    _require_ref(
        ref=capacity_request_ref,
        digest=capacity_request_ref.semantic_hash,
        artifact_kind=O4ArtifactKind.CAPACITY_REQUEST,
        field_name="capacity_request",
    )
    _require_ref(
        ref=capacity_receipt_ref,
        digest=capacity_receipt_ref.semantic_hash,
        artifact_kind=O4ArtifactKind.CAPACITY_RECEIPT,
        field_name="capacity_receipt",
    )
    return Phase1ERevalidationBatchRequest(
        program_dates=(program_date_request,),
        phase0a_policy_hash=compiler_dependency.phase0a_policy_registry_hash,
        source_requirement_registry_hash=source_requirement_registry_hash,
        query_registry_hash=compiler_dependency.source_query_registry_hash,
        calendar_hash=compiler_dependency.calendar_identity_hash,
        label_policy_bundle_hash=None,
        dataset_schema_fingerprint=compiler_dependency.dataset_schema_fingerprint,
        partition_policy_hash=compiler_dependency.partition_policy_hash,
        store_backend_config_hash=compiler_dependency.store_backend_policy_hash,
        capacity_request_ref=capacity_request_ref.semantic_hash,
        capacity_receipt_ref=capacity_receipt_ref.semantic_hash,
        capacity_program_workload_hash=program_input.capacity_program_workload_hash,
        capacity_program_coverage_hash=str(capacity_coverage.coverage_hash),
        compiler_version=compiler_dependency.compiler_version,
        serializer_version=compiler_dependency.serializer_version,
        compiler_source_hash=compiler_dependency.compiler_source_hash,
        artifact_store_policy_hash=compiler_dependency.artifact_store_policy_hash,
    )


def _lineage_by_component(
    *,
    projection: AdvisoryStrategyPackageInputProjectionV1,
    window_lineages: tuple[ProgramWindowLineage, ...],
) -> dict[str | None, ProgramWindowLineage]:
    projection_ids = tuple(item.alpha_component_id for item in projection.legs)
    expected_ids: tuple[str | None, ...] = projection_ids if projection.alpha_mode is AlphaMode.MULTI else (None,)
    actual = {item.alpha_component_id: item for item in window_lineages}
    if len(actual) != len(window_lineages) or set(actual) != set(expected_ids):
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "window lineage identities do not exactly match the admitted input projection",
            context={"expected_component_ids": list(expected_ids), "actual_component_ids": list(actual)},
        )
    projection_by_id = {item.alpha_component_id: item for item in projection.legs}
    for component_id, lineage in actual.items():
        projection_leg = projection.legs[0] if component_id is None else projection_by_id[component_id]
        if lineage.required_window != projection_leg.required_window:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "window lineage required_window differs from the admitted input projection",
                context={"alpha_component_id": component_id},
            )
    return actual


def _single_effective_lineage(
    lineages: dict[str | None, ProgramWindowLineage],
) -> ProgramWindowLineage:
    effective_dates = {item.effective_trade_date for item in lineages.values()}
    if len(effective_dates) != 1:
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            "Program leg lineages do not share one effective trade date",
        )
    return next(iter(lineages.values()))


def _expected_partition_ref(expected: ExpectedLogicalInput) -> str:
    if expected.source_role in _WINDOWED_ROLES:
        return f"{expected.expected_window_start_date.isoformat()}:{expected.effective_trade_date.isoformat()}"
    return expected.effective_trade_date.isoformat()


def _require_ref(
    *,
    ref: AdvisoryImmutableArtifactRef,
    digest: str,
    artifact_kind: O4ArtifactKind,
    field_name: str,
) -> None:
    if ref.semantic_hash != digest or ref.artifact_kind != artifact_kind.value:
        raise RealDevOnboardingError(
            REASON_SOURCE_MAPPING_CONFLICT,
            f"{field_name} immutable ref differs from the typed artifact",
        )
