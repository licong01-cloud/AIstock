"""Authoritative DEV-only O4 orchestration for Advisory historical research inputs."""

from __future__ import annotations

import hashlib
import logging
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator, Mapping

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    AdvisorySourceRequirementRegistry,
    AdvisorySourceResolutionArtifact,
    ArtifactStorePolicyArtifact,
    CalendarIdentityArtifact,
    HistoricalProgramStatus,
    O4ArtifactKind,
    O4_ARTIFACT_STORE_POLICY_HASH,
    O4_ARTIFACT_STORE_POLICY_PAYLOAD,
    ObserverConfigArtifact,
    PartitionPolicyArtifact,
    Phase0APolicyRegistryArtifact,
    Phase1ECompileAggregateStatus,
    Phase1ECompileProgramResult,
    Phase1ECompileProgramStatus,
    Phase1ECompileReceipt,
    Phase1EProgramCompilerDependency,
    Phase1EProgramDateInput,
    Phase1ERealInputBuildRequest,
    ProgramPlanReadiness,
    ProgramSourceReadiness,
    REASON_SOURCE_MAPPING_CONFLICT,
    RealDevHistoricalRunReceipt,
    RealDevHistoricalRunRequest,
    RealDevOnboardingError,
    SourceQueryRegistryArtifact,
    StoreBackendPolicyArtifact,
    database_identity_hash,
)
from backend.services.advisory_dev_input_onboarding.historical_onboarding import (
    ExactDevConnectionFactory,
    HistoricalOnboardingEvidenceStore,
    _database_identity,
    prospective_target_entry_cutoff,
)
from backend.services.advisory_dev_input_onboarding.phase1e_input_builder import (
    PersistedDseSourceReadReceipt,
    ProgramWindowLineage,
    build_phase1e_batch_request,
    build_pre_observation_scope,
    build_program_input_unit,
    build_real_input_bundle,
    reconcile_dse_and_build_requirement_set,
)
from backend.services.advisory_dev_input_onboarding.phase1e_inputs import Phase1EInputArtifactStore
from backend.services.advisory_dev_input_onboarding.phase1e_source_mapping import compiled_o4_source_mapping_registry
from backend.services.advisory_dev_input_onboarding.production_projection import readonly_onboarding_connection
from backend.services.advisory_phase0a.evidence_projection_postgres import AdvisoryPostgresEvidenceProjection
from backend.services.advisory_phase0a.models import Phase0APolicyRegistry
from backend.services.advisory_phase0a.policy import (
    POLICY_REGISTRY_ROOT,
    canonical_json_sha256,
    load_frozen_policy_registry,
)
from backend.services.advisory_phase1.readiness_plan import (
    EvidenceOrigin,
    Phase1EAuditOutcome,
    Phase1EError,
    Phase1EProgramDateEvidence,
    Phase1EProgramDateRequest,
    Phase1EReadinessPlanCompiler,
)
from backend.services.advisory_phase1.readiness_plan_postgres import PostgresPhase1EInputProvider
from backend.services.advisory_phase1.readiness_plan_store import ContentAddressedPlanStore
from backend.services.advisory_phase1.release_schema_contract import TargetLabel
from backend.services.advisory_phase1.release_schema_verify_postgres import (
    DatabaseConnectionConfig,
    resolve_database_connection,
)
from backend.services.advisory_phase1.source_capacity import (
    AdvisoryPhase1CapacityProbe,
    Phase1ECapacityPlanningReceiptV2,
    Phase1ECapacityPlanningRequestV2,
    Phase1ECapacityPolicyV1,
    Phase1ECapacityProgramCoverageV1,
    Phase1EProgramCapacityWorkload,
    build_capacity_program_coverage_v1,
    build_capacity_receipt_v2,
    build_capacity_request_v2,
)
from backend.services.advisory_phase1.source_ledger_postgres import PostgresSourceAvailabilityLedger
from backend.services.advisory_phase1.source_observer import (
    SOURCE_QUERY_TEMPLATES,
    SourceObserverConfigBundle,
    registered_source_observer_configs,
)
from backend.services.advisory_phase1.source_observer_postgres import PostgresSourceObserverRepository
from backend.services.advisory_phase1.source_resolution import (
    FixtureSourceRevisionResolver,
    ResearchReadiness,
    SourceRequirement,
    SourceRequirementSet,
    build_source_requirement_common_pit_identity_hash,
)
from backend.services.advisory_phase1.source_revision import AvailabilityRequirement, SourceRevisionKind
from backend.services.advisory_program import AdvisoryProgramPGRepository, _binding_payload
from backend.services.strategy_package.advisory_input_projection import project_advisory_inputs
from backend.services.strategy_package.repository import StrategyPackageRepository


LOGGER = logging.getLogger(__name__)
REASON_O4_INPUT_PENDING = "ADVISORY_DEV_ONBOARDING_SOURCE_EVIDENCE_PENDING"
REASON_O4_COMPILE_EMPTY = "ADVISORY_PHASE1E_ZERO_PLAN_NOT_SUCCESS"
PARTITION_POLICY_PAYLOAD = {
    "schema_version": "advisory_o4_partition_policy_v1",
    "daily": "one_trading_date_per_partition",
    "as_of_snapshot": "one_effective_trade_date_per_snapshot",
    "inference_window": {
        "buffer_trading_days": 5,
        "calendar_source": "market.trading_calendar",
        "calendar_version": "market.trading_calendar.v1",
        "window_resolution": "trading_calendar",
    },
}


class FrozenO4SourceRequirementCompiler:
    """Return one already resolved, hash-closed Phase1 requirement set for one Program batch."""

    def __init__(self, *, registry_hash: str, requirement_set: SourceRequirementSet) -> None:
        self._registry_hash = registry_hash
        self._requirement_set = requirement_set

    @property
    def registry_hash(self) -> str:
        return self._registry_hash

    def compile(self, **kwargs: Any) -> SourceRequirementSet:
        binding = kwargs["binding"]
        scope = kwargs["scope"]
        request = kwargs["request"]
        requirement_set = self._requirement_set
        if (
            requirement_set.program_id != request.program_id
            or requirement_set.package_id != binding.package_id
            or requirement_set.admission_scope_id != scope.admission_scope_id
        ):
            raise Phase1EError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "frozen O4 source requirement set identity differs from the compiler scope",
            )
        return requirement_set


class AdvisoryPhase1EOrchestrationService:
    def __init__(self, *, repository_root: Path, now_provider: Any | None = None) -> None:
        self.repository_root = repository_root.resolve()
        self.now_provider = now_provider or (lambda: datetime.now(UTC))

    def observe_source(
        self,
        *,
        historical_request_ref: AdvisoryImmutableArtifactRef,
        env_file: Path,
        evidence_root: Path,
        artifact_root: Path,
        config_id: str = "phase1e_advisory_inputs_dev_v2",
        config_version: str = "v2",
    ) -> dict[str, Any]:
        historical_store = HistoricalOnboardingEvidenceStore(root=evidence_root)
        request = historical_store.load(historical_request_ref)
        if not isinstance(request, RealDevHistoricalRunRequest):
            raise ValueError("historical_request_ref does not resolve to a historical request")
        database, write_factory = self._dev_write_factory(env_file=env_file, expected_identity_hash=request.target_database_identity_hash)
        store = self._input_store(artifact_root=artifact_root, env_file=env_file, code_commit=request.code_release_id)
        config = self._observer_config(config_id=config_id, config_version=config_version)
        common = self._publish_common_artifacts(
            store=store,
            config=config,
            store_backend_root=self._dataset_store_root(env_file=env_file, explicit_root=None),
            policy=None,
        )
        mapping = compiled_o4_source_mapping_registry()
        mapping_ref = store.publish(
            artifact_kind=O4ArtifactKind.SOURCE_MAPPING_REGISTRY,
            model=mapping,
            semantic_hash=str(mapping.registry_hash),
        )
        package_repository = StrategyPackageRepository(conn_factory=write_factory)
        program_repository = AdvisoryProgramPGRepository(conn_factory=write_factory)
        program_results: list[dict[str, Any]] = []
        for spec in request.program_specs:
            try:
                binding = self._dated_binding(
                    repository=program_repository,
                    program_id=spec.program_id,
                    trade_date=request.decision_trade_date,
                    package_id=spec.package_id,
                )
                package = package_repository.get(spec.package_id)
                if package.manifest_sha256 != spec.manifest_sha256:
                    raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "dated package manifest differs from request")
                projection = project_advisory_inputs(package.manifest)
                projection_ref = store.publish(
                    artifact_kind=O4ArtifactKind.STRATEGY_PACKAGE_INPUT_PROJECTION,
                    model=projection,
                    semantic_hash=str(projection.projection_hash),
                )
                with AdvisoryPostgresEvidenceProjection(write_factory).snapshot() as snapshot:
                    lineages = self._pre_observation_lineages(
                        projection=projection,
                        decision_trade_date=request.decision_trade_date,
                        calendar_reader=snapshot,
                    )
                    decision_cutoff_ts = self._prospective_decision_cutoff(
                        calendar_reader=snapshot,
                        decision_trade_date=request.decision_trade_date,
                    )
                calendar_artifact = CalendarIdentityArtifact(
                    payload=self._calendar_payload(lineages=lineages, decision_trade_date=request.decision_trade_date)
                )
                calendar_ref = store.publish(
                    artifact_kind=O4ArtifactKind.CALENDAR_IDENTITY,
                    model=calendar_artifact,
                    semantic_hash=str(calendar_artifact.content_hash),
                )
                scope = build_pre_observation_scope(
                    target_database_identity_hash=request.target_database_identity_hash,
                    program_id=spec.program_id,
                    decision_trade_date=request.decision_trade_date,
                    pit_universe_key="shsz_st_pit_active_v1",
                    style_family=spec.style,
                    binding_version_id=binding.binding_version_id,
                    binding_payload_hash=canonical_json_sha256(_binding_payload(binding)),
                    selection_normalized_config_hash=canonical_json_sha256(binding.runtime_config_json),
                    projection=projection,
                    projection_ref=projection_ref,
                    mapping_registry=mapping,
                    mapping_registry_ref=mapping_ref,
                    source_query_registry_ref=common["source_query_registry_ref"],
                    source_query_registry_hash=common["source_query_registry_ref"].semantic_hash,
                    window_policy_ref=common["partition_policy_ref"],
                    window_policy_hash=common["partition_policy_ref"].semantic_hash,
                    decision_cutoff_ts=decision_cutoff_ts,
                    window_lineages=lineages,
                )
                scope_ref = store.publish(
                    artifact_kind=O4ArtifactKind.SOURCE_OBSERVATION_SCOPE_REQUEST,
                    model=scope,
                    semantic_hash=str(scope.observation_scope_hash),
                )
                program_results.append(
                    {
                        "program_id": spec.program_id,
                        "status": "COMPLETE",
                        "observation_scope_ref": scope_ref,
                        "calendar_identity_ref": calendar_ref,
                    }
                )
            except Exception as exc:
                LOGGER.exception("advisory_o4_observation_scope_failed program_id=%s", spec.program_id)
                reason_code = getattr(exc, "reason_code", None) or REASON_SOURCE_MAPPING_CONFLICT
                program_results.append(
                    {
                        "program_id": spec.program_id,
                        "status": "PENDING" if reason_code == REASON_O4_INPUT_PENDING else "FAILED",
                        "reason_codes": (reason_code,),
                        "message": str(exc),
                    }
                )
        summary = PostgresSourceObserverRepository(
            conn_factory=write_factory,
            ledger=PostgresSourceAvailabilityLedger(conn_factory=write_factory),
        ).observe_once(config=config, registry=SOURCE_QUERY_TEMPLATES)
        program_statuses = {item["status"] for item in program_results}
        aggregate_status = (
            "FAILED"
            if "FAILED" in program_statuses or not summary.succeeded
            else "PENDING"
            if "PENDING" in program_statuses
            else "COMPLETE"
        )
        return {
            "command": "observe-source",
            "target_database_identity_hash": request.target_database_identity_hash,
            "source_mapping_registry_ref": mapping_ref,
            "observer_summary": summary.as_dict(),
            "program_results": program_results,
            "aggregate_status": aggregate_status,
            "ok": aggregate_status == "COMPLETE",
        }

    def build_phase1e_inputs(
        self,
        *,
        historical_request_ref: AdvisoryImmutableArtifactRef,
        historical_receipt_ref: AdvisoryImmutableArtifactRef,
        observation_scope_refs: tuple[AdvisoryImmutableArtifactRef, ...],
        source_mapping_registry_ref: AdvisoryImmutableArtifactRef,
        capacity_policy_ref: AdvisoryImmutableArtifactRef,
        env_file: Path,
        evidence_root: Path,
        artifact_root: Path,
        policy_registry_root: Path = POLICY_REGISTRY_ROOT,
        config_id: str = "phase1e_advisory_inputs_dev_v2",
        config_version: str = "v2",
    ) -> dict[str, Any]:
        historical_store = HistoricalOnboardingEvidenceStore(root=evidence_root)
        request = historical_store.load(historical_request_ref)
        receipt = historical_store.load(historical_receipt_ref)
        if not isinstance(request, RealDevHistoricalRunRequest) or not isinstance(receipt, RealDevHistoricalRunReceipt):
            raise ValueError("historical refs resolve to the wrong contract types")
        if receipt.historical_request_hash != request.historical_request_hash:
            raise ValueError("historical receipt does not bind the supplied request")
        database, readonly_factory = self._dev_read_factory(env_file=env_file, expected_identity_hash=request.target_database_identity_hash)
        store = self._input_store(artifact_root=artifact_root, env_file=env_file, code_commit=request.code_release_id)
        config = self._observer_config(config_id=config_id, config_version=config_version)
        policy = load_frozen_policy_registry(
            policy_registry_id=request.policy_registry_id,
            policy_version=request.policy_registry_version,
            registry_root=policy_registry_root,
        )
        if policy.registry_content_hash != request.policy_registry_hash:
            raise ValueError("frozen Phase0A policy registry differs from the historical request")
        common = self._publish_common_artifacts(
            store=store,
            config=config,
            store_backend_root=self._dataset_store_root(env_file=env_file, explicit_root=None),
            policy=policy,
        )
        mapping = store.load(ref=source_mapping_registry_ref, model_type=type(compiled_o4_source_mapping_registry()))
        capacity_policy = store.load(ref=capacity_policy_ref, model_type=Phase1ECapacityPolicyV1)
        observation_by_identity = {}
        from backend.services.advisory_dev_input_onboarding.contracts import AdvisorySourceObservationScopeRequest
        specs_by_program = {item.program_id: item for item in request.program_specs}
        for ref in observation_scope_refs:
            scope = store.load(ref=ref, model_type=AdvisorySourceObservationScopeRequest)
            identity = (scope.program_id, scope.decision_trade_date)
            if identity in observation_by_identity:
                raise RealDevOnboardingError(
                    REASON_SOURCE_MAPPING_CONFLICT,
                    "multiple pre-observation scopes bind the same Program/date",
                )
            spec = specs_by_program.get(scope.program_id)
            if (
                spec is None
                or scope.decision_trade_date != request.decision_trade_date
                or scope.target_database_identity_hash != request.target_database_identity_hash
                or scope.package_id != spec.package_id
                or scope.manifest_sha256 != spec.manifest_sha256
                or scope.alpha_mode is not spec.alpha_mode
                or scope.style_family != spec.style
                or scope.source_mapping_registry_hash != str(mapping.registry_hash)
                or scope.source_query_registry_hash != common["source_query_registry_ref"].semantic_hash
                or scope.window_policy_hash != common["partition_policy_ref"].semantic_hash
            ):
                raise RealDevOnboardingError(
                    REASON_SOURCE_MAPPING_CONFLICT,
                    "pre-observation scope identity differs from the O3 request or compiled O4 policies",
                )
            observation_by_identity[identity] = (scope, ref)
        provider = PostgresPhase1EInputProvider(
            projection=AdvisoryPostgresEvidenceProjection(readonly_factory),
            policy=policy,
        )
        receipt_result_by_program = {item.program_id: item for item in receipt.program_results}
        program_dates: list[Phase1EProgramDateInput] = []
        prepared: dict[tuple[str, date], dict[str, Any]] = {}
        for spec in request.program_specs:
            historical_result = receipt_result_by_program[spec.program_id]
            if historical_result.status is not HistoricalProgramStatus.COMPLETE:
                program_dates.append(
                    Phase1EProgramDateInput(
                        program_id=spec.program_id,
                        decision_trade_date=request.decision_trade_date,
                        package_id=spec.package_id,
                        manifest_sha256=spec.manifest_sha256,
                        alpha_mode=spec.alpha_mode,
                        style_family=spec.style,
                        historical_status=historical_result.status,
                        historical_reason_codes=historical_result.reason_codes,
                    )
                )
                continue
            observation_pair = observation_by_identity.get((spec.program_id, request.decision_trade_date))
            observation = observation_pair[0] if observation_pair is not None else None
            observation_ref = observation_pair[1] if observation_pair is not None else None
            program_request = self._program_date_request(
                request=request,
                receipt=receipt,
                historical_result=historical_result,
                observation=observation,
            )
            try:
                evidence = provider.resolve_program_date(request=program_request, batch_request=None)  # type: ignore[arg-type]
                program_request = program_request.model_copy(
                    update={"label_as_of_ts": self._dse_decision_cutoff(evidence=evidence)}
                )
                _, audit, target, scopes = Phase1EReadinessPlanCompiler.prepare_program_audit(
                    program_date=program_request,
                    evidence=evidence,
                )
                scope = self._single_scope(scopes)
                calendar_artifact = self._calendar_artifact_from_evidence(evidence=evidence)
                calendar_ref = store.publish(
                    artifact_kind=O4ArtifactKind.CALENDAR_IDENTITY,
                    model=calendar_artifact,
                    semantic_hash=str(calendar_artifact.content_hash),
                )
                dependency = self._compiler_dependency(
                    request=request,
                    receipt=receipt,
                    historical_result=historical_result,
                    spec=spec,
                    audit=audit,
                    scope=scope,
                    common=common,
                    calendar_ref=calendar_ref,
                    historical_receipt_ref=historical_receipt_ref,
                )
                dependency_ref = store.publish(
                    artifact_kind=O4ArtifactKind.PROGRAM_COMPILER_DEPENDENCY,
                    model=dependency,
                    semantic_hash=str(dependency.dependency_hash),
                )
                program_date = Phase1EProgramDateInput(
                    program_id=spec.program_id,
                    decision_trade_date=request.decision_trade_date,
                    package_id=spec.package_id,
                    manifest_sha256=spec.manifest_sha256,
                    alpha_mode=spec.alpha_mode,
                    style_family=spec.style,
                    historical_status=HistoricalProgramStatus.COMPLETE,
                    historical_program_run_id=str(historical_result.historical_program_run_id),
                    historical_batch_receipt_ref=historical_receipt_ref,
                    historical_batch_receipt_hash=str(receipt.receipt_hash),
                    compiler_dependency_ref=dependency_ref,
                    compiler_dependency_hash=str(dependency.dependency_hash),
                )
                program_dates.append(program_date)
                prepared[(spec.program_id, request.decision_trade_date)] = {
                    "program_date": program_date,
                    "program_request": program_request,
                    "audit": audit,
                    "target": target,
                    "scope": scope,
                    "observation": observation,
                    "observation_ref": observation_ref,
                    "dependency": dependency,
                    "dependency_ref": dependency_ref,
                }
            except Exception as exc:
                LOGGER.exception("advisory_o4_program_identity_failed program_id=%s", spec.program_id)
                program_date = Phase1EProgramDateInput(
                    program_id=spec.program_id,
                    decision_trade_date=request.decision_trade_date,
                    package_id=spec.package_id,
                    manifest_sha256=spec.manifest_sha256,
                    alpha_mode=spec.alpha_mode,
                    style_family=spec.style,
                    historical_status=HistoricalProgramStatus.COMPLETE,
                    historical_program_run_id=str(historical_result.historical_program_run_id),
                    historical_batch_receipt_ref=historical_receipt_ref,
                    historical_batch_receipt_hash=str(receipt.receipt_hash),
                )
                program_dates.append(program_date)
                prepared[(spec.program_id, request.decision_trade_date)] = {
                    "program_date": program_date,
                    "preparation_error": exc,
                }
            finally:
                provider.close_program_date()
        build_request = Phase1ERealInputBuildRequest(
            historical_run_request_ref=historical_request_ref,
            historical_run_request_hash=str(request.historical_request_hash),
            historical_run_receipt_ref=historical_receipt_ref,
            historical_run_receipt_hash=str(receipt.receipt_hash),
            target_database_identity_hash=request.target_database_identity_hash,
            target_package_asset_root_hash=request.target_package_asset_root_hash,
            program_dates=tuple(program_dates),
            source_mapping_registry_ref=source_mapping_registry_ref,
            source_mapping_registry_hash=str(mapping.registry_hash),
            capacity_policy_ref=capacity_policy_ref,
            capacity_policy_hash=str(capacity_policy.policy_hash),
            code_release_id=request.code_release_id,
            code_release_hash=request.code_release_hash,
        )
        build_request_ref = store.publish(
            artifact_kind=O4ArtifactKind.REAL_INPUT_BUILD_REQUEST,
            model=build_request,
            semantic_hash=str(build_request.build_request_hash),
        )
        requirement_sets = []
        program_inputs = []
        for program_date in build_request.program_dates:
            context = prepared.get((program_date.program_id, program_date.decision_trade_date))
            if context is None:
                program_inputs.append(
                    build_program_input_unit(
                        program_date=program_date,
                        compiler_dependency_ref=program_date.compiler_dependency_ref,
                        compiler_dependency_hash=program_date.compiler_dependency_hash,
                        source_requirement_set_ref=None,
                        source_requirement_set_hash=None,
                        source_resolution_receipt_ref=None,
                        source_resolution_receipt_hash=None,
                        source_readiness=ProgramSourceReadiness.NOT_EVALUATED,
                        capacity_program_workload_ref=None,
                        capacity_program_workload_hash=None,
                        capacity_coverage_ref=None,
                        capacity_coverage=None,
                        phase1e_program_date_request_ref=None,
                        phase1e_program_date_request_hash=None,
                    )
                )
                continue
            if context.get("preparation_error") is not None:
                exc = context["preparation_error"]
                program_inputs.append(
                    build_program_input_unit(
                        program_date=program_date,
                        compiler_dependency_ref=None,
                        compiler_dependency_hash=None,
                        source_requirement_set_ref=None,
                        source_requirement_set_hash=None,
                        source_resolution_receipt_ref=None,
                        source_resolution_receipt_hash=None,
                        source_readiness=ProgramSourceReadiness.NOT_EVALUATED,
                        capacity_program_workload_ref=None,
                        capacity_program_workload_hash=None,
                        capacity_coverage_ref=None,
                        capacity_coverage=None,
                        phase1e_program_date_request_ref=None,
                        phase1e_program_date_request_hash=None,
                        identity_blocked=True,
                        reason_codes=(getattr(exc, "reason_code", None) or "ADVISORY_PHASE1E_PROGRAM_IDENTITY_FAILED",),
                    )
                )
                continue
            try:
                if context["observation"] is None or context["observation_ref"] is None:
                    program_request = context["program_request"]
                    program_request_ref = store.publish(
                        artifact_kind=O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST,
                        model=program_request,
                        semantic_hash=str(program_request.program_date_request_hash),
                    )
                    program_inputs.append(
                        build_program_input_unit(
                            program_date=program_date,
                            compiler_dependency_ref=context["dependency_ref"],
                            compiler_dependency_hash=str(context["dependency"].dependency_hash),
                            source_requirement_set_ref=None,
                            source_requirement_set_hash=None,
                            source_resolution_receipt_ref=None,
                            source_resolution_receipt_hash=None,
                            source_readiness=ProgramSourceReadiness.PENDING,
                            capacity_program_workload_ref=None,
                            capacity_program_workload_hash=None,
                            capacity_coverage_ref=None,
                            capacity_coverage=None,
                            phase1e_program_date_request_ref=program_request_ref,
                            phase1e_program_date_request_hash=str(program_request.program_date_request_hash),
                            reason_codes=(REASON_O4_INPUT_PENDING,),
                        )
                    )
                    continue
                evidence = provider.resolve_program_date(
                    request=context["program_request"],
                    batch_request=None,  # type: ignore[arg-type]
                )
                context["program_request_evidence"] = evidence
                physical_set, lineages = self._physical_requirement_set(context=context)
                physical_ref = store.publish(
                    artifact_kind=O4ArtifactKind.SOURCE_REQUIREMENT_SET,
                    model=physical_set,
                    semantic_hash=str(physical_set.requirement_set_hash),
                )
                phase1_set = self._phase1_requirement_set(
                    physical_set=physical_set,
                    mapping=mapping,
                    context=context,
                )
                events = {}
                for requirement in phase1_set.requirements:
                    for event in evidence.audit_readers.source_probe.list_source_events(
                        dataset_name=requirement.dataset_name,
                        source_role=requirement.source_role,
                        partition_key=requirement.partition_key,
                    ):
                        events[event.event_content_hash] = event
                source_result = FixtureSourceRevisionResolver().resolve(
                    requirement_set=phase1_set,
                    availability_events=tuple(events[key] for key in sorted(events)),
                )
                resolution_artifact = AdvisorySourceResolutionArtifact(
                    program_id=program_date.program_id,
                    decision_trade_date=program_date.decision_trade_date,
                    physical_requirement_set_ref=physical_ref,
                    physical_requirement_set_hash=str(physical_set.requirement_set_hash),
                    phase1_requirement_set=phase1_set,
                    resolution_receipt=source_result.receipt,
                )
                resolution_ref = store.publish(
                    artifact_kind=O4ArtifactKind.SOURCE_RESOLUTION_RECEIPT,
                    model=resolution_artifact,
                    semantic_hash=str(resolution_artifact.artifact_hash),
                )
                requirement_sets.append(physical_set)
                workload = self._source_only_workload(
                    context=context,
                    requirement_set_hash=str(physical_set.requirement_set_hash),
                )
                workload_ref = store.publish(
                    artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_WORKLOAD,
                    model=workload,
                    semantic_hash=str(workload.program_workload_hash),
                )
                program_request = context["program_request"]
                program_request_ref = store.publish(
                    artifact_kind=O4ArtifactKind.PHASE1E_PROGRAM_DATE_REQUEST,
                    model=program_request,
                    semantic_hash=str(program_request.program_date_request_hash),
                )
                source_readiness = (
                    ProgramSourceReadiness.READY
                    if source_result.receipt.readiness is ResearchReadiness.RESEARCH_READY
                    else ProgramSourceReadiness.BLOCKED
                    if source_result.receipt.readiness is ResearchReadiness.BLOCKED
                    else ProgramSourceReadiness.PENDING
                )
                program_inputs.append(
                    build_program_input_unit(
                        program_date=program_date,
                        compiler_dependency_ref=context["dependency_ref"],
                        compiler_dependency_hash=str(context["dependency"].dependency_hash),
                        source_requirement_set_ref=physical_ref,
                        source_requirement_set_hash=str(physical_set.requirement_set_hash),
                        source_resolution_receipt_ref=resolution_ref,
                        source_resolution_receipt_hash=str(resolution_artifact.artifact_hash),
                        source_readiness=source_readiness,
                        capacity_program_workload_ref=workload_ref,
                        capacity_program_workload_hash=str(workload.program_workload_hash),
                        capacity_coverage_ref=None,
                        capacity_coverage=None,
                        phase1e_program_date_request_ref=program_request_ref,
                        phase1e_program_date_request_hash=str(program_request.program_date_request_hash),
                        reason_codes=tuple(source_result.receipt.reason_codes),
                    )
                )
            except Exception as exc:
                LOGGER.exception("advisory_o4_program_input_failed program_id=%s", program_date.program_id)
                program_inputs.append(
                    build_program_input_unit(
                        program_date=program_date,
                        compiler_dependency_ref=context["dependency_ref"],
                        compiler_dependency_hash=str(context["dependency"].dependency_hash),
                        source_requirement_set_ref=None,
                        source_requirement_set_hash=None,
                        source_resolution_receipt_ref=None,
                        source_resolution_receipt_hash=None,
                        source_readiness=ProgramSourceReadiness.BLOCKED,
                        capacity_program_workload_ref=None,
                        capacity_program_workload_hash=None,
                        capacity_coverage_ref=None,
                        capacity_coverage=None,
                        phase1e_program_date_request_ref=None,
                        phase1e_program_date_request_hash=None,
                        reason_codes=(getattr(exc, "reason_code", None) or REASON_SOURCE_MAPPING_CONFLICT,),
                    )
                )
            finally:
                provider.close_program_date()
        registry = AdvisorySourceRequirementRegistry(
            build_request_hash=str(build_request.build_request_hash),
            source_mapping_registry_hash=str(mapping.registry_hash),
            source_query_registry_hash=common["source_query_registry_ref"].semantic_hash,
            program_requirement_sets=tuple(requirement_sets),
        ) if requirement_sets else None
        registry_ref = (
            store.publish(
                artifact_kind=O4ArtifactKind.SOURCE_REQUIREMENT_REGISTRY,
                model=registry,
                semantic_hash=str(registry.registry_hash),
            )
            if registry is not None else None
        )
        bundle = build_real_input_bundle(
            build_request_ref=build_request_ref,
            build_request_hash=str(build_request.build_request_hash),
            target_database_identity_hash=request.target_database_identity_hash,
            capacity_policy_ref=capacity_policy_ref,
            capacity_policy_hash=str(capacity_policy.policy_hash),
            source_mapping_registry_ref=source_mapping_registry_ref,
            source_mapping_registry_hash=str(mapping.registry_hash),
            source_requirement_registry_ref=registry_ref,
            source_requirement_registry_hash=str(registry.registry_hash) if registry is not None else None,
            capacity_request_ref=None,
            capacity_request_hash=None,
            capacity_receipt_ref=None,
            capacity_receipt_hash=None,
            program_inputs=tuple(program_inputs),
        )
        bundle_ref = store.publish(
            artifact_kind=O4ArtifactKind.INPUT_BUNDLE,
            model=bundle,
            semantic_hash=str(bundle.input_bundle_hash),
        )
        return {"command": "build-phase1e-inputs", "input_bundle_ref": bundle_ref, "bundle": bundle, "ok": bundle.aggregate_readiness.value != "BLOCKED"}

    def plan_capacity(
        self,
        *,
        input_bundle_ref: AdvisoryImmutableArtifactRef,
        env_file: Path,
        artifact_root: Path,
        advisory_store_root: Path | None = None,
        config_id: str = "phase1e_advisory_inputs_dev_v2",
        config_version: str = "v2",
    ) -> dict[str, Any]:
        store = self._input_store(artifact_root=artifact_root, env_file=env_file, code_commit=None)
        from backend.services.advisory_dev_input_onboarding.contracts import Phase1ERealInputBundle
        bundle = store.load(ref=input_bundle_ref, model_type=Phase1ERealInputBundle)
        build_request = store.load(ref=bundle.build_request_ref, model_type=Phase1ERealInputBuildRequest)
        _, readonly_factory = self._dev_read_factory(env_file=env_file, expected_identity_hash=bundle.target_database_identity_hash)
        config = self._observer_config(config_id=config_id, config_version=config_version)
        workloads = tuple(
            store.load(ref=item.capacity_program_workload_ref, model_type=Phase1EProgramCapacityWorkload)
            for item in bundle.program_inputs if item.capacity_program_workload_ref is not None
        )
        if not workloads:
            raise RealDevOnboardingError(REASON_O4_INPUT_PENDING, "pre-capacity bundle has no exact Program workloads")
        dependencies = [
            store.load(ref=item.compiler_dependency_ref, model_type=Phase1EProgramCompilerDependency)
            for item in bundle.program_inputs if item.compiler_dependency_ref is not None
        ]
        observer_refs = {item.observer_config_ref for item in dependencies}
        query_refs = {item.source_query_registry_ref for item in dependencies}
        store_refs = {item.store_backend_policy_ref for item in dependencies}
        if len(observer_refs) != 1 or len(query_refs) != 1 or len(store_refs) != 1:
            raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "capacity Programs do not share one exact DEV observer/query/store identity")
        capacity_policy = store.load(ref=bundle.capacity_policy_ref, model_type=Phase1ECapacityPolicyV1)
        dataset_root = self._dataset_store_root(env_file=env_file, explicit_root=advisory_store_root)
        registry = store.load(ref=bundle.source_requirement_registry_ref, model_type=AdvisorySourceRequirementRegistry)
        history_dates = [
            date.fromisoformat(str(value))
            for requirement_set in registry.program_requirement_sets
            for requirement in requirement_set.physical_requirements
            for value in requirement.partition_key.values()
            if self._is_iso_date(value)
        ]
        if not history_dates:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "capacity requirement registry has no exact business dates",
            )
        store_policy_artifact = store.load(
            ref=next(iter(store_refs)),
            model_type=StoreBackendPolicyArtifact,
        )
        if store_policy_artifact.payload.get("root_identity_hash") != canonical_json_sha256(str(dataset_root)):
            raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "capacity dataset root differs from the frozen store policy")
        with AdvisoryPostgresEvidenceProjection(readonly_factory).snapshot() as snapshot:
            capacity_as_of_ts = snapshot.postgres_now()
        request = build_capacity_request_v2(
            observer_config_ref=next(iter(observer_refs)),
            query_registry_ref=next(iter(query_refs)),
            capacity_policy_ref=bundle.capacity_policy_ref,
            capacity_policy=capacity_policy,
            as_of_ts=capacity_as_of_ts,
            history_start_trade_date=min(history_dates),
            history_end_trade_date=max(history_dates),
            program_workloads=workloads,
            store_root_ref=next(iter(store_refs)),
        )
        request_ref = store.publish(artifact_kind=O4ArtifactKind.CAPACITY_REQUEST, model=request, semantic_hash=str(request.request_hash))
        measurements = AdvisoryPhase1CapacityProbe(conn_factory=readonly_factory).probe_v2(
            request=request,
            config=config,
            target_database_identity_hash=bundle.target_database_identity_hash,
            advisory_store_root=dataset_root,
            registry=SOURCE_QUERY_TEMPLATES,
        )
        receipt = build_capacity_receipt_v2(request=request, request_ref=request_ref, measurements=measurements)
        receipt_ref = store.publish(artifact_kind=O4ArtifactKind.CAPACITY_RECEIPT, model=receipt, semantic_hash=str(receipt.receipt_hash))
        program_dates = {(item.program_id, item.decision_trade_date): item for item in build_request.program_dates}
        updated_inputs = []
        for item in bundle.program_inputs:
            if item.capacity_program_workload_ref is None:
                updated_inputs.append(item)
                continue
            workload = store.load(ref=item.capacity_program_workload_ref, model_type=Phase1EProgramCapacityWorkload)
            coverage = build_capacity_program_coverage_v1(
                request=request,
                request_ref=request_ref,
                receipt=receipt,
                receipt_ref=receipt_ref,
                workload=workload,
                workload_ref=item.capacity_program_workload_ref,
            )
            coverage_ref = store.publish(
                artifact_kind=O4ArtifactKind.CAPACITY_PROGRAM_COVERAGE,
                model=coverage,
                semantic_hash=str(coverage.coverage_hash),
            )
            updated_inputs.append(
                build_program_input_unit(
                    program_date=program_dates[(item.program_id, item.decision_trade_date)],
                    compiler_dependency_ref=item.compiler_dependency_ref,
                    compiler_dependency_hash=item.compiler_dependency_hash,
                    source_requirement_set_ref=item.source_requirement_set_ref,
                    source_requirement_set_hash=item.source_requirement_set_hash,
                    source_resolution_receipt_ref=item.source_resolution_receipt_ref,
                    source_resolution_receipt_hash=item.source_resolution_receipt_hash,
                    source_readiness=item.source_readiness,
                    capacity_program_workload_ref=item.capacity_program_workload_ref,
                    capacity_program_workload_hash=item.capacity_program_workload_hash,
                    capacity_coverage_ref=coverage_ref,
                    capacity_coverage=coverage,
                    phase1e_program_date_request_ref=item.phase1e_program_date_request_ref,
                    phase1e_program_date_request_hash=item.phase1e_program_date_request_hash,
                    reason_codes=item.reason_codes,
                )
            )
        post_bundle = build_real_input_bundle(
            build_request_ref=bundle.build_request_ref,
            build_request_hash=bundle.build_request_hash,
            target_database_identity_hash=bundle.target_database_identity_hash,
            capacity_policy_ref=bundle.capacity_policy_ref,
            capacity_policy_hash=bundle.capacity_policy_hash,
            source_mapping_registry_ref=bundle.source_mapping_registry_ref,
            source_mapping_registry_hash=bundle.source_mapping_registry_hash,
            source_requirement_registry_ref=bundle.source_requirement_registry_ref,
            source_requirement_registry_hash=bundle.source_requirement_registry_hash,
            capacity_request_ref=request_ref,
            capacity_request_hash=str(request.request_hash),
            capacity_receipt_ref=receipt_ref,
            capacity_receipt_hash=str(receipt.receipt_hash),
            program_inputs=tuple(updated_inputs),
        )
        post_ref = store.publish(artifact_kind=O4ArtifactKind.INPUT_BUNDLE, model=post_bundle, semantic_hash=str(post_bundle.input_bundle_hash))
        return {"command": "plan-capacity", "input_bundle_ref": post_ref, "bundle": post_bundle, "capacity_receipt": receipt, "ok": True}

    def compile_phase1e(
        self,
        *,
        input_bundle_ref: AdvisoryImmutableArtifactRef,
        env_file: Path,
        artifact_root: Path,
    ) -> dict[str, Any]:
        store = self._input_store(artifact_root=artifact_root, env_file=env_file, code_commit=None)
        from backend.services.advisory_dev_input_onboarding.contracts import Phase1ERealInputBundle
        bundle = store.load(ref=input_bundle_ref, model_type=Phase1ERealInputBundle)
        if bundle.capacity_request_ref is None or bundle.capacity_receipt_ref is None or bundle.source_requirement_registry_ref is None:
            raise RealDevOnboardingError(REASON_O4_INPUT_PENDING, "compile-phase1e requires a post-capacity input bundle")
        capacity_request = store.load(ref=bundle.capacity_request_ref, model_type=Phase1ECapacityPlanningRequestV2)
        capacity_receipt = store.load(ref=bundle.capacity_receipt_ref, model_type=Phase1ECapacityPlanningReceiptV2)
        _, readonly_factory = self._dev_read_factory(env_file=env_file, expected_identity_hash=bundle.target_database_identity_hash)
        plan_store = ContentAddressedPlanStore(root=artifact_root, policy_hash=O4_ARTIFACT_STORE_POLICY_HASH)
        results: list[Phase1ECompileProgramResult] = []
        for item in bundle.program_inputs:
            if item.plan_readiness is not ProgramPlanReadiness.FULL_READY:
                status = (
                    Phase1ECompileProgramStatus.BLOCKED
                    if item.plan_readiness is ProgramPlanReadiness.BLOCKED
                    else Phase1ECompileProgramStatus.PENDING
                )
                results.append(
                    Phase1ECompileProgramResult(
                        program_id=item.program_id,
                        decision_trade_date=item.decision_trade_date,
                        status=status,
                        reason_codes=item.reason_codes or (REASON_O4_INPUT_PENDING,),
                    )
                )
                continue
            try:
                dependency = store.load(ref=item.compiler_dependency_ref, model_type=Phase1EProgramCompilerDependency)
                program_request = store.load(ref=item.phase1e_program_date_request_ref, model_type=Phase1EProgramDateRequest)
                coverage = store.load(ref=item.capacity_coverage_ref, model_type=Phase1ECapacityProgramCoverageV1)
                source_artifact = store.load(ref=item.source_resolution_receipt_ref, model_type=AdvisorySourceResolutionArtifact)
                batch = build_phase1e_batch_request(
                    program_input=item,
                    program_date_request=program_request,
                    compiler_dependency=dependency,
                    source_requirement_registry_hash=bundle.source_requirement_registry_hash,
                    capacity_request_ref=bundle.capacity_request_ref,
                    capacity_receipt_ref=bundle.capacity_receipt_ref,
                    capacity_coverage=coverage,
                )
                if batch is None:
                    raise RealDevOnboardingError(REASON_O4_COMPILE_EMPTY, "FULL_READY Program did not produce a compiler batch")
                batch_ref = store.publish(
                    artifact_kind=O4ArtifactKind.PHASE1E_BATCH_REQUEST,
                    model=batch,
                    semantic_hash=str(batch.invocation_request_hash),
                )
                policy_artifact = store.load(ref=dependency.phase0a_policy_registry_ref, model_type=Phase0APolicyRegistryArtifact)
                policy = Phase0APolicyRegistry.model_validate({**policy_artifact.payload, "registry_content_hash": dependency.phase0a_policy_registry_hash})
                provider = PostgresPhase1EInputProvider(
                    projection=AdvisoryPostgresEvidenceProjection(readonly_factory),
                    policy=policy,
                )
                compiler = Phase1EReadinessPlanCompiler(
                    source_requirement_compiler=FrozenO4SourceRequirementCompiler(
                        registry_hash=bundle.source_requirement_registry_hash,
                        requirement_set=source_artifact.phase1_requirement_set,
                    ),
                    capacity_request=capacity_request,
                    capacity_receipt=capacity_receipt,
                    capacity_program_coverage=coverage,
                    precomputed_audit_outcome=Phase1EAuditOutcome(
                        request=dependency.phase0a_audit_request,
                        receipt=dependency.phase0a_audit_receipt,
                        handoff_report=dependency.handoff_readiness_report,
                        handoff_bundle=dependency.phase1_handoff_bundle,
                    ),
                    artifact_store=plan_store,
                )
                try:
                    plans, batch_receipt = compiler.compile_batch(request=batch, provider=provider)
                finally:
                    provider.close_program_date()
                plan_refs = tuple(
                    self._plan_ref(plan_store=plan_store, kind="plan", identity=str(plan.plan_hash))
                    for plan in plans
                )
                batch_receipt_ref = self._plan_ref(
                    plan_store=plan_store,
                    kind="batch",
                    identity=str(batch_receipt.batch_receipt_hash),
                )
                if batch_receipt.failed_input_scopes or not plans:
                    reasons = tuple(item.reason_code for item in batch_receipt.failed_input_scopes) or (REASON_O4_COMPILE_EMPTY,)
                    results.append(
                        Phase1ECompileProgramResult(
                            program_id=item.program_id,
                            decision_trade_date=item.decision_trade_date,
                            status=Phase1ECompileProgramStatus.FAILED,
                            phase1e_batch_request_ref=batch_ref,
                            phase1e_batch_request_hash=str(batch.invocation_request_hash),
                            plan_refs=plan_refs,
                            batch_receipt_ref=batch_receipt_ref,
                            batch_receipt_hash=str(batch_receipt.batch_receipt_hash),
                            reason_codes=tuple(sorted(set(reasons))),
                        )
                    )
                    continue
                results.append(
                    Phase1ECompileProgramResult(
                        program_id=item.program_id,
                        decision_trade_date=item.decision_trade_date,
                        status=Phase1ECompileProgramStatus.COMPLETE,
                        phase1e_batch_request_ref=batch_ref,
                        phase1e_batch_request_hash=str(batch.invocation_request_hash),
                        plan_refs=plan_refs,
                        batch_receipt_ref=batch_receipt_ref,
                        batch_receipt_hash=str(batch_receipt.batch_receipt_hash),
                    )
                )
            except Exception as exc:
                LOGGER.exception("advisory_o4_compile_failed program_id=%s", item.program_id)
                results.append(
                    Phase1ECompileProgramResult(
                        program_id=item.program_id,
                        decision_trade_date=item.decision_trade_date,
                        status=Phase1ECompileProgramStatus.FAILED,
                        reason_codes=(getattr(exc, "reason_code", None) or "ADVISORY_PHASE1E_UNEXPECTED_ERROR",),
                    )
                )
        statuses = {item.status for item in results}
        aggregate = (
            Phase1ECompileAggregateStatus.COMPLETE
            if statuses == {Phase1ECompileProgramStatus.COMPLETE}
            else Phase1ECompileAggregateStatus.FAILED
            if statuses <= {Phase1ECompileProgramStatus.BLOCKED, Phase1ECompileProgramStatus.FAILED}
            else Phase1ECompileAggregateStatus.PARTIAL
        )
        receipt = Phase1ECompileReceipt(
            input_bundle_ref=input_bundle_ref,
            input_bundle_hash=str(bundle.input_bundle_hash),
            program_results=tuple(results),
            aggregate_status=aggregate,
        )
        receipt_ref = store.publish(
            artifact_kind=O4ArtifactKind.PHASE1E_COMPILE_RECEIPT,
            model=receipt,
            semantic_hash=str(receipt.compile_receipt_hash),
        )
        return {"command": "compile-phase1e", "compile_receipt_ref": receipt_ref, "compile_receipt": receipt, "ok": aggregate is Phase1ECompileAggregateStatus.COMPLETE}

    def _publish_common_artifacts(
        self,
        *,
        store: Phase1EInputArtifactStore,
        config: SourceObserverConfigBundle,
        store_backend_root: Path,
        policy: Phase0APolicyRegistry | None,
    ) -> dict[str, AdvisoryImmutableArtifactRef]:
        query = SourceQueryRegistryArtifact(payload=config.query_registry_payload(SOURCE_QUERY_TEMPLATES))
        observer = ObserverConfigArtifact(payload=config.canonical_payload(SOURCE_QUERY_TEMPLATES))
        partition = PartitionPolicyArtifact(payload=PARTITION_POLICY_PAYLOAD)
        store_policy = StoreBackendPolicyArtifact(
            payload={
                "schema_version": "advisory_o4_store_backend_policy_v1",
                "root_identity_hash": canonical_json_sha256(str(store_backend_root.resolve())),
                "external_only": True,
            }
        )
        artifact_policy = ArtifactStorePolicyArtifact(payload=O4_ARTIFACT_STORE_POLICY_PAYLOAD)
        models = {
            "source_query_registry_ref": (O4ArtifactKind.SOURCE_QUERY_REGISTRY, query),
            "observer_config_ref": (O4ArtifactKind.OBSERVER_CONFIG, observer),
            "partition_policy_ref": (O4ArtifactKind.PARTITION_POLICY, partition),
            "store_backend_policy_ref": (O4ArtifactKind.STORE_BACKEND_POLICY, store_policy),
            "artifact_store_policy_ref": (O4ArtifactKind.ARTIFACT_STORE_POLICY, artifact_policy),
        }
        if query.content_hash != config.query_registry_hash(SOURCE_QUERY_TEMPLATES):
            raise ValueError("source query registry typed artifact hash differs from compiled config")
        if observer.content_hash != config.config_hash(SOURCE_QUERY_TEMPLATES):
            raise ValueError("observer config typed artifact hash differs from compiled config")
        if artifact_policy.content_hash != O4_ARTIFACT_STORE_POLICY_HASH:
            raise ValueError("artifact store policy typed artifact hash differs from the O4 store policy")
        if policy is not None:
            phase0_payload = policy.model_dump(mode="python", exclude={"registry_content_hash"})
            phase0 = Phase0APolicyRegistryArtifact(payload=phase0_payload)
            if phase0.content_hash != policy.registry_content_hash:
                raise ValueError("Phase0A policy typed artifact hash differs from the frozen registry")
            models["phase0a_policy_registry_ref"] = (O4ArtifactKind.PHASE0A_POLICY_REGISTRY, phase0)
        return {
            name: store.publish(artifact_kind=kind, model=model, semantic_hash=str(model.content_hash))
            for name, (kind, model) in models.items()
        }

    def _compiler_dependency(self, **kwargs: Any) -> Phase1EProgramCompilerDependency:
        request = kwargs["request"]
        receipt = kwargs["receipt"]
        result = kwargs["historical_result"]
        spec = kwargs["spec"]
        audit = kwargs["audit"]
        scope = kwargs["scope"]
        common = kwargs["common"]
        calendar_ref = kwargs["calendar_ref"]
        return Phase1EProgramCompilerDependency(
            program_id=spec.program_id,
            decision_trade_date=request.decision_trade_date,
            package_id=spec.package_id,
            manifest_sha256=spec.manifest_sha256,
            alpha_mode=spec.alpha_mode,
            style_family=spec.style,
            historical_program_run_id=str(result.historical_program_run_id),
            historical_batch_receipt_ref=kwargs["historical_receipt_ref"],
            historical_batch_receipt_hash=str(receipt.receipt_hash),
            phase0a_audit_request=audit.request,
            phase0a_audit_receipt=audit.receipt,
            handoff_readiness_report=audit.handoff_report,
            phase1_handoff_bundle=audit.handoff_bundle,
            admission_scope_id=scope.admission_scope_id if scope is not None else None,
            admission_scope_hash=scope.admission_scope_hash if scope is not None else None,
            phase0a_policy_registry_ref=common["phase0a_policy_registry_ref"],
            phase0a_policy_registry_hash=common["phase0a_policy_registry_ref"].semantic_hash,
            source_query_registry_ref=common["source_query_registry_ref"],
            source_query_registry_hash=common["source_query_registry_ref"].semantic_hash,
            observer_config_ref=common["observer_config_ref"],
            observer_config_hash=common["observer_config_ref"].semantic_hash,
            calendar_identity_ref=calendar_ref,
            calendar_identity_hash=calendar_ref.semantic_hash,
            dataset_schema_fingerprint=self._dataset_schema_fingerprint(),
            partition_policy_ref=common["partition_policy_ref"],
            partition_policy_hash=common["partition_policy_ref"].semantic_hash,
            store_backend_policy_ref=common["store_backend_policy_ref"],
            store_backend_policy_hash=common["store_backend_policy_ref"].semantic_hash,
            artifact_store_policy_ref=common["artifact_store_policy_ref"],
            artifact_store_policy_hash=common["artifact_store_policy_ref"].semantic_hash,
            compiler_version="phase1e-readiness-plan-v1",
            serializer_version="advisory-phase1e-canonical-v1",
            compiler_source_hash=self._compiler_source_hash(),
        )

    def _physical_requirement_set(self, *, context: Mapping[str, Any]) -> tuple[Any, tuple[ProgramWindowLineage, ...]]:
        evidence = context["program_request_evidence"]
        payload = evidence.selection_evidence.evidence_payload_json
        actual_cutoff = self._dse_decision_cutoff(evidence=evidence)
        if actual_cutoff > context["observation"].decision_cutoff_ts:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "actual DSE decision cutoff exceeds the frozen pre-observation cutoff",
            )
        lineages = self._dse_window_lineages(payload=payload, evidence=evidence)
        receipts = tuple(PersistedDseSourceReadReceipt.model_validate(item) for item in payload["phase0a_source_evidence"])
        return reconcile_dse_and_build_requirement_set(
            observation_scope=context["observation"],
            observation_scope_ref=context["observation_ref"],
            dse_evidence_hash=evidence.selection_evidence.artifact_hash,
            selection_artifact_hash=str(evidence.selection_artifact.artifact_payload_sha256),
            source_receipts=receipts,
            window_lineages=lineages,
        ), lineages

    def _phase1_requirement_set(self, *, physical_set: Any, mapping: Any, context: Mapping[str, Any]) -> SourceRequirementSet:
        dependency = context["dependency"]
        scope = context["scope"]
        target = context["target"]
        if scope is None:
            raise RealDevOnboardingError(REASON_O4_INPUT_PENDING, "audit produced no admission scope")
        universe_policy_hash = Phase1EReadinessPlanCompiler._universe_policy_hash(
            target=target,
            decision_date=physical_set.decision_trade_date,
        )
        if universe_policy_hash is None:
            raise RealDevOnboardingError(REASON_O4_INPUT_PENDING, "audit produced no universe policy identity")
        source_cutoff = context["program_request"].label_as_of_ts
        common_hash = build_source_requirement_common_pit_identity_hash(
            admission_scope_id=scope.admission_scope_id,
            admission_scope_hash=scope.admission_scope_hash,
            handoff_readiness_hash=dependency.handoff_readiness_report.handoff_readiness_hash,
            program_id=physical_set.program_id,
            binding_version_id=str(context["program_request_evidence"].dated_binding.binding_version_id),
            package_id=dependency.package_id,
            manifest_sha256=dependency.manifest_sha256,
            alpha_mode=dependency.alpha_mode.value,
            decision_as_of_trade_date=physical_set.decision_trade_date,
            requested_source_cutoff=source_cutoff,
            query_registry_hash=dependency.source_query_registry_hash,
            calendar_hash=dependency.calendar_identity_hash,
            universe_policy_hash=universe_policy_hash,
            data_source="DB_HISTORICAL",
            execution_origin=EvidenceOrigin.MANUAL_HISTORICAL_RESEARCH.value,
            research_scope="HISTORICAL_RESEARCH_ONLY",
            execution_prohibited=True,
            research_only=True,
        )
        physical_by_identity = {
            (item.observer_query_template_id, item.observer_query_template_version): item
            for entry in mapping.entries for item in entry.physical_requirements
        }
        requirements = []
        for index, item in enumerate(physical_set.physical_requirements):
            physical = physical_by_identity[(item.query_template_id, item.query_template_version)]
            dates = [date.fromisoformat(str(value)) for value in item.partition_key.values() if self._is_iso_date(value)]
            business_min = min(dates) if dates else physical_set.decision_trade_date
            business_max = max(dates) if dates else physical_set.decision_trade_date
            requirements.append(
                SourceRequirement(
                    consumer_scope_id=f"{scope.admission_scope_id}:{index}",
                    source_role=item.source_role,
                    dataset_name=item.dataset_name,
                    query_template_id=item.query_template_id,
                    query_template_version=item.query_template_version,
                    query_template_hash=physical.observer_query_template_hash,
                    bound_parameters=item.partition_key,
                    bound_parameter_hash=canonical_json_sha256(item.partition_key),
                    partition_key=item.partition_key,
                    revision_kind=SourceRevisionKind.PARTITION_CONTENT_HASH,
                    availability_requirement=AvailabilityRequirement.DECISION_CUTOFF,
                    business_min_date=business_min,
                    business_max_date=business_max,
                    requested_cutoff=source_cutoff,
                    enforced_cutoff_predicate_hash=physical.cutoff_predicate_hash,
                    common_pit_identity_hash=common_hash,
                )
            )
        return SourceRequirementSet(
            admission_scope_id=scope.admission_scope_id,
            admission_scope_hash=scope.admission_scope_hash,
            handoff_readiness_hash=dependency.handoff_readiness_report.handoff_readiness_hash,
            program_id=physical_set.program_id,
            binding_version_id=str(context["program_request_evidence"].dated_binding.binding_version_id),
            package_id=dependency.package_id,
            manifest_sha256=dependency.manifest_sha256,
            alpha_mode=dependency.alpha_mode.value,
            decision_as_of_trade_date=physical_set.decision_trade_date,
            requested_source_cutoff=source_cutoff,
            label_as_of_ts=context["program_request"].label_as_of_ts,
            query_registry_hash=dependency.source_query_registry_hash,
            calendar_hash=dependency.calendar_identity_hash,
            universe_policy_hash=universe_policy_hash,
            formal_oos_status=scope.formal_oos_status.value,
            evidence_scope=scope.evidence_scope.value,
            requirements=tuple(requirements),
        )

    def _source_only_workload(self, *, context: Mapping[str, Any], requirement_set_hash: str) -> Phase1EProgramCapacityWorkload:
        evidence = context["program_request_evidence"]
        payload = evidence.selection_evidence.evidence_payload_json
        universe = int(evidence.selection_artifact.universe_count)
        if universe <= 0:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "selection artifact does not carry a positive authoritative universe count",
            )
        point_in_time = payload.get("point_in_time_context") or {}
        parent_universe = point_in_time.get("parent_input_universe_count")
        if parent_universe is not None and int(parent_universe) != universe:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "multi-Alpha parent universe count differs from the selection artifact",
            )
        universe_evidence = payload.get("phase0a_universe_evidence") or {}
        package_layers = [
            item for item in universe_evidence.get("layers", [])
            if item.get("layer") == "package_eligible_universe"
        ]
        if len(package_layers) != 1 or int(package_layers[0].get("output_count", -1)) != universe:
            raise RealDevOnboardingError(
                REASON_SOURCE_MAPPING_CONFLICT,
                "package-eligible universe evidence differs from the selection artifact",
            )
        return Phase1EProgramCapacityWorkload(
            program_id=context["program_date"].program_id,
            decision_trade_date=context["program_date"].decision_trade_date,
            style_family=context["program_date"].style_family,
            package_id=context["program_date"].package_id,
            manifest_sha256=context["program_date"].manifest_sha256,
            alpha_mode=context["program_date"].alpha_mode,
            candidate_depth=len(evidence.selection_artifact.scores_json),
            input_universe_count=int(universe),
            workload_scope="SOURCE_CAPTURE_ONLY",
            horizons=(),
            projection_count=0,
            stage_projection_factor=0,
            source_requirement_set_hash=requirement_set_hash,
        )

    def _program_date_request(self, *, request: Any, receipt: Any, historical_result: Any, observation: Any | None) -> Phase1EProgramDateRequest:
        spec = next(item for item in request.program_specs if item.program_id == historical_result.program_id)
        return Phase1EProgramDateRequest(
            program_id=historical_result.program_id,
            decision_trade_date=request.decision_trade_date,
            evidence_origin=EvidenceOrigin.MANUAL_HISTORICAL_RESEARCH,
            expected_package_id=historical_result.package_id,
            expected_manifest_sha256=spec.manifest_sha256,
            expected_alpha_mode=historical_result.alpha_mode.value,
            expected_style_family=spec.style,
            historical_batch_receipt_ref=receipt.batch_id,
            label_as_of_ts=observation.decision_cutoff_ts if observation is not None else self._aware_now(),
        )

    @staticmethod
    def _dse_decision_cutoff(*, evidence: Phase1EProgramDateEvidence) -> datetime:
        raw = evidence.selection_evidence.evidence_payload_json.get("decision_clock", {}).get("decision_cutoff_ts")
        value = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if value.tzinfo is None or value.utcoffset() is None:
            raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "DSE decision cutoff is not timezone-aware")
        return value.astimezone(UTC)

    @staticmethod
    def _single_scope(scopes: tuple[Any, ...]) -> Any | None:
        eligible = tuple(item for item in scopes if item.formal_oos_status.value != "FORMAL_OOS")
        if len(eligible) > 1:
            raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "one Program/date produced multiple admission scopes")
        return eligible[0] if eligible else None

    def _calendar_artifact_from_evidence(self, *, evidence: Phase1EProgramDateEvidence) -> CalendarIdentityArtifact:
        payload = evidence.selection_evidence.evidence_payload_json.get("decision_clock") or {}
        if not payload:
            raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "DSE decision clock is missing")
        return CalendarIdentityArtifact(payload=payload)

    def _dse_window_lineages(self, *, payload: Mapping[str, Any], evidence: Phase1EProgramDateEvidence) -> tuple[ProgramWindowLineage, ...]:
        context = dict(payload.get("point_in_time_context") or {})
        effective = date.fromisoformat(str(context["effective_trade_date"]))
        per_leg = context.get("per_leg_window_lineage")
        rows = []
        if isinstance(per_leg, dict) and per_leg:
            items = sorted(per_leg.items())
        else:
            items = [(None, context)]
        for component_id, raw in items:
            start = date.fromisoformat(str(raw["window_start_date"]))
            trading_dates = tuple(evidence.audit_readers.calendar.list_trading_days(start_date=start, end_date=effective))
            rows.append(
                ProgramWindowLineage(
                    alpha_component_id=component_id,
                    window_start_date=start,
                    effective_trade_date=effective,
                    required_window=int(raw["required_window"]),
                    window_resolution=str(raw["window_resolution"]),
                    window_lineage_hash=str(raw["window_lineage_hash"]),
                    trading_dates=trading_dates,
                )
            )
        return tuple(rows)

    @staticmethod
    def _pre_observation_lineages(*, projection: Any, decision_trade_date: date, calendar_reader: Any) -> tuple[ProgramWindowLineage, ...]:
        rows = []
        dates = tuple(
            calendar_reader.list_trading_days(
                start_date=decision_trade_date - timedelta(days=3000),
                end_date=decision_trade_date,
            )
        )
        for leg in projection.legs:
            window_policy = PARTITION_POLICY_PAYLOAD["inference_window"]
            required_calendar_days = leg.required_window + int(window_policy["buffer_trading_days"])
            selected = dates[-required_calendar_days:]
            if len(selected) < required_calendar_days:
                raise RealDevOnboardingError(REASON_O4_INPUT_PENDING, "trading calendar does not cover the projected window")
            component_id = leg.alpha_component_id if projection.alpha_mode.value == "multi_alpha" else None
            calendar_source = str(window_policy["calendar_source"])
            calendar_identity_hash = canonical_json_sha256(
                {
                    "dataset_id": calendar_source,
                    "effective_trade_date": selected[-1].isoformat(),
                    "calendar_version": str(window_policy["calendar_version"]),
                    "calendar_source": calendar_source,
                }
            )
            window_resolution = str(window_policy["window_resolution"])
            lineage_hash = canonical_json_sha256(
                {
                    "calendar_identity_hash": calendar_identity_hash,
                    "window_start_date": selected[0].isoformat(),
                    "required_window": leg.required_window,
                    "window_resolution": window_resolution,
                }
            )
            rows.append(
                ProgramWindowLineage(
                    alpha_component_id=component_id,
                    window_start_date=selected[0],
                    effective_trade_date=selected[-1],
                    required_window=leg.required_window,
                    window_resolution=window_resolution,
                    window_lineage_hash=lineage_hash,
                    trading_dates=selected,
                )
            )
        return tuple(rows)

    @staticmethod
    def _calendar_payload(*, lineages: tuple[ProgramWindowLineage, ...], decision_trade_date: date) -> dict[str, Any]:
        return {
            "schema_version": "advisory_o4_calendar_identity_v1",
            "decision_trade_date": decision_trade_date,
            "lineages": [item.model_dump(mode="json") for item in lineages],
        }

    @staticmethod
    def _dataset_schema_fingerprint() -> str:
        return canonical_json_sha256(
            [
                {
                    "template_id": template.template_id,
                    "template_version": template.template_version,
                    "schema_fingerprint": template.schema_fingerprint,
                }
                for template in sorted(
                    SOURCE_QUERY_TEMPLATES.values(),
                    key=lambda item: (item.template_id, item.template_version),
                )
            ]
        )

    @staticmethod
    def _prospective_decision_cutoff(*, calendar_reader: Any, decision_trade_date: date) -> datetime:
        target_dates = calendar_reader.list_trading_days(
            start_date=decision_trade_date + timedelta(days=1),
            end_date=decision_trade_date + timedelta(days=30),
        )
        if not target_dates:
            raise RealDevOnboardingError(
                REASON_O4_INPUT_PENDING,
                "trading calendar does not provide the next target trade date",
            )
        return prospective_target_entry_cutoff(target_dates[0])

    @staticmethod
    def _dated_binding(*, repository: Any, program_id: str, trade_date: date, package_id: str) -> Any:
        matches = [
            item for item in repository.list_binding_versions(program_id)
            if item.effective_from_trade_date is not None
            and item.effective_from_trade_date <= trade_date
            and (item.effective_to_trade_date is None or trade_date <= item.effective_to_trade_date)
            and package_id in item.package_ids
        ]
        if len(matches) != 1:
            raise RealDevOnboardingError(REASON_SOURCE_MAPPING_CONFLICT, "dated Program binding is not unique")
        return matches[0]

    def _input_store(self, *, artifact_root: Path, env_file: Path, code_commit: str | None) -> Phase1EInputArtifactStore:
        root = artifact_root.expanduser().resolve()
        configured = self._env_values(env_file).get("AISTOCK_ADVISORY_PHASE1E_ARTIFACT_ROOT")
        if configured and Path(configured).expanduser().resolve() != root:
            raise ValueError("explicit artifact root conflicts with the env-file artifact root")
        return Phase1EInputArtifactStore(root=root, producer_code_commit=code_commit)

    def _dataset_store_root(self, *, env_file: Path, explicit_root: Path | None) -> Path:
        configured = self._env_values(env_file).get("AISTOCK_ADVISORY_DATASET_STORE_ROOT")
        if explicit_root is None and not configured:
            raise ValueError("explicit Advisory dataset store root is required")
        root = explicit_root.expanduser().resolve() if explicit_root is not None else Path(str(configured)).expanduser().resolve()
        if configured and explicit_root is not None and Path(configured).expanduser().resolve() != root:
            raise ValueError("explicit Advisory dataset store root conflicts with the env file")
        if not root.is_dir():
            raise ValueError("Advisory dataset store root does not exist")
        return root

    def _dev_write_factory(self, *, env_file: Path, expected_identity_hash: str) -> tuple[DatabaseConnectionConfig, Any]:
        config = resolve_database_connection(target_label=TargetLabel.DEV, env_file=env_file)
        factory = ExactDevConnectionFactory(config, expected_database_identity_hash=expected_identity_hash)
        with factory() as connection:
            if database_identity_hash(_database_identity(connection=connection, config=config)) != expected_identity_hash:
                raise ValueError("DEV database identity differs from the request")
        return config, factory

    def _dev_read_factory(self, *, env_file: Path, expected_identity_hash: str) -> tuple[DatabaseConnectionConfig, Any]:
        config = resolve_database_connection(target_label=TargetLabel.DEV, env_file=env_file)

        @contextmanager
        def factory() -> Iterator[Any]:
            with readonly_onboarding_connection(config) as connection:
                identity = _database_identity(connection=connection, config=config)
                if database_identity_hash(identity) != expected_identity_hash:
                    raise ValueError("read-only DEV database identity differs from the request")
                yield connection

        with factory():
            pass
        return config, factory

    @staticmethod
    def _observer_config(*, config_id: str, config_version: str) -> SourceObserverConfigBundle:
        config = registered_source_observer_configs().get((config_id, config_version))
        if config is None:
            raise ValueError("unknown compiled DEV source observer config")
        return config

    @staticmethod
    def _env_values(env_file: Path) -> dict[str, str]:
        path = env_file.expanduser().resolve(strict=True)
        values: dict[str, str] = {}
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
        return values

    def _aware_now(self) -> datetime:
        value = self.now_provider()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("O4 orchestration clock must be timezone-aware")
        return value.astimezone(UTC)

    def _compiler_source_hash(self) -> str:
        path = self.repository_root / "backend/services/advisory_phase1/readiness_plan.py"
        return hashlib.sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def _plan_ref(*, plan_store: ContentAddressedPlanStore, kind: str, identity: str) -> AdvisoryImmutableArtifactRef:
        document = plan_store.verify(kind=kind, identity=identity, semantic_hash=identity)
        return AdvisoryImmutableArtifactRef(
            artifact_kind=f"phase1e_{kind}",
            store_policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
            relative_path=plan_store.relative_path(kind=kind, identity=identity, semantic_hash=identity),
            semantic_hash=identity,
            file_sha256=str(document["file_sha256"]),
        )

    @staticmethod
    def _is_iso_date(value: Any) -> bool:
        try:
            date.fromisoformat(str(value))
        except ValueError:
            return False
        return True
