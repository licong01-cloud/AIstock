"""Explicit composition roots for Phase 1R historical candidate production."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
import hashlib
import os
from pathlib import Path
from typing import Any

import psycopg2

from backend.services.strategy_package.historical_selection_providers import (
    build_historical_range_read_only_providers,
    historical_read_only_connection_factory,
)
from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.selection_computation import StrategyPackageSelectionComputation
from backend.services.strategy_package.selection_signal_preparation import (
    build_historical_strategy_package_signal_preparation,
)

from .artifact_store import HistoricalRangeArtifactStore
from .candidate_producer import HistoricalRangeCandidateProducer
from .catalog_postgres import PostgresHistoricalRangeSourceRevisionVerifier
from .catalog_postgres import PostgresHistoricalRangeCatalogExecutor
from .calendar_resolver import HistoricalRangeCalendarResolver
from .code_release import HistoricalRangeCodeReleaseResolver
from .decision_mark_provider import (
    HistoricalRangeDecisionMarkProvider,
    PostgresHistoricalRangeDecisionMarkReader,
)
from .executor import HistoricalRangeBatchExecutionService, HistoricalRangeDayExecutor
from .repository import PostgresHistoricalRangeRepository
from .outcome_evaluator import (
    FrozenHistoricalRangeOutcomeInputFactory,
    HistoricalRangeAggregateOutcomeEvaluator,
    HistoricalRangeOutcomePolicyProvider,
    HistoricalRangeOutcomeSubjectInputProvider,
    PostgresHistoricalRangeOutcomeSubjectInputProvider,
    PostgresHistoricalRangeOutcomeEvaluator,
    PostgresHistoricalRangeOutcomeInputIdentityResolver,
)
from .outcome_planner import (
    HistoricalRangeOutcomePlanner,
)
from .outcome_service import (
    HistoricalRangeOutcomeApplicationService,
    HistoricalRangeSummaryCoordinator,
)
from .outcome_source import PostgresHistoricalRangeOutcomeSourceProvider
from .outcome_policy_provider import (
    ArtifactHistoricalRangeOutcomePolicyProvider,
)
from .models import HistoricalRangeArtifactRefV1
from .retrospective_projection import (
    PostgresHistoricalRangeCandidateProjectionLoader,
)
from .dataset_bridge import (
    HistoricalRangeDatasetBridgeApplicationService,
    HistoricalRangeDatasetBridgeService,
    PostgresHistoricalRangeBridgeInputLoader,
)
from .dataset_bridge_postgres import PostgresHistoricalRangeBridgeAdapters
from backend.services.advisory_phase1.calculation_evidence import (
    LocalCalculationEvidenceStore,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from .models import HistoricalRangeSummaryPolicyV1
from .summary_service import (
    Phase1WinnerDefinitionV1,
    HistoricalRangeRecallDenominatorProvider,
    HistoricalRangeSummaryCoordinatorService,
    PostgresHistoricalRangeRecallDenominatorProvider,
    PostgresHistoricalRangeSummaryContextProvider,
    PostgresHistoricalRangeSummaryOutcomeSetLoader,
)
from .outcome_policy_catalog import (
    R4_DEFAULT_HORIZONS,
    R4_LONG_TREND_HORIZONS,
    load_historical_range_outcome_policy_catalog,
)
from .query_repository import PostgresHistoricalRangeQueryRepository
from .service import (
    HistoricalRangeApplicationService,
    HistoricalRangeRuntime,
    HistoricalRangeServiceError,
    RuntimeFactory,
)
from .planning_service import HistoricalRangePlanningService
from .request_resolver import (
    HistoricalRangeAdmittedPackageResolver,
    HistoricalRangeProgramResolver,
)
from .requirement_planner import HistoricalRangeSourceRequirementPlanner
from .runtime_factories import (
    HistoricalRangeR5BridgeRequestFactory,
    HistoricalRangeR5DerivedIdentities,
    HistoricalRangeR5OutcomeRequestFactory,
    HistoricalRangeR5PolicyRegistry,
    historical_range_store_identity,
    R5_OUTCOME_CONTRACT_VERSION,
    R5_PARTITION_POLICY_ID,
    R5_QUERY_REGISTRY_VERSION,
)
from backend.services.advisory_program import AdvisoryProgramPGRepository
from backend.services.strategy_package.models import PackageStatus


def build_historical_range_candidate_producer(
    *,
    conn_factory: Callable[[], Any],
    candidate_artifact_root: Path,
    task_runtime_root: Path,
    package_asset_root: Path,
    repository_root: Path,
    hmm_snapshot_provider: Any | None,
) -> HistoricalRangeCandidateProducer:
    """Build the isolated historical path without current Selection/Paper repositories."""

    if conn_factory is None:
        raise ValueError("historical candidate composition requires conn_factory")
    package_repository = StrategyPackageRepository(
        conn_factory=historical_read_only_connection_factory(conn_factory)
    )
    signal_preparation = build_historical_strategy_package_signal_preparation(
        package_reader=package_repository,
        package_asset_store=LocalPackageAssetStore(root=package_asset_root),
        runtime_root=task_runtime_root,
        repository_root=repository_root,
        hmm_snapshot_provider=hmm_snapshot_provider,
    )
    return HistoricalRangeCandidateProducer(
        signal_preparation=signal_preparation,
        computation=StrategyPackageSelectionComputation(),
        providers=build_historical_range_read_only_providers(conn_factory=conn_factory),
        source_verifier=PostgresHistoricalRangeSourceRevisionVerifier(conn_factory=conn_factory),
        artifact_store=HistoricalRangeArtifactStore(root=candidate_artifact_root),
    )


def build_historical_range_decision_mark_provider(
    *,
    conn_factory: Callable[[], Any],
    artifact_root: Path,
) -> HistoricalRangeDecisionMarkProvider:
    """Build the explicit DB-historical mark path with no inferred root."""

    if conn_factory is None:
        raise ValueError("historical decision-mark composition requires conn_factory")
    if artifact_root is None:
        raise ValueError("historical decision-mark composition requires artifact_root")
    return HistoricalRangeDecisionMarkProvider(
        reader=PostgresHistoricalRangeDecisionMarkReader(conn_factory=conn_factory),
        source_verifier=PostgresHistoricalRangeSourceRevisionVerifier(conn_factory=conn_factory),
        artifact_store=HistoricalRangeArtifactStore(root=artifact_root),
    )


def build_historical_range_batch_execution_service(
    *,
    conn_factory: Callable[[], Any],
    artifact_root: Path,
    task_runtime_root: Path,
    package_asset_root: Path,
    repository_root: Path,
    hmm_snapshot_provider: Any | None,
) -> HistoricalRangeBatchExecutionService:
    """Build the formal isolated R3 executor from explicit dependencies only."""

    if conn_factory is None:
        raise ValueError("historical execution composition requires conn_factory")
    required_paths = {
        "artifact_root": artifact_root,
        "task_runtime_root": task_runtime_root,
        "package_asset_root": package_asset_root,
        "repository_root": repository_root,
    }
    missing = sorted(name for name, value in required_paths.items() if value is None)
    if missing:
        raise ValueError(f"historical execution composition requires explicit paths: {missing}")
    artifact_store = HistoricalRangeArtifactStore(root=artifact_root)
    repository = PostgresHistoricalRangeRepository(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
    )
    candidate_producer = build_historical_range_candidate_producer(
        conn_factory=conn_factory,
        candidate_artifact_root=artifact_root,
        task_runtime_root=task_runtime_root,
        package_asset_root=package_asset_root,
        repository_root=repository_root,
        hmm_snapshot_provider=hmm_snapshot_provider,
    )
    decision_mark_provider = build_historical_range_decision_mark_provider(
        conn_factory=conn_factory,
        artifact_root=artifact_root,
    )
    return HistoricalRangeBatchExecutionService(
        day_executor=HistoricalRangeDayExecutor(
            repository=repository,
            artifact_store=artifact_store,
            candidate_producer=candidate_producer,
            decision_mark_provider=decision_mark_provider,
        )
    )


class PolicyBackedHistoricalRangeOutcomeCalendar:
    """Resolve T/E/S/X only from the explicitly frozen range policy."""

    def __init__(self, policy_provider: HistoricalRangeOutcomePolicyProvider) -> None:
        self._policy_provider = policy_provider

    def timeline(
        self,
        *,
        policy_bundle_hash: str,
        decision_trade_date: date,
        horizon_trade_days: int,
    ) -> tuple[date, date, date, date]:
        policy = self._policy_provider.load(policy_bundle_hash)
        return policy.calendar.timeline(
            decision_date=decision_trade_date,
            horizon_trading_days=horizon_trade_days,
        )

    def next_trading_day(
        self, *, policy_bundle_hash: str, current_trade_date: date
    ) -> date:
        policy = self._policy_provider.load(policy_bundle_hash)
        return policy.calendar.next_trading_day(current_trade_date)


def build_artifact_historical_range_outcome_policy_provider(
    *,
    artifact_root: Path,
    component_root: Path,
    policy_bundle_refs: dict[str, HistoricalRangeArtifactRefV1],
) -> ArtifactHistoricalRangeOutcomePolicyProvider:
    """Build the exact artifact/component policy catalog without current lookup."""

    if artifact_root is None or component_root is None or not policy_bundle_refs:
        raise ValueError("range policy composition requires explicit roots and refs")
    return ArtifactHistoricalRangeOutcomePolicyProvider(
        artifact_store=HistoricalRangeArtifactStore(root=artifact_root),
        component_root=component_root,
        policy_bundle_refs=policy_bundle_refs,
    )


def build_historical_range_outcome_application_service(
    *,
    conn_factory: Callable[[], Any],
    artifact_root: Path,
    policy_provider: HistoricalRangeOutcomePolicyProvider,
    producer_code_hash: str,
    outcome_contract_version: str,
    subject_input_provider: HistoricalRangeOutcomeSubjectInputProvider | None = None,
    summary_coordinator: HistoricalRangeSummaryCoordinator | None = None,
) -> HistoricalRangeOutcomeApplicationService:
    """Compose the real R4 read/write boundary from explicit dependencies.

    Package validation, current Selection lookup, and runtime inference are
    intentionally absent from this composition root.
    """

    if conn_factory is None or artifact_root is None:
        raise ValueError("R4 outcome composition requires explicit DB and artifact dependencies")
    if policy_provider is None:
        raise ValueError("R4 outcome composition requires a frozen policy provider")
    artifact_store = HistoricalRangeArtifactStore(root=artifact_root)
    repository = PostgresHistoricalRangeRepository(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
    )
    source_provider = PostgresHistoricalRangeOutcomeSourceProvider(
        conn_factory=historical_read_only_connection_factory(conn_factory)
    )
    subject_provider = subject_input_provider or PostgresHistoricalRangeOutcomeSubjectInputProvider(
        conn_factory=historical_read_only_connection_factory(conn_factory),
        source_provider=source_provider,
        policy_provider=policy_provider,
        candidate_projection_loader=PostgresHistoricalRangeCandidateProjectionLoader(
            conn_factory=historical_read_only_connection_factory(conn_factory),
            artifact_store=artifact_store,
            policy_provider=policy_provider,
        ),
    )
    input_factory = FrozenHistoricalRangeOutcomeInputFactory(
        subject_provider=subject_provider,
        policy_provider=policy_provider,
    )
    evaluator = PostgresHistoricalRangeOutcomeEvaluator(
        source_provider=source_provider,
        input_factory=input_factory,
        aggregate_evaluator=HistoricalRangeAggregateOutcomeEvaluator(
            provider=repository,
            artifact_store=artifact_store,
        ),
    )
    planner = HistoricalRangeOutcomePlanner(
        subject_provider=repository,
        calendar=PolicyBackedHistoricalRangeOutcomeCalendar(policy_provider),
        producer_code_hash=producer_code_hash,
        outcome_contract_version=outcome_contract_version,
        input_identity_resolver=PostgresHistoricalRangeOutcomeInputIdentityResolver(
            source_provider=source_provider,
            aggregate_provider=repository,
        ),
        latest_outcome=lambda outcome_logical_id: repository.load_latest_outcome(
            outcome_logical_id=outcome_logical_id
        ),
    )
    return HistoricalRangeOutcomeApplicationService(
        repository=repository,
        artifact_store=artifact_store,
        planner=planner,
        evaluator=evaluator,
        summary_coordinator=summary_coordinator,
    )


def build_historical_range_summary_coordinator(
    *,
    conn_factory: Callable[[], Any],
    artifact_root: Path,
    policy: HistoricalRangeSummaryPolicyV1,
    label_as_of_trade_date: date,
    producer_code_hash: str,
    denominator_provider: HistoricalRangeRecallDenominatorProvider | None = None,
    repository_root: Path | None = None,
    calculation_evidence_root: Path | None = None,
    calculation_evidence_store_identity: dict[str, Any] | None = None,
    dataset_store_root: Path | None = None,
    dataset_store_identity: dict[str, Any] | None = None,
    winner_definitions: tuple[Phase1WinnerDefinitionV1, ...] = (),
) -> HistoricalRangeSummaryCoordinatorService:
    """Compose exact-outcome summary refresh with decision-T-only context reads."""

    artifact_store = HistoricalRangeArtifactStore(root=artifact_root)
    repository = PostgresHistoricalRangeRepository(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
    )
    read_only_factory = historical_read_only_connection_factory(conn_factory)
    source_provider = PostgresHistoricalRangeOutcomeSourceProvider(
        conn_factory=read_only_factory
    )
    context_provider = PostgresHistoricalRangeSummaryContextProvider(
        conn_factory=read_only_factory,
        artifact_store=artifact_store,
        source_provider=source_provider,
    )
    recall_dependencies = (
        repository_root,
        calculation_evidence_root,
        calculation_evidence_store_identity,
        dataset_store_root,
        dataset_store_identity,
        winner_definitions or None,
    )
    if denominator_provider is None and any(item is not None for item in recall_dependencies):
        if any(item is None for item in recall_dependencies):
            raise ValueError(
                "Recall composition requires repository, dataset/evidence stores, and winner definitions together"
            )
        evidence_store = LocalCalculationEvidenceStore(
            root=calculation_evidence_root,
            repository_root=repository_root,
            store_identity=calculation_evidence_store_identity,
        )
        dataset_store = LocalContentAddressedStore(
            root=dataset_store_root,
            repository_root=repository_root,
            store_identity=dataset_store_identity,
        )
        denominator_provider = PostgresHistoricalRangeRecallDenominatorProvider(
            conn_factory=read_only_factory,
            dataset_store=dataset_store,
            calculation_evidence_reader=evidence_store,
            winner_definitions=winner_definitions,
        )
    return HistoricalRangeSummaryCoordinatorService(
        repository=repository,
        artifact_store=artifact_store,
        outcome_set_loader=PostgresHistoricalRangeSummaryOutcomeSetLoader(
            repository=repository,
            artifact_store=artifact_store,
            context_provider=context_provider,
            denominator_provider=denominator_provider,
        ),
        policy=policy,
        label_as_of_trade_date=label_as_of_trade_date,
        producer_code_hash=producer_code_hash,
    )


def build_historical_range_dataset_bridge_service(
    *,
    conn_factory: Callable[[], Any],
    artifact_root: Path,
    repository_root: Path,
    calculation_evidence_root: Path,
    calculation_evidence_store_identity: dict[str, Any],
    dataset_store_root: Path,
    dataset_store_identity: dict[str, Any],
    policy_provider: HistoricalRangeOutcomePolicyProvider,
    producer_code_hash: str,
    code_commit: str,
    query_registry_version: str,
    builder_hash: str,
    writer_hash: str,
    partition_policy_id: str,
) -> HistoricalRangeDatasetBridgeApplicationService:
    """Compose the exact-ref range bridge without formal selector fallback."""

    if conn_factory is None or policy_provider is None:
        raise ValueError("R4 bridge composition requires DB and policy dependencies")
    artifact_store = HistoricalRangeArtifactStore(root=artifact_root)
    repository = PostgresHistoricalRangeRepository(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
    )
    read_only_factory = historical_read_only_connection_factory(conn_factory)
    input_loader = PostgresHistoricalRangeBridgeInputLoader(
        repository=repository,
        projection_loader=PostgresHistoricalRangeCandidateProjectionLoader(
            conn_factory=read_only_factory,
            artifact_store=artifact_store,
            policy_provider=policy_provider,
        ),
    )
    evidence_store = LocalCalculationEvidenceStore(
        root=calculation_evidence_root,
        repository_root=repository_root,
        store_identity=calculation_evidence_store_identity,
    )
    dataset_store = LocalContentAddressedStore(
        root=dataset_store_root,
        repository_root=repository_root,
        store_identity=dataset_store_identity,
    )
    adapters = PostgresHistoricalRangeBridgeAdapters(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
        calculation_evidence_store=evidence_store,
        dataset_store=dataset_store,
        code_commit=code_commit,
        query_registry_version=query_registry_version,
        builder_hash=builder_hash,
        writer_hash=writer_hash,
        partition_policy_id=partition_policy_id,
    )
    bridge_service = HistoricalRangeDatasetBridgeService(
        artifact_store=artifact_store,
        capture_writer=adapters,
        dataset_builder=adapters,
        snapshot_writer=adapters,
        producer_code_hash=producer_code_hash,
        input_loader=input_loader,
    )
    return HistoricalRangeDatasetBridgeApplicationService(
        repository=repository,
        artifact_store=artifact_store,
        bridge_service=bridge_service,
    )


_R5_DB_ENV_KEYS = (
    "TDX_DB_HOST",
    "TDX_DB_PORT",
    "TDX_DB_NAME",
    "TDX_DB_USER",
    "TDX_DB_PASSWORD",
)

_R5_MUTATION_ENV_KEYS = (
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT",
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_TASK_RUNTIME_ROOT",
    "AISTOCK_PACKAGE_ASSET_STORE_ROOT",
    "AISTOCK_REPOSITORY_ROOT",
    "AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT",
    "AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT",
    "AISTOCK_ADVISORY_DATASET_STORE_ROOT",
)

_R5_CODE_RELEASE_CLOSURE = (
    "backend/services/advisory_historical_range/candidate_producer.py",
    "backend/services/advisory_historical_range/executor.py",
    "backend/services/advisory_historical_range/models.py",
    "backend/services/advisory_historical_range/repository.py",
    "backend/services/advisory_historical_range/semantics.py",
    "backend/services/strategy_package/selection_computation.py",
    "backend/services/strategy_package/selection_signal_preparation.py",
)

_R5_SELECTION_CLOSURE = (
    "backend/services/strategy_package/selection_computation.py",
    "backend/services/strategy_package/selection_signal_preparation.py",
)


def build_historical_range_r5_application_service(
    *,
    query_runtime_factory: RuntimeFactory,
    mutation_runtime_factory: RuntimeFactory,
) -> HistoricalRangeApplicationService:
    """Compose R5 without request-scoped connections or implicit dependencies."""

    if query_runtime_factory is None or mutation_runtime_factory is None:
        raise ValueError("R5 composition requires explicit query and mutation runtime factories")
    return HistoricalRangeApplicationService(
        query_runtime_factory=query_runtime_factory,
        mutation_runtime_factory=mutation_runtime_factory,
    )


def build_explicit_historical_range_r5_runtime_factory(
    *,
    conn_factory: Callable[[], Any],
    artifact_root: Path,
    task_runtime_root: Path,
    package_asset_root: Path,
    repository_root: Path,
    policy_component_root: Path,
    calculation_evidence_root: Path,
    dataset_store_root: Path,
    hmm_snapshot_provider: Any | None = None,
) -> RuntimeFactory:
    """Build the complete R1-R4 mutation runtime from explicit roots only."""

    configured_paths = {
        "artifact_root": artifact_root,
        "task_runtime_root": task_runtime_root,
        "package_asset_root": package_asset_root,
        "repository_root": repository_root,
        "policy_component_root": policy_component_root,
        "calculation_evidence_root": calculation_evidence_root,
        "dataset_store_root": dataset_store_root,
    }
    invalid = sorted(
        name
        for name, path in configured_paths.items()
        if path is None or not Path(path).is_absolute()
    )
    if invalid:
        raise HistoricalRangeServiceError(
            "ADVISORY_HR_CONFIGURATION_INVALID",
            "historical-range mutation roots must be explicit absolute paths",
            http_status=503,
            context={"invalid_configuration": invalid},
        )
    repository_root = repository_root.resolve(strict=True)
    policy_component_root.mkdir(parents=True, exist_ok=True)
    policy_component_root = policy_component_root.resolve(strict=True)
    artifact_store = HistoricalRangeArtifactStore(root=artifact_root)
    identities = HistoricalRangeR5DerivedIdentities.from_repository(repository_root)
    selection_hash = _historical_range_code_set_hash(
        repository_root, _R5_SELECTION_CLOSURE
    )
    store_identity = historical_range_store_identity()

    def runtime() -> HistoricalRangeRuntime:
        query = PostgresHistoricalRangeQueryRepository(conn_factory=conn_factory)
        repository = PostgresHistoricalRangeRepository(
            conn_factory=conn_factory,
            artifact_store=artifact_store,
        )
        read_only_factory = historical_read_only_connection_factory(conn_factory)
        package_repository = StrategyPackageRepository(conn_factory=read_only_factory)
        program_repository = AdvisoryProgramPGRepository(conn_factory=conn_factory)
        planning = HistoricalRangePlanningService(
            program_resolver=HistoricalRangeProgramResolver(
                package_resolver=HistoricalRangeAdmittedPackageResolver(
                    package_reader=package_repository
                ),
                program_reader=program_repository,
            ),
            calendar_resolver=HistoricalRangeCalendarResolver(
                conn_factory=read_only_factory
            ),
            code_release_resolver=HistoricalRangeCodeReleaseResolver(
                repository_root=repository_root,
                closure_paths=_R5_CODE_RELEASE_CLOSURE,
            ),
            requirement_planner=HistoricalRangeSourceRequirementPlanner(),
            catalog_executor=PostgresHistoricalRangeCatalogExecutor(
                conn_factory=read_only_factory
            ),
            repository=repository,
            artifact_store=artifact_store,
            selection_semantics_version="strategy_package_selection_semantics_v1",
            selection_semantics_hash=selection_hash,
        )
        execution = build_historical_range_batch_execution_service(
            conn_factory=conn_factory,
            artifact_root=artifact_root,
            task_runtime_root=task_runtime_root,
            package_asset_root=package_asset_root,
            repository_root=repository_root,
            hmm_snapshot_provider=hmm_snapshot_provider,
        )
        policy_provider = ArtifactHistoricalRangeOutcomePolicyProvider(
            artifact_store=artifact_store,
            component_root=policy_component_root,
            policy_bundle_refs={},
        )
        policy_registry = HistoricalRangeR5PolicyRegistry(
            conn_factory=conn_factory,
            repository=repository,
            query=query,
            artifact_store=artifact_store,
            component_root=policy_component_root,
            provider=policy_provider,
        )
        outcome_requests = HistoricalRangeR5OutcomeRequestFactory(
            policy_registry=policy_registry,
            identities=identities,
        )

        def outcome_service_factory(request: Any) -> HistoricalRangeOutcomeApplicationService:
            summary = build_historical_range_summary_coordinator(
                conn_factory=conn_factory,
                artifact_root=artifact_root,
                policy=outcome_requests.summary_policy(request),
                label_as_of_trade_date=request.label_as_of_trade_date,
                producer_code_hash=identities.summary_producer_hash,
            )
            return build_historical_range_outcome_application_service(
                conn_factory=conn_factory,
                artifact_root=artifact_root,
                policy_provider=policy_provider,
                producer_code_hash=identities.outcome_producer_hash,
                outcome_contract_version=R5_OUTCOME_CONTRACT_VERSION,
                summary_coordinator=summary,
            )

        outcome = build_historical_range_outcome_application_service(
            conn_factory=conn_factory,
            artifact_root=artifact_root,
            policy_provider=policy_provider,
            producer_code_hash=identities.outcome_producer_hash,
            outcome_contract_version=R5_OUTCOME_CONTRACT_VERSION,
        )
        bridge_requests = HistoricalRangeR5BridgeRequestFactory(
            conn_factory=conn_factory,
            policy_registry=policy_registry,
            artifact_store=artifact_store,
            identities=identities,
        )
        bridge = build_historical_range_dataset_bridge_service(
            conn_factory=conn_factory,
            artifact_root=artifact_root,
            repository_root=repository_root,
            calculation_evidence_root=calculation_evidence_root,
            calculation_evidence_store_identity=store_identity,
            dataset_store_root=dataset_store_root,
            dataset_store_identity=store_identity,
            policy_provider=policy_provider,
            producer_code_hash=identities.outcome_producer_hash,
            code_commit=identities.code_commit,
            query_registry_version=R5_QUERY_REGISTRY_VERSION,
            builder_hash=identities.bridge_builder_hash,
            writer_hash=identities.bridge_writer_hash,
            partition_policy_id=R5_PARTITION_POLICY_ID,
        )
        return HistoricalRangeRuntime(
            query=query,
            repository=repository,
            planning=planning,
            execution=execution,
            outcome=outcome,
            bridge=bridge,
            outcome_requests=outcome_requests,
            bridge_requests=bridge_requests,
            options_projector=lambda: _project_historical_range_options(conn_factory),
            artifact_store=artifact_store,
            outcome_service_factory=outcome_service_factory,
        )

    return runtime


def build_environment_historical_range_r5_application_service() -> HistoricalRangeApplicationService:
    """Build the HTTP service from explicit environment configuration only."""

    conn_factory = explicit_historical_range_connection_factory()

    def query_runtime() -> HistoricalRangeRuntime:
        query = PostgresHistoricalRangeQueryRepository(conn_factory=conn_factory)
        return HistoricalRangeRuntime(
            query=query,
            repository=None,  # type: ignore[arg-type]
            planning=None,  # type: ignore[arg-type]
            execution=None,  # type: ignore[arg-type]
            outcome=None,  # type: ignore[arg-type]
            bridge=None,  # type: ignore[arg-type]
            outcome_requests=None,  # type: ignore[arg-type]
            bridge_requests=None,  # type: ignore[arg-type]
            options_projector=lambda: _project_historical_range_options(conn_factory),
        )

    def mutation_runtime() -> HistoricalRangeRuntime:
        missing = [
            key for key in _R5_MUTATION_ENV_KEYS if not str(os.getenv(key) or "").strip()
        ]
        if missing:
            raise HistoricalRangeServiceError(
                "ADVISORY_HR_CONFIGURATION_UNAVAILABLE",
                "historical-range mutation runtime is not explicitly configured",
                http_status=503,
                retryable=True,
                context={"missing_configuration": missing},
            )
        try:
            factory = build_explicit_historical_range_r5_runtime_factory(
                conn_factory=conn_factory,
                artifact_root=Path(os.environ["AISTOCK_ADVISORY_HISTORICAL_RANGE_ARTIFACT_ROOT"]),
                task_runtime_root=Path(os.environ["AISTOCK_ADVISORY_HISTORICAL_RANGE_TASK_RUNTIME_ROOT"]),
                package_asset_root=Path(os.environ["AISTOCK_PACKAGE_ASSET_STORE_ROOT"]),
                repository_root=Path(os.environ["AISTOCK_REPOSITORY_ROOT"]),
                policy_component_root=Path(
                    os.environ["AISTOCK_ADVISORY_HISTORICAL_RANGE_POLICY_COMPONENT_ROOT"]
                ),
                calculation_evidence_root=Path(
                    os.environ["AISTOCK_ADVISORY_CALCULATION_EVIDENCE_ROOT"]
                ),
                dataset_store_root=Path(os.environ["AISTOCK_ADVISORY_DATASET_STORE_ROOT"]),
            )
            return factory()
        except HistoricalRangeServiceError:
            raise
        except Exception as exc:
            raise HistoricalRangeServiceError(
                "ADVISORY_HR_CONFIGURATION_INVALID",
                "historical-range mutation runtime configuration is invalid",
                http_status=503,
                retryable=False,
                context={"error_type": type(exc).__name__},
            ) from exc

    return build_historical_range_r5_application_service(
        query_runtime_factory=query_runtime,
        mutation_runtime_factory=mutation_runtime,
    )


def explicit_historical_range_connection_factory() -> Callable[[], Any]:
    missing = [key for key in _R5_DB_ENV_KEYS if os.getenv(key) is None]
    if missing:
        raise HistoricalRangeServiceError(
            "ADVISORY_HR_CONFIGURATION_UNAVAILABLE",
            "historical-range database configuration is incomplete",
            http_status=503,
            retryable=True,
            context={"missing_configuration": missing},
        )
    try:
        port = int(str(os.environ["TDX_DB_PORT"]))
    except ValueError as exc:
        raise HistoricalRangeServiceError(
            "ADVISORY_HR_CONFIGURATION_INVALID",
            "historical-range database port is invalid",
            http_status=503,
            context={"configuration_key": "TDX_DB_PORT"},
        ) from exc
    config = {
        "host": os.environ["TDX_DB_HOST"],
        "port": port,
        "dbname": os.environ["TDX_DB_NAME"],
        "user": os.environ["TDX_DB_USER"],
        "password": os.environ["TDX_DB_PASSWORD"],
        "application_name": "AIstock-advisory-historical-range-r5",
        "options": "-c client_encoding=utf8",
        "connect_timeout": 5,
    }

    def connect() -> Any:
        return psycopg2.connect(**config)

    return connect


def _project_historical_range_options(conn_factory: Callable[[], Any]) -> dict[str, Any]:
    program_repository = AdvisoryProgramPGRepository(conn_factory=conn_factory)
    package_repository = StrategyPackageRepository(conn_factory=historical_read_only_connection_factory(conn_factory))
    programs = []
    for program in program_repository.list_programs(include_archived=False):
        active = program_repository.get_active_binding_version(program.program_id)
        if active is None:
            continue
        programs.append(
            {
                "program_id": program.program_id,
                "name": program.program_name,
                "version": program.version,
                "active_binding_version_id": active.binding_version_id,
                "package_id": active.package_ids[0] if len(active.package_ids) == 1 else None,
                "target_count": program.target_count,
                "review_policy_summary": program.review_policy,
            }
        )
    admitted_statuses = {
        PackageStatus.SELECTION_ENABLED.value,
        PackageStatus.PAPER_ENABLED.value,
        PackageStatus.PAPER_RUNNING.value,
        PackageStatus.PAPER_PASSED.value,
    }
    packages = []
    for record in package_repository.list_summaries(limit=500):
        if str(record["package_status"]) not in admitted_statuses:
            continue
        packages.append(
            {
                "package_id": str(record["package_id"]),
                "name": str(record["package_name"]),
                "alpha_mode": str(record["alpha_mode"]),
                "component_count": int(record["alpha_count"]),
                "manifest_sha256": str(record["manifest_sha256"]),
                "package_version": str(record["package_version"]),
            }
        )
    catalog = load_historical_range_outcome_policy_catalog()
    return {
        "ok": True,
        "data": {
            "existing_programs": sorted(programs, key=lambda item: (item["name"], item["program_id"])),
            "admitted_packages": sorted(packages, key=lambda item: (item["name"], item["package_id"])),
            "outcome_catalog": {
                "catalog_version": catalog.catalog_version,
                "catalog_content_hash": catalog.catalog_content_hash,
                "default_horizons": list(R4_DEFAULT_HORIZONS),
                "long_trend_horizons": list(R4_LONG_TREND_HORIZONS),
                "allowed_maturity_statuses": ["COMPLETE", "CENSORED", "TERMINAL"],
            },
        },
    }


def _historical_range_code_set_hash(
    repository_root: Path, relative_paths: tuple[str, ...]
) -> str:
    digest = hashlib.sha256()
    for relative_path in relative_paths:
        path = (repository_root / relative_path).resolve(strict=True)
        path.relative_to(repository_root)
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()
