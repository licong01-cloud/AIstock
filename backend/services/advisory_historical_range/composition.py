"""Explicit composition roots for Phase 1R historical candidate production."""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

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
