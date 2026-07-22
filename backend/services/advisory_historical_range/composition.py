"""Explicit composition roots for Phase 1R historical candidate production."""

from __future__ import annotations

from collections.abc import Callable
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
