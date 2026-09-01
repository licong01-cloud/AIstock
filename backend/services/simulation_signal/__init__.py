"""Broker-neutral simulation signal layer."""

from .contracts import (
    DailySelectionEvidence,
    InMemorySelectionEvidenceRepository,
    RebalanceIntent,
    SelectionEvidenceRepository,
    TargetPortfolio,
    assert_selection_only_payload_boundary,
)
from .rebalance import RebalanceIntentService
from .strategy_package_selection import (
    DailySelectionSignalService,
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
)
from .target_portfolio import TargetPortfolioService

__all__ = [
    "DailySelectionEvidence",
    "DailySelectionSignalService",
    "InMemorySelectionEvidenceRepository",
    "RebalanceIntent",
    "RebalanceIntentService",
    "SelectionEvidenceRepository",
    "StrategyPackageSelectionResult",
    "StrategyPackageSelectionService",
    "TargetPortfolio",
    "TargetPortfolioService",
    "assert_selection_only_payload_boundary",
]
