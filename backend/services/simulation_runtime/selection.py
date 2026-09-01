"""Temporary import compatibility for the broker-neutral signal owner.

The selection implementation belongs exclusively to ``simulation_signal``.
This module contains no business logic and exists only so out-of-scope
consumers can migrate under their own validation lanes before physical
retirement removes the legacy import path.
"""

from backend.services.simulation_signal.strategy_package_selection import (
    DailySelectionSignalService,
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
)

__all__ = [
    "DailySelectionSignalService",
    "StrategyPackageSelectionResult",
    "StrategyPackageSelectionService",
]
