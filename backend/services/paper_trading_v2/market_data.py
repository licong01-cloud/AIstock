"""Compatibility import for the retired Paper-owned minute provider path.

The implementation is owned by :mod:`backend.services.simulation_data.minute_provider`.
Paper v2 callers retain this import until the final legacy-source retirement phase.
"""

from backend.services.simulation_data.minute_provider import (
    SimulationMinuteMarketDataProvider as PaperV2MinuteMarketDataProvider,
)
from backend.services.simulation_data.contracts import MinuteDataSource

__all__ = ["MinuteDataSource", "PaperV2MinuteMarketDataProvider"]
