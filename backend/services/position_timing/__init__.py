"""Position timing advice bounded context.

The package owns advice intents and immutable advice artifacts.  It deliberately
does not own positions, market data, schedulers, notifications, or orders.
"""

from .service import PositionTimingService, build_position_timing_service

__all__ = ["PositionTimingService", "build_position_timing_service"]
