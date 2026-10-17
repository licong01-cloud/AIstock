"""Position timing advice bounded context.

The package owns advice intents and immutable advice artifacts.  It deliberately
does not own positions, market data, schedulers, notifications, or orders.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from .service import PositionTimingService

__all__ = ["PositionTimingService", "build_position_timing_service"]


def __getattr__(name: str) -> Any:
    """Keep offline research imports independent from the online service graph."""

    if name in __all__:
        from .service import PositionTimingService, build_position_timing_service

        return {
            "PositionTimingService": PositionTimingService,
            "build_position_timing_service": build_position_timing_service,
        }[name]
    raise AttributeError(name)
