"""Adapter-side BrokerBackend abstraction (Engine §3.6 / R-Q9).

Engine itself never imports anything from this package: each adapter selects a
concrete BrokerBackend per portfolio binding. Engine's OrderIntent contract is
backend-agnostic.

Concrete backends:
  - LocalSimBackend  : in-process matching against TDX/DB minute bars
                       (today's paper_trading_v2 default)
  - MiniQMTSimBackend: routes OrderIntent to MiniQMT SIM account (PR-005)
"""

from typing import TYPE_CHECKING, Any

from .base import (
    BrokerAccountSnapshot,
    BrokerBackend,
    BrokerBindCapacity,
    CancelAck,
    FillEvent,
    MarketDataChannel,
    OrderHandle,
    OrderHandleStatus,
    OrderHandleStatusState,
    SubscriptionHandle,
)
if TYPE_CHECKING:
    from .localsim import LocalSimBackend
    from .minqmtsim import MiniQMTSimBackend


def __getattr__(name: str) -> Any:
    """Load concrete adapters on demand without creating a package import cycle."""

    if name == "LocalSimBackend":
        from .localsim import LocalSimBackend

        return LocalSimBackend
    if name == "MiniQMTSimBackend":
        from .minqmtsim import MiniQMTSimBackend

        return MiniQMTSimBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = [
    "BrokerAccountSnapshot",
    "BrokerBackend",
    "BrokerBindCapacity",
    "CancelAck",
    "FillEvent",
    "LocalSimBackend",
    "MiniQMTSimBackend",
    "MarketDataChannel",
    "OrderHandle",
    "OrderHandleStatus",
    "OrderHandleStatusState",
    "SubscriptionHandle",
]
