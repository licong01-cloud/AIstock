"""Adapter-side BrokerBackend abstraction (Engine §3.6 / R-Q9).

Engine itself never imports anything from this package: each adapter selects a
concrete BrokerBackend per portfolio binding. Engine's OrderIntent contract is
backend-agnostic.

Concrete backends:
  - LocalSimBackend  : in-process matching against TDX/DB minute bars
                       (today's paper_trading_v2 default)
  - MiniQMTSimBackend: routes OrderIntent to miniQMT 仿真账户 (PR-005, not yet
                       implemented in this round)
"""

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
from .localsim import LocalSimBackend

__all__ = [
    "BrokerAccountSnapshot",
    "BrokerBackend",
    "BrokerBindCapacity",
    "CancelAck",
    "FillEvent",
    "LocalSimBackend",
    "MarketDataChannel",
    "OrderHandle",
    "OrderHandleStatus",
    "OrderHandleStatusState",
    "SubscriptionHandle",
]
