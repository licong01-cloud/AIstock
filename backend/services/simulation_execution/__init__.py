"""Broker-neutral simulation execution contracts.

Runtime implementations remain with their current owners until SIM-LR-B.  This
package is the sole owner of identities and handles shared by LocalSIM and
MiniQMT.
"""

from .broker import (
    BackendId,
    BrokerAccountSnapshot,
    BrokerBackend,
    BrokerBackendId,
    BrokerBindCapacity,
    CancelAck,
    FillEvent,
    MarketDataChannel,
    OrderHandle,
    OrderHandleStatus,
    SubscriptionHandle,
)

__all__ = [
    "BackendId",
    "BrokerAccountSnapshot",
    "BrokerBackend",
    "BrokerBackendId",
    "BrokerBindCapacity",
    "CancelAck",
    "FillEvent",
    "MarketDataChannel",
    "OrderHandle",
    "OrderHandleStatus",
    "SubscriptionHandle",
]
