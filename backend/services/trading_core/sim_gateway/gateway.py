"""SimGateway facade over LocalSimBackend.

vnpy-style surface (``connect`` / ``send_order`` / ``cancel_order`` /
``query_status`` / ``subscribe_fill`` / ``close``) so the daemon-side runner
and the future Paper Adapter can stay decoupled from broker internals.

Synchronicity: matches LocalSim — ``send_order`` blocks until terminal status,
fill callbacks fire before return. Documented via the
``SimGatewayConnectionState`` lifecycle so tests can pin the contract.
"""

from __future__ import annotations

import threading
from enum import Enum
from typing import Callable

from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    BrokerBackend,
    CancelAck,
    FillEvent,
    LocalSimBackend,
    OrderHandle,
    OrderHandleStatus,
    SubscriptionHandle,
)
from backend.services.trading_core.errors import TradingCoreError
from backend.services.trading_core.models import OrderIntent, PositionLot


class SimGatewayConnectError(TradingCoreError):
    """Raised when ``SimGateway.connect`` is called from a non-INIT state."""

    error_code = "SIM_GATEWAY_CONNECT_ERROR"


class SimGatewayConnectionState(str, Enum):
    INIT = "INIT"
    CONNECTED = "CONNECTED"
    CLOSED = "CLOSED"


class SimGateway:
    """vnpy-style facade over a single ``BrokerBackend`` instance.

    One gateway pins one backend (which itself pins one portfolio). To run
    multiple portfolios in the same process, instantiate one SimGateway per
    portfolio.
    """

    gateway_name = "PAPER_V2_SIM"

    def __init__(self, backend: BrokerBackend) -> None:
        if not isinstance(backend, BrokerBackend):
            raise TypeError(
                f"SimGateway requires a BrokerBackend instance, got {type(backend)!r}"
            )
        self._backend = backend
        self._state = SimGatewayConnectionState.INIT
        self._lock = threading.RLock()

    # ----- read accessors -----
    @property
    def backend(self) -> BrokerBackend:
        return self._backend

    @property
    def backend_id(self) -> str:
        return self._backend.backend_id

    @property
    def state(self) -> SimGatewayConnectionState:
        return self._state

    # ----- vnpy-style lifecycle -----
    def connect(self) -> None:
        """Bring the gateway into CONNECTED state.

        For LocalSim there is no network handshake; this is a state-machine
        marker so the daemon can guard "no order before connect" / "no order
        after close" invariants. Idempotent on already-CONNECTED state is
        intentionally rejected to surface caller mistakes.
        """
        with self._lock:
            if self._state == SimGatewayConnectionState.CONNECTED:
                raise SimGatewayConnectError(
                    "SimGateway already connected; close before reconnecting",
                    context={"backend_id": self.backend_id, "state": self._state.value},
                )
            if self._state == SimGatewayConnectionState.CLOSED:
                raise SimGatewayConnectError(
                    "SimGateway has been closed; instantiate a new one",
                    context={"backend_id": self.backend_id, "state": self._state.value},
                )
            self._state = SimGatewayConnectionState.CONNECTED

    def close(self) -> None:
        """Move the gateway to CLOSED. Idempotent."""
        with self._lock:
            if self._state == SimGatewayConnectionState.CLOSED:
                return
            self._state = SimGatewayConnectionState.CLOSED
            # Local backends expose ``shutdown`` for parity; safe to call.
            shutdown = getattr(self._backend, "shutdown", None)
            if callable(shutdown):
                shutdown()

    # ----- order surface -----
    def send_order(self, intent: OrderIntent) -> OrderHandle:
        """Submit an OrderIntent. Returns a handle whose status is terminal
        on return for LocalSim (per Lead 2026-05-08 decision (4))."""
        self._require_connected()
        return self._backend.submit_order_intent(intent)

    def cancel_order(self, handle: OrderHandle) -> CancelAck:
        self._require_connected()
        return self._backend.cancel(handle)

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        self._require_connected()
        return self._backend.query_status(handle)

    def query_account(self) -> BrokerAccountSnapshot:
        self._require_connected()
        return self._backend.query_account()

    def query_positions(self) -> dict[str, PositionLot]:
        self._require_connected()
        return self._backend.query_positions()

    # ----- subscription surface -----
    def subscribe_fill(
        self, callback: Callable[[FillEvent], None]
    ) -> SubscriptionHandle:
        self._require_connected()
        return self._backend.subscribe_fill_callback(callback)

    def unsubscribe_fill(self, handle: SubscriptionHandle) -> None:
        # unsubscribe is allowed in any state -- callers may release callbacks
        # while shutting down.
        self._backend.unsubscribe_fill_callback(handle)

    # ----- internals -----
    def _require_connected(self) -> None:
        if self._state != SimGatewayConnectionState.CONNECTED:
            raise SimGatewayConnectError(
                "SimGateway is not in CONNECTED state",
                context={"backend_id": self.backend_id, "state": self._state.value},
            )

    @classmethod
    def from_local_sim(cls, backend: LocalSimBackend) -> "SimGateway":
        """Convenience constructor for the most common case."""
        return cls(backend)
