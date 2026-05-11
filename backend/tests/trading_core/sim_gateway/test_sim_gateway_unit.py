"""Unit tests for ``trading_core.sim_gateway.SimGateway``.

These tests exercise the facade in isolation using a stub BrokerBackend, so
they don't depend on LocalSim's internal market-data wiring.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Callable

import pytest

from backend.services.paper_trading_v2.broker import (
    BrokerAccountSnapshot,
    BrokerBackend,
    BrokerBindCapacity,
    CancelAck,
    FillEvent,
    MarketDataChannel,
    OrderHandle,
    OrderHandleStatus,
    SubscriptionHandle,
)
from backend.services.paper_trading_v2.broker.base import BackendId
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType, PositionLot
from backend.services.trading_core.sim_gateway import (
    SimGateway,
    SimGatewayConnectError,
    SimGatewayConnectionState,
)


class _StubBackend(BrokerBackend):
    backend_id: BackendId = "local_sim"
    backend_version = "stub-0"

    def __init__(self) -> None:
        self.submit_calls = 0
        self.cancel_calls = 0
        self.shutdown_calls = 0
        self._subscribers: dict[str, Callable[[FillEvent], None]] = {}

    def submit_order_intent(self, intent: OrderIntent) -> OrderHandle:
        self.submit_calls += 1
        return OrderHandle(
            handle_id=f"stub_{self.submit_calls}",
            backend_id=self.backend_id,
            submitted_at=datetime.now(UTC),
            intent_id=intent.intent_id,
        )

    def cancel(self, handle: OrderHandle) -> CancelAck:
        self.cancel_calls += 1
        return CancelAck(handle_id=handle.handle_id, accepted=True, reason="stub")

    def query_status(self, handle: OrderHandle) -> OrderHandleStatus:
        return OrderHandleStatus(
            handle_id=handle.handle_id,
            state="filled",
            filled_quantity=100,
            avg_fill_price=Decimal("10.00"),
            last_event_at=datetime.now(UTC),
        )

    def subscribe_fill_callback(self, cb: Callable[[FillEvent], None]) -> SubscriptionHandle:
        sub = SubscriptionHandle(subscription_id=f"sub_{len(self._subscribers)+1}", backend_id=self.backend_id)
        self._subscribers[sub.subscription_id] = cb
        return sub

    def unsubscribe_fill_callback(self, handle: SubscriptionHandle) -> None:
        self._subscribers.pop(handle.subscription_id, None)

    def query_account(self) -> BrokerAccountSnapshot:
        return BrokerAccountSnapshot(
            backend_id=self.backend_id,
            cash=Decimal("100000"),
            nav=Decimal("100000"),
            margin_used=None,
            as_of=datetime.now(UTC),
        )

    def query_positions(self) -> dict[str, PositionLot]:
        return {}

    def market_data_channel(self) -> MarketDataChannel:
        return MarketDataChannel(
            backend_id=self.backend_id,
            source=MinuteDataSource.DB_HISTORICAL,
            channel_kind="in_process_db",
        )

    def bind_capacity(self) -> BrokerBindCapacity:
        return BrokerBindCapacity(
            backend_id=self.backend_id,
            max_concurrent_packages=1,
            rejection_reason_if_exceeded="stub",
        )

    def shutdown(self) -> None:
        self.shutdown_calls += 1


def _intent() -> OrderIntent:
    from datetime import date
    return OrderIntent(
        package_id="pkg_x",
        portfolio_id="paper_x",
        symbol="600000.SH",
        side=OrderSide.BUY,
        quantity=100,
        order_type=OrderType.MARKET,
        target_trade_date=date(2024, 1, 2),
    )


def test_simgateway_rejects_non_brokerbackend() -> None:
    with pytest.raises(TypeError, match="BrokerBackend"):
        SimGateway("not a backend")  # type: ignore[arg-type]


def test_simgateway_initial_state_is_init() -> None:
    g = SimGateway(_StubBackend())
    assert g.state == SimGatewayConnectionState.INIT
    assert g.gateway_name == "PAPER_V2_SIM"
    assert g.backend_id == "local_sim"


def test_simgateway_send_order_requires_connected() -> None:
    g = SimGateway(_StubBackend())
    with pytest.raises(SimGatewayConnectError):
        g.send_order(_intent())


def test_simgateway_close_invokes_backend_shutdown() -> None:
    backend = _StubBackend()
    g = SimGateway(backend)
    g.connect()
    g.close()
    assert backend.shutdown_calls == 1


def test_simgateway_unsubscribe_works_after_close() -> None:
    """unsubscribe is allowed in any state -- helps cleanup paths."""
    backend = _StubBackend()
    g = SimGateway(backend)
    g.connect()
    sub = g.subscribe_fill(lambda e: None)
    g.close()
    # Should not raise even though we're CLOSED.
    g.unsubscribe_fill(sub)


def test_simgateway_full_happy_path() -> None:
    backend = _StubBackend()
    g = SimGateway(backend)
    g.connect()
    handle = g.send_order(_intent())
    assert backend.submit_calls == 1
    status = g.query_status(handle)
    assert status.state == "filled"
    ack = g.cancel_order(handle)
    assert ack.accepted is True
    assert backend.cancel_calls == 1
    g.close()
