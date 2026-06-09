"""MiniQMT gateway protocol and fake gateway used by Phase 2 validation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from backend.services.trading_core.models import OrderSide

from .models import MiniQMTChildOrder


@dataclass(frozen=True)
class MiniQMTGatewayOrderAck:
    accepted: bool
    broker_order_id: str | None
    message: str
    raw: dict[str, Any] = field(default_factory=dict)


class MiniQMTGateway(Protocol):
    def connect(self, *, runtime_id: str) -> None:
        ...

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:
        ...

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:
        ...

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:
        ...

    def submit_child_order(self, order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck:
        ...


class FakeMiniQMTGateway:
    """Controlled fake broker; no production MiniQMT process is touched."""

    def __init__(
        self,
        *,
        orders: list[dict[str, Any]] | None = None,
        trades: list[dict[str, Any]] | None = None,
        positions: list[dict[str, Any]] | None = None,
        accept_orders: bool = True,
    ) -> None:
        self.connected_runtime_ids: list[str] = []
        self.submitted_orders: list[MiniQMTChildOrder] = []
        self._orders = list(orders or [])
        self._trades = list(trades or [])
        self._positions = list(positions or [])
        self._accept_orders = accept_orders
        self._next_order_id = 1

    def connect(self, *, runtime_id: str) -> None:
        self.connected_runtime_ids.append(runtime_id)

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:
        return [dict(item, runtime_id=runtime_id) for item in self._orders]

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:
        return [dict(item, runtime_id=runtime_id) for item in self._trades]

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:
        return [dict(item, runtime_id=runtime_id) for item in self._positions]

    def submit_child_order(self, order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck:
        self.submitted_orders.append(order)
        if not self._accept_orders:
            return MiniQMTGatewayOrderAck(
                accepted=False,
                broker_order_id=None,
                message="fake broker rejected child order",
                raw={"gateway": "fake_miniqmt", "rejected": True},
            )
        broker_order_id = f"fake_qmt_{self._next_order_id:06d}"
        self._next_order_id += 1
        side_code = 23 if order.side == OrderSide.BUY else 24
        self._orders.append(
            {
                "broker_order_id": broker_order_id,
                "stock_code": order.symbol,
                "order_type": side_code,
                "order_volume": order.quantity,
                "price": order.price,
                "status": "SUBMITTED",
            }
        )
        return MiniQMTGatewayOrderAck(
            accepted=True,
            broker_order_id=broker_order_id,
            message="fake broker accepted child order",
            raw={"gateway": "fake_miniqmt", "order_type": side_code},
        )
