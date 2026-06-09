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


@dataclass(frozen=True)
class MiniQMTGatewayCancelAck:
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

    def cancel_child_order(self, order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck:
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
        self.cancelled_orders: list[MiniQMTChildOrder] = []
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

    def cancel_child_order(self, order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck:
        self.cancelled_orders.append(order)
        for broker_order in self._orders:
            if broker_order.get("broker_order_id") == order.broker_order_id:
                broker_order["status"] = "CANCEL_REQUESTED"
                broker_order["cancel_reason"] = reason
        return MiniQMTGatewayCancelAck(
            accepted=True,
            broker_order_id=order.broker_order_id,
            message="fake broker accepted cancel request",
            raw={"gateway": "fake_miniqmt", "cancel_reason": reason},
        )


class QmtClientMiniQMTGateway:
    """Runtime gateway adapter for the existing AIstock miniQMT client.

    The adapter keeps broker mutation behind ``MiniQMTExecutionRuntime``.  It is
    intentionally a thin duck-typed boundary so unit tests can inject a fake
    client without importing xtquant.
    """

    def __init__(
        self,
        *,
        qmt_client: Any,
        strategy_name: str | None = None,
        order_remark_prefix: str = "aistock-opcmd",
    ) -> None:
        self.qmt_client = qmt_client
        self.strategy_name = str(strategy_name or "").strip() or None
        self.order_remark_prefix = str(order_remark_prefix or "aistock-opcmd").strip() or "aistock-opcmd"
        self.connected_runtime_ids: list[str] = []
        self.submitted_orders: list[MiniQMTChildOrder] = []
        self.cancelled_orders: list[MiniQMTChildOrder] = []

    def connect(self, *, runtime_id: str) -> None:
        # Do not auto-restart or reconnect services from operator commands.
        self.connected_runtime_ids.append(runtime_id)

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        getter = getattr(self.qmt_client, "get_orders", None)
        if not callable(getter):
            return []
        return [dict(item) for item in (getter(cancelable_only=False) or [])]

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        getter = getattr(self.qmt_client, "get_trades", None)
        if not callable(getter):
            return []
        return [dict(item) for item in (getter() or [])]

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        getter = getattr(self.qmt_client, "get_positions", None)
        if not callable(getter):
            return []
        return [dict(item) for item in (getter() or [])]

    def submit_child_order(self, order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck:
        self.submitted_orders.append(order)
        submitter = getattr(self.qmt_client, "place_order", None)
        if not callable(submitter):
            return MiniQMTGatewayOrderAck(
                accepted=False,
                broker_order_id=None,
                message="qmt client does not expose place_order",
                raw={"gateway": "qmt_client_miniqmt", "error_code": "QMT_PLACE_ORDER_UNAVAILABLE"},
            )
        order_type = 23 if order.side == OrderSide.BUY else 24
        strategy_name = _metadata_text(order, "strategy_name") or self.strategy_name or order.strategy_slot_id
        order_remark = _metadata_text(order, "order_remark") or f"{self.order_remark_prefix}-{order.child_order_id[-12:]}"
        try:
            broker_order_id, message = submitter(
                stock_code=order.symbol,
                order_type=order_type,
                order_volume=int(order.quantity),
                price_type=int(order.price_type),
                price=float(order.price or 0.0),
                strategy_name=strategy_name,
                order_remark=order_remark,
            )
        except Exception as exc:  # noqa: BLE001
            return MiniQMTGatewayOrderAck(
                accepted=False,
                broker_order_id=None,
                message=f"{type(exc).__name__}: {exc}",
                raw={"gateway": "qmt_client_miniqmt", "exception_type": type(exc).__name__},
            )
        diagnostic = _maybe_call(self.qmt_client, "get_last_order_diagnostic") or {}
        try:
            parsed_order_id = int(broker_order_id)
        except (TypeError, ValueError):
            parsed_order_id = -1
        accepted = parsed_order_id > 0
        return MiniQMTGatewayOrderAck(
            accepted=accepted,
            broker_order_id=str(broker_order_id) if accepted else None,
            message=str(message or ""),
            raw={
                "gateway": "qmt_client_miniqmt",
                "diagnostic": diagnostic,
                "strategy_name": strategy_name,
                "order_remark": order_remark,
                "order_type": order_type,
            },
        )

    def cancel_child_order(self, order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck:
        self.cancelled_orders.append(order)
        order_id = order.broker_order_id or _metadata_broker_order_id(order)
        if not order_id:
            return MiniQMTGatewayCancelAck(
                accepted=False,
                broker_order_id=None,
                message="child order has no broker_order_id",
                raw={"gateway": "qmt_client_miniqmt", "reason": reason},
            )
        canceler = getattr(self.qmt_client, "cancel_order", None)
        if not callable(canceler):
            return MiniQMTGatewayCancelAck(
                accepted=False,
                broker_order_id=str(order_id),
                message="qmt client does not expose cancel_order",
                raw={"gateway": "qmt_client_miniqmt", "error_code": "QMT_CANCEL_ORDER_UNAVAILABLE"},
            )
        try:
            accepted, message = canceler(str(order_id))
        except Exception as exc:  # noqa: BLE001
            return MiniQMTGatewayCancelAck(
                accepted=False,
                broker_order_id=str(order_id),
                message=f"{type(exc).__name__}: {exc}",
                raw={"gateway": "qmt_client_miniqmt", "exception_type": type(exc).__name__, "reason": reason},
            )
        diagnostic = _maybe_call(self.qmt_client, "get_last_cancel_diagnostic") or {}
        return MiniQMTGatewayCancelAck(
            accepted=bool(accepted),
            broker_order_id=str(order_id),
            message=str(message or ""),
            raw={"gateway": "qmt_client_miniqmt", "diagnostic": diagnostic, "reason": reason},
        )


def _metadata_text(order: MiniQMTChildOrder, key: str) -> str | None:
    value = order.metadata.get(key)
    if value is None and isinstance(order.metadata.get("broker_order"), dict):
        value = order.metadata["broker_order"].get(key)
    text = str(value or "").strip()
    return text or None


def _metadata_broker_order_id(order: MiniQMTChildOrder) -> str | None:
    for key in ("order_id", "qmt_order_id", "broker_order_id"):
        value = _metadata_text(order, key)
        if value:
            return value
    return None


def _maybe_call(obj: Any, method_name: str) -> Any:
    method = getattr(obj, method_name, None)
    if not callable(method):
        return None
    try:
        return method()
    except Exception:  # noqa: BLE001
        return None
