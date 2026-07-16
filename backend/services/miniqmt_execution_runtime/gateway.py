"""MiniQMT gateway protocols and controlled test gateway."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from backend.services.trading_core.models import OrderSide

from .models import MiniQMTChildOrder, MiniQMTExecutionEvent


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


class MiniQMTGatewayEventSink(Protocol):
    def on_tick(self, *, symbol: str, price: float, payload: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        ...

    def record_order_event(
        self,
        *,
        broker_order_id: str,
        status: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        ...

    def record_trade_event(
        self,
        *,
        broker_order_id: str,
        quantity: int,
        price: float,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        ...

    def record_account_event(self, *, payload: dict[str, Any]) -> MiniQMTExecutionEvent:
        ...

    def record_disconnect_event(
        self,
        *,
        reason: str,
        payload: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionEvent:
        ...


class MiniQMTGatewayEventSource(Protocol):
    def bind_event_sink(self, sink: MiniQMTGatewayEventSink) -> None:
        ...

    def on_order(self, raw_order: dict[str, Any]) -> MiniQMTExecutionEvent:
        ...

    def on_trade(self, raw_trade: dict[str, Any]) -> MiniQMTExecutionEvent:
        ...

    def on_tick(self, raw_tick: dict[str, Any]) -> MiniQMTExecutionEvent:
        ...

    def on_account(self, raw_account: dict[str, Any]) -> MiniQMTExecutionEvent:
        ...

    def on_disconnect(self, raw_event: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        ...


class MiniQMTGatewayEventSourceError(RuntimeError):
    """Loud gateway event-source failure with a stable reason code."""

    def __init__(self, message: str, *, reason_code: str, context: dict[str, Any] | None = None) -> None:
        self.reason_code = reason_code
        self.context = dict(context or {})
        super().__init__(f"{message}; reason_code={reason_code}; context={self.context}")


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


class QmtClientMiniQMTEventLoopGateway(QmtClientMiniQMTGateway):
    """Real MiniQMT event-loop gateway.

    This adapter is used only by the explicit ``event_loop`` runtime path. It
    converts raw broker callbacks into runtime events and refuses to turn
    missing broker query APIs into silent empty snapshots.
    """

    def __init__(
        self,
        *,
        qmt_client: Any,
        event_sink: MiniQMTGatewayEventSink | None = None,
        strategy_name: str | None = None,
        order_remark_prefix: str = "aistock-eventloop",
    ) -> None:
        super().__init__(
            qmt_client=qmt_client,
            strategy_name=strategy_name,
            order_remark_prefix=order_remark_prefix,
        )
        self._event_sink = event_sink

    def bind_event_sink(self, sink: MiniQMTGatewayEventSink) -> None:
        self._event_sink = sink

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:
        return _required_qmt_list(
            self.qmt_client,
            "get_orders",
            reason_code="MINIQMT_EVENT_LOOP_SYNC_ORDERS_UNAVAILABLE",
            kwargs={"cancelable_only": False},
            runtime_id=runtime_id,
        )

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:
        return _required_qmt_list(
            self.qmt_client,
            "get_trades",
            reason_code="MINIQMT_EVENT_LOOP_SYNC_TRADES_UNAVAILABLE",
            runtime_id=runtime_id,
        )

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:
        return _required_qmt_list(
            self.qmt_client,
            "get_positions",
            reason_code="MINIQMT_EVENT_LOOP_SYNC_POSITIONS_UNAVAILABLE",
            runtime_id=runtime_id,
        )

    def on_order(self, raw_order: dict[str, Any]) -> MiniQMTExecutionEvent:
        payload = dict(raw_order)
        broker_order_id = _payload_text(payload, "broker_order_id", "order_id", "qmt_order_id", "native_order_id")
        if not broker_order_id:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT order callback missing broker order id",
                reason_code="MINIQMT_EVENT_LOOP_ORDER_ID_MISSING",
                context={"raw_order": payload},
            )
        raw_status = payload.get("order_status")
        if raw_status is None:
            raw_status = payload.get("status") or payload.get("raw_status")
        if raw_status is None or str(raw_status).strip() == "":
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT order callback missing status",
                reason_code="MINIQMT_EVENT_LOOP_ORDER_STATUS_MISSING",
                context={"broker_order_id": broker_order_id},
            )
        payload.setdefault("broker_order_id", broker_order_id)
        payload.setdefault("reason_code", "MINIQMT_EVENT_LOOP_ORDER_CALLBACK")
        return self._require_event_sink().record_order_event(
            broker_order_id=broker_order_id,
            status=str(raw_status),
            payload=payload,
        )

    def on_trade(self, raw_trade: dict[str, Any]) -> MiniQMTExecutionEvent:
        payload = dict(raw_trade)
        broker_order_id = _payload_text(payload, "broker_order_id", "order_id", "qmt_order_id", "native_order_id")
        if not broker_order_id:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT trade callback missing broker order id",
                reason_code="MINIQMT_EVENT_LOOP_TRADE_ORDER_ID_MISSING",
                context={"raw_trade": payload},
            )
        quantity = _payload_int(payload, "traded_volume", "quantity", "volume", "filled_quantity")
        price = _payload_float(payload, "traded_price", "price", "avg_price")
        payload.setdefault("broker_order_id", broker_order_id)
        payload.setdefault("reason_code", "MINIQMT_EVENT_LOOP_TRADE_CALLBACK")
        return self._require_event_sink().record_trade_event(
            broker_order_id=broker_order_id,
            quantity=quantity,
            price=price,
            payload=payload,
        )

    def on_tick(self, raw_tick: dict[str, Any]) -> MiniQMTExecutionEvent:
        payload = dict(raw_tick)
        symbol = _payload_text(payload, "symbol", "stock_code", "instrument", "code")
        if not symbol:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT tick callback missing symbol",
                reason_code="MINIQMT_EVENT_LOOP_TICK_SYMBOL_MISSING",
                context={"raw_tick": payload},
            )
        price = _payload_float(payload, "price", "last_price", "current_price")
        payload.setdefault("symbol", symbol)
        payload.setdefault("price", price)
        payload.setdefault("reason_code", "MINIQMT_EVENT_LOOP_TICK_CALLBACK")
        return self._require_event_sink().on_tick(symbol=symbol, price=price, payload=payload)

    def on_account(self, raw_account: dict[str, Any]) -> MiniQMTExecutionEvent:
        payload = dict(raw_account)
        payload.setdefault("reason_code", "MINIQMT_EVENT_LOOP_ACCOUNT_CALLBACK")
        return self._require_event_sink().record_account_event(payload=payload)

    def on_disconnect(self, raw_event: dict[str, Any] | None = None) -> MiniQMTExecutionEvent:
        payload = dict(raw_event or {})
        reason = str(payload.get("reason") or payload.get("message") or "MiniQMT gateway disconnected")
        payload.setdefault("reason_code", "MINIQMT_GATEWAY_DISCONNECTED")
        return self._require_event_sink().record_disconnect_event(reason=reason, payload=payload)

    def _require_event_sink(self) -> MiniQMTGatewayEventSink:
        if self._event_sink is None:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT event-loop gateway has no runtime event sink",
                reason_code="MINIQMT_EVENT_LOOP_SINK_MISSING",
            )
        return self._event_sink


def _required_qmt_list(
    qmt_client: Any,
    method_name: str,
    *,
    reason_code: str,
    runtime_id: str,
    kwargs: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    getter = getattr(qmt_client, method_name, None)
    if not callable(getter):
        raise MiniQMTGatewayEventSourceError(
            "MiniQMT event-loop gateway requires broker query API",
            reason_code=reason_code,
            context={"method_name": method_name, "runtime_id": runtime_id},
        )
    try:
        raw_items = getter(**dict(kwargs or {}))
    except Exception as exc:  # noqa: BLE001
        raise MiniQMTGatewayEventSourceError(
            "MiniQMT broker query failed for event-loop gateway",
            reason_code=reason_code,
            context={"method_name": method_name, "runtime_id": runtime_id, "error": f"{type(exc).__name__}: {exc}"},
        ) from exc
    if raw_items is None:
        raise MiniQMTGatewayEventSourceError(
            "MiniQMT broker query returned no snapshot for event-loop gateway",
            reason_code=reason_code,
            context={"method_name": method_name, "runtime_id": runtime_id},
        )
    return [dict(item) for item in raw_items]


def _payload_text(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _payload_int(payload: dict[str, Any], *keys: str) -> int:
    parsed_by_key: dict[str, int] = {}
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            if isinstance(value, bool):
                raise ValueError("boolean is not an integer callback fact")
            decimal_value = Decimal(str(value))
            if not decimal_value.is_finite() or decimal_value != decimal_value.to_integral_value():
                raise ValueError("callback fact is not a finite integer")
            parsed = int(decimal_value)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT callback contains an invalid integer field",
                reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_INVALID",
                context={"field": key, "value": value, "keys": list(keys)},
            ) from exc
        if parsed <= 0:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT callback contains a non-positive integer field",
                reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_INVALID",
                context={"field": key, "value": value, "keys": list(keys)},
            )
        parsed_by_key[key] = parsed
    if parsed_by_key:
        distinct_values = set(parsed_by_key.values())
        if len(distinct_values) != 1:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT callback contains conflicting integer aliases",
                reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_CONFLICT",
                context={"keys": list(keys), "parsed_values": parsed_by_key},
            )
        return next(iter(distinct_values))
    raise MiniQMTGatewayEventSourceError(
        "MiniQMT callback missing positive integer field",
        reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_MISSING",
        context={"keys": list(keys)},
    )


def _payload_float(payload: dict[str, Any], *keys: str) -> float:
    parsed_by_key: dict[str, Decimal] = {}
    for key in keys:
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            if isinstance(value, bool):
                raise ValueError("boolean is not a numeric callback fact")
            parsed = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT callback contains an invalid numeric price field",
                reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_INVALID",
                context={"field": key, "value": value, "keys": list(keys)},
            ) from exc
        if not parsed.is_finite() or parsed <= 0:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT callback contains a non-positive or non-finite price field",
                reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_INVALID",
                context={"field": key, "value": value, "keys": list(keys)},
            )
        parsed_by_key[key] = parsed
    if parsed_by_key:
        distinct_values = set(parsed_by_key.values())
        if len(distinct_values) != 1:
            raise MiniQMTGatewayEventSourceError(
                "MiniQMT callback contains conflicting numeric price aliases",
                reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_CONFLICT",
                context={
                    "keys": list(keys),
                    "parsed_values": {key: str(value) for key, value in parsed_by_key.items()},
                },
            )
        return float(next(iter(distinct_values)))
    raise MiniQMTGatewayEventSourceError(
        "MiniQMT callback missing numeric price field",
        reason_code="MINIQMT_EVENT_LOOP_NUMERIC_FIELD_MISSING",
        context={"keys": list(keys)},
    )
