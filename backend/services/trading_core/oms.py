"""Order management state machine for Trading Core v2."""

from __future__ import annotations

from datetime import UTC, datetime

from .errors import InvalidStateTransitionError
from .models import (
    Fill,
    Order,
    OrderEvent,
    OrderEventType,
    OrderIntent,
    OrderStatus,
)


FINAL_STATUSES = {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}


class OMS:
    """Small fail-fast OMS.

    It does not mutate cash or positions. Ledger updates must consume emitted
    fills/events separately.
    """

    def create_order(self, intent: OrderIntent) -> Order:
        return Order(
            intent_id=intent.intent_id,
            package_id=intent.package_id,
            portfolio_id=intent.portfolio_id,
            symbol=intent.symbol,
            side=intent.side,
            quantity=intent.quantity,
            order_type=intent.order_type,
            limit_price=intent.limit_price,
            status=OrderStatus.SUBMITTED,
            metadata=dict(intent.metadata),
        )

    def apply_fill(self, order: Order, fill: Fill) -> tuple[Order, OrderEvent]:
        if order.status in FINAL_STATUSES:
            raise InvalidStateTransitionError(
                "cannot fill a final order",
                context={"order_id": order.order_id, "status": order.status.value},
            )
        if fill.order_id != order.order_id:
            raise InvalidStateTransitionError(
                "fill.order_id does not match order",
                context={"order_id": order.order_id, "fill_order_id": fill.order_id},
            )
        if fill.symbol != order.symbol or fill.side != order.side:
            raise InvalidStateTransitionError(
                "fill symbol or side does not match order",
                context={
                    "order_id": order.order_id,
                    "order_symbol": order.symbol,
                    "fill_symbol": fill.symbol,
                    "order_side": order.side.value,
                    "fill_side": fill.side.value,
                },
            )
        if fill.quantity > order.remaining_quantity:
            raise InvalidStateTransitionError(
                "fill quantity exceeds remaining quantity",
                context={
                    "order_id": order.order_id,
                    "remaining_quantity": order.remaining_quantity,
                    "fill_quantity": fill.quantity,
                },
            )

        new_filled = order.filled_quantity + fill.quantity
        current_notional = (order.avg_fill_price or 0.0) * order.filled_quantity
        new_notional = current_notional + fill.price * fill.quantity
        avg_price = new_notional / new_filled
        new_status = (
            OrderStatus.FILLED if new_filled == order.quantity else OrderStatus.PARTIALLY_FILLED
        )
        updated = order.model_copy(
            update={
                "filled_quantity": new_filled,
                "avg_fill_price": avg_price,
                "status": new_status,
                "updated_at": datetime.now(UTC),
            }
        )
        event = OrderEvent(
            order_id=order.order_id,
            event_type=(
                OrderEventType.FILLED
                if new_status == OrderStatus.FILLED
                else OrderEventType.PARTIALLY_FILLED
            ),
            fill=fill,
            reason=fill.reason,
        )
        return updated, event

    def cancel_order(self, order: Order, reason: str) -> tuple[Order, OrderEvent]:
        if order.status in FINAL_STATUSES:
            raise InvalidStateTransitionError(
                "cannot cancel a final order",
                context={"order_id": order.order_id, "status": order.status.value},
            )
        updated = order.model_copy(
            update={"status": OrderStatus.CANCELLED, "updated_at": datetime.now(UTC)}
        )
        event = OrderEvent(
            order_id=order.order_id,
            event_type=OrderEventType.CANCELLED,
            reason=reason,
        )
        return updated, event

    def reject_order(self, order: Order, reason: str) -> tuple[Order, OrderEvent]:
        if order.status in FINAL_STATUSES:
            raise InvalidStateTransitionError(
                "cannot reject a final order",
                context={"order_id": order.order_id, "status": order.status.value},
            )
        updated = order.model_copy(
            update={"status": OrderStatus.REJECTED, "updated_at": datetime.now(UTC)}
        )
        event = OrderEvent(
            order_id=order.order_id,
            event_type=OrderEventType.REJECTED,
            reason=reason,
        )
        return updated, event
