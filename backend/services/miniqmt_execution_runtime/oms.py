"""Durable OMS projection helpers for MiniQMTExecutionRuntime.

The event-loop runtime keeps runtime-local algo/order projections only as a
cache.  When a qmt_strategy_ledger repository is provided, broker order/trade
facts are written to that ledger and active child-order state is reconciled from
those facts instead of trusting the runtime projection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_OPEN_LIKE,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderLedgerRecord,
    OrderIntentRecord,
    TradeLedgerRecord,
    is_open_like_order_status,
    is_partial_order_status,
    is_terminal_order_status,
)
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import OrderSide

from .models import MiniQMTChildOrder, MiniQMTChildOrderStatus, MiniQMTExecutionAlgoInstance
from .repository import MiniQMTExecutionRuntimeRepository


@dataclass(frozen=True)
class MiniQMTOmsProjection:
    runtime_id: str
    active_algo_instances: tuple[MiniQMTExecutionAlgoInstance, ...]
    active_child_orders: tuple[MiniQMTChildOrder, ...]
    order_ledger_facts: tuple[OrderLedgerRecord, ...] = ()


class MiniQMTOmsLedger:
    """Read/write facade over runtime projections and qmt_strategy OMS facts."""

    def __init__(
        self,
        repository: MiniQMTExecutionRuntimeRepository,
        *,
        strategy_ledger_repository: Any | None = None,
        account_id: str | None = None,
        trade_date: date | None = None,
    ) -> None:
        self._repository = repository
        self._strategy_ledger_repository = strategy_ledger_repository
        self._account_id = str(account_id or "").strip() or None
        self._trade_date = trade_date

    @property
    def uses_qmt_strategy_authority(self) -> bool:
        return self._strategy_ledger_repository is not None

    def record_algo_instance(self, instance: MiniQMTExecutionAlgoInstance) -> MiniQMTExecutionAlgoInstance:
        return self._repository.upsert_algo_instance(instance)

    def record_child_order(self, order: MiniQMTChildOrder) -> MiniQMTChildOrder:
        stored = self._repository.upsert_child_order(order)
        self._upsert_order_ledger_fact(stored)
        return stored

    def record_trade_fill(
        self,
        child_order: MiniQMTChildOrder,
        *,
        quantity: int,
        price: float,
        payload: dict[str, Any] | None = None,
    ) -> tuple[TradeLedgerRecord | None, bool]:
        if self._strategy_ledger_repository is None:
            return None, False
        self._require_qmt_strategy_context()
        trade_id = _trade_id(payload or {})
        if not trade_id:
            raise RuntimeError(
                "MiniQMT event-loop trade callback requires broker trade id for qmt_strategy ledger idempotency; "
                f"reason_code=MINIQMT_RUNTIME_TRADE_ID_MISSING, child_order_id={child_order.child_order_id}, "
                f"broker_order_id={child_order.broker_order_id}"
            )
        qmt_order_id = _qmt_order_id_for_ledger(child_order)
        if not qmt_order_id:
            raise RuntimeError(
                "MiniQMT event-loop trade callback cannot be persisted without broker order id; "
                f"reason_code=MINIQMT_RUNTIME_TRADE_ORDER_ID_MISSING, child_order_id={child_order.child_order_id}"
            )
        trade = TradeLedgerRecord(
            trade_id=trade_id,
            intent_id=child_order.parent_intent_id,
            strategy_id=_strategy_id(child_order),
            qmt_order_id=qmt_order_id,
            qmt_order_sysid=_optional_text((payload or {}).get("qmt_order_sysid") or (payload or {}).get("order_sysid")),
            symbol=child_order.symbol,
            side=child_order.side.value,
            price=_decimal(price, field_name="trade_price"),
            quantity=max(int(quantity), 0),
            amount=_decimal(price, field_name="trade_price") * Decimal(max(int(quantity), 0)),
            trade_date=self._trade_date,
            account_id=self._account_id or "",
            commission=_decimal(
                (payload or {}).get("commission") or (payload or {}).get("fee") or 0,
                field_name="commission",
            ),
            trade_time=_trade_time(payload or {}),
            order_remark=_order_remark(child_order),
            raw_json={
                **dict(payload or {}),
                "runtime_child_order_id": child_order.child_order_id,
                "runtime_algo_instance_id": child_order.algo_instance_id,
                "runtime_parent_intent_id": child_order.parent_intent_id,
            },
        )
        return self._strategy_ledger_repository.upsert_trade_ledger(trade)

    def reconcile_child_orders_from_ledger(self, runtime_id: str) -> list[MiniQMTChildOrder]:
        if self._strategy_ledger_repository is None:
            return []
        self._require_qmt_strategy_context()
        reconciled: list[MiniQMTChildOrder] = []
        for child in self._repository.list_child_orders(runtime_id, active_only=False):
            qmt_order_id = _qmt_order_id_for_ledger(child)
            if not qmt_order_id:
                continue
            ledger_order = self._strategy_ledger_repository.get_order_ledger(self._account_id or "", qmt_order_id)
            if ledger_order is None:
                continue
            child_status = _child_status_from_order_ledger(ledger_order)
            if child.status == child_status and child.metadata.get("qmt_strategy_ledger_authority") is True:
                continue
            updated = child.model_copy(
                update={
                    "status": child_status,
                    "metadata": {
                        **dict(child.metadata),
                        "qmt_strategy_ledger_authority": True,
                        "qmt_strategy_order_ledger": _order_ledger_payload(ledger_order),
                    },
                }
            )
            reconciled.append(self._repository.upsert_child_order(updated))
        return reconciled

    def active_projection(self, runtime_id: str) -> MiniQMTOmsProjection:
        self.reconcile_child_orders_from_ledger(runtime_id)
        return MiniQMTOmsProjection(
            runtime_id=runtime_id,
            active_algo_instances=tuple(self._repository.list_algo_instances(runtime_id, active_only=True)),
            active_child_orders=tuple(self._repository.list_child_orders(runtime_id, active_only=True)),
            order_ledger_facts=tuple(self._list_order_ledger_facts()),
        )

    def _upsert_order_ledger_fact(self, order: MiniQMTChildOrder) -> None:
        if self._strategy_ledger_repository is None:
            return
        self._require_qmt_strategy_context()
        qmt_order_id = _qmt_order_id_for_ledger(order)
        if not qmt_order_id:
            return
        self._ensure_order_intent_fact(order)
        self._strategy_ledger_repository.upsert_order_ledger(
            OrderLedgerRecord(
                intent_id=order.parent_intent_id,
                strategy_id=_strategy_id(order),
                strategy_name=_strategy_name(order),
                qmt_order_id=qmt_order_id,
                qmt_order_sysid=_optional_text(order.metadata.get("qmt_order_sysid") or order.metadata.get("order_sysid")),
                symbol=order.symbol,
                order_type=BUY_ORDER_TYPE if order.side == OrderSide.BUY else SELL_ORDER_TYPE,
                order_volume=int(order.quantity),
                traded_volume=_traded_volume(order),
                order_status=_order_status(order.status),
                account_id=self._account_id or "",
                trade_date=self._trade_date,
                price_type=int(order.price_type),
                price=_decimal(order.price, field_name="order_price"),
                traded_price=_decimal(
                    order.metadata.get("traded_price") or order.metadata.get("last_trade_price") or 0,
                    field_name="traded_price",
                ),
                status_msg=_status_msg(order),
                order_remark=_order_remark(order),
                raw_json={
                    **order.model_dump(mode="json"),
                    "runtime_child_order_id": order.child_order_id,
                    "runtime_algo_instance_id": order.algo_instance_id,
                    "runtime_parent_intent_id": order.parent_intent_id,
                    "qmt_strategy_ledger_authority": True,
                },
                last_synced_at=datetime.now(UTC),
            )
        )

    def _ensure_order_intent_fact(self, order: MiniQMTChildOrder) -> None:
        getter = getattr(self._strategy_ledger_repository, "get_order_intent", None)
        creator = getattr(self._strategy_ledger_repository, "create_order_intent", None)
        account_getter = getattr(self._strategy_ledger_repository, "get_virtual_account", None)
        if not callable(getter) or not callable(creator) or not callable(account_getter):
            raise RuntimeError(
                "MiniQMT event-loop OMS requires qmt_strategy account and order_intent persistence before order_ledger writes; "
                "reason_code=MINIQMT_RUNTIME_OMS_ORDER_INTENT_REPOSITORY_MISSING"
            )
        try:
            getter(order.parent_intent_id)
            return
        except DataUnavailableError:
            intent_missing = True
        if not intent_missing:
            return
        try:
            account_getter(_strategy_id(order))
        except DataUnavailableError as exc:
            raise RuntimeError(
                "MiniQMT event-loop OMS cannot write qmt_strategy order facts without a virtual account; "
                f"reason_code=MINIQMT_RUNTIME_OMS_STRATEGY_ACCOUNT_MISSING, strategy_id={_strategy_id(order)!r}, "
                f"account_id={self._account_id!r}"
            ) from exc
        creator(
            OrderIntentRecord(
                intent_id=order.parent_intent_id,
                strategy_id=_strategy_id(order),
                strategy_name=_strategy_name(order),
                symbol=order.symbol,
                side=order.side.value,
                order_type=BUY_ORDER_TYPE if order.side == OrderSide.BUY else SELL_ORDER_TYPE,
                quantity=int(order.quantity),
                price_type=int(order.price_type),
                order_remark=_order_remark(order),
                account_id=self._account_id or "",
                trade_date=self._trade_date,
                package_id=_optional_text(order.metadata.get("package_id")),
                limit_price=_decimal(order.price, field_name="order_price"),
                preflight_status=IntentPreflightStatus.PASSED,
                submit_status=(
                    IntentSubmitStatus.REJECTED
                    if order.status == MiniQMTChildOrderStatus.REJECTED
                    else IntentSubmitStatus.SUBMITTED
                ),
                metadata={
                    "source": "miniqmt_event_loop_runtime_oms",
                    "runtime_id": order.runtime_id,
                    "runtime_child_order_id": order.child_order_id,
                    "runtime_algo_instance_id": order.algo_instance_id,
                    **dict(order.metadata),
                },
                submitted_at=order.submitted_at or datetime.now(UTC),
            )
        )

    def _list_order_ledger_facts(self) -> list[OrderLedgerRecord]:
        if self._strategy_ledger_repository is None or self._account_id is None:
            return []
        lister = getattr(self._strategy_ledger_repository, "list_order_ledger", None)
        if not callable(lister):
            raise RuntimeError(
                "MiniQMT event-loop OMS requires qmt_strategy list_order_ledger for authoritative projection; "
                "reason_code=MINIQMT_RUNTIME_OMS_ORDER_LEDGER_LIST_MISSING"
            )
        return list(lister(account_id=self._account_id, trade_date=self._trade_date))

    def _require_qmt_strategy_context(self) -> None:
        if self._account_id is None or self._trade_date is None:
            raise RuntimeError(
                "MiniQMT event-loop OMS requires account_id and trade_date before writing qmt_strategy ledger facts; "
                "reason_code=MINIQMT_RUNTIME_OMS_CONTEXT_MISSING"
            )


def _strategy_id(order: MiniQMTChildOrder) -> str:
    return _optional_text(order.metadata.get("strategy_id")) or order.strategy_slot_id


def _strategy_name(order: MiniQMTChildOrder) -> str:
    return _optional_text(order.metadata.get("strategy_name")) or order.strategy_slot_id


def _order_remark(order: MiniQMTChildOrder) -> str:
    return _optional_text(order.metadata.get("order_remark")) or order.child_order_id


def _status_msg(order: MiniQMTChildOrder) -> str:
    return _optional_text(
        order.metadata.get("status_msg")
        or order.metadata.get("gateway_message")
        or order.metadata.get("broker_status_msg")
    ) or order.status.value


def _qmt_order_id_for_ledger(order: MiniQMTChildOrder) -> str | None:
    broker_order_id = _optional_text(order.broker_order_id)
    if broker_order_id:
        return broker_order_id
    if order.status in {MiniQMTChildOrderStatus.REJECTED, MiniQMTChildOrderStatus.CANCELLED}:
        return order.child_order_id
    return None


def _order_status(status: MiniQMTChildOrderStatus) -> int:
    if status == MiniQMTChildOrderStatus.PARTIALLY_FILLED:
        return STATUS_PART_SUCC
    if status == MiniQMTChildOrderStatus.FILLED:
        return STATUS_FILLED
    if status == MiniQMTChildOrderStatus.CANCELLED:
        return STATUS_CANCELLED
    if status == MiniQMTChildOrderStatus.REJECTED:
        return STATUS_REJECTED
    return STATUS_OPEN_LIKE


def _child_status_from_order_ledger(order: OrderLedgerRecord) -> MiniQMTChildOrderStatus:
    raw_status = order.order_status
    if is_terminal_order_status(raw_status):
        if raw_status == STATUS_CANCELLED:
            return MiniQMTChildOrderStatus.CANCELLED
        if raw_status == STATUS_FILLED:
            return MiniQMTChildOrderStatus.FILLED
        if raw_status == STATUS_REJECTED:
            return MiniQMTChildOrderStatus.REJECTED
    if is_partial_order_status(raw_status) or int(order.traded_volume or 0) > 0:
        if int(order.traded_volume or 0) >= max(int(order.order_volume or 0), 1):
            return MiniQMTChildOrderStatus.FILLED
        return MiniQMTChildOrderStatus.PARTIALLY_FILLED
    if is_open_like_order_status(raw_status) or raw_status is None:
        return MiniQMTChildOrderStatus.SUBMITTED
    raise RuntimeError(
        "qmt_strategy order_ledger contains unknown MiniQMT order status; "
        f"reason_code=MINIQMT_RUNTIME_UNKNOWN_BROKER_ORDER_STATUS, qmt_order_id={order.qmt_order_id}, "
        f"order_status={raw_status!r}"
    )


def _traded_volume(order: MiniQMTChildOrder) -> int:
    for key in ("cumulative_quantity", "filled_quantity", "traded_volume", "quantity"):
        value = order.metadata.get(key)
        if value in (None, ""):
            continue
        try:
            return max(int(value), 0)
        except (TypeError, ValueError):
            continue
    trade_event = order.metadata.get("last_trade_event")
    if isinstance(trade_event, dict):
        for key in ("cumulative_quantity", "filled_quantity", "traded_volume", "quantity"):
            value = trade_event.get(key)
            if value in (None, ""):
                continue
            try:
                return max(int(value), 0)
            except (TypeError, ValueError):
                continue
    order_event = order.metadata.get("broker_order_event")
    if isinstance(order_event, dict):
        for key in ("cumulative_quantity", "filled_quantity", "traded_volume", "quantity"):
            value = order_event.get(key)
            if value in (None, ""):
                continue
            try:
                return max(int(value), 0)
            except (TypeError, ValueError):
                continue
    if order.status == MiniQMTChildOrderStatus.FILLED:
        return int(order.quantity)
    return 0


def _trade_id(payload: dict[str, Any]) -> str | None:
    for key in ("trade_id", "traded_id", "deal_id", "qmt_trade_id", "native_trade_id"):
        value = _optional_text(payload.get(key))
        if value:
            return value
    return None


def _trade_time(payload: dict[str, Any]) -> datetime | None:
    value = payload.get("trade_time") or payload.get("traded_time")
    if isinstance(value, datetime):
        return value
    return None


def _decimal(value: Any, *, field_name: str) -> Decimal:
    if value is None or value == "":
        return Decimal("0")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise RuntimeError(
            "MiniQMT event-loop OMS received invalid decimal field; "
            f"reason_code=MINIQMT_RUNTIME_OMS_DECIMAL_INVALID, field_name={field_name}, value={value!r}"
        ) from exc
    if not parsed.is_finite():
        raise RuntimeError(
            "MiniQMT event-loop OMS received non-finite decimal field; "
            f"reason_code=MINIQMT_RUNTIME_OMS_DECIMAL_INVALID, field_name={field_name}, value={value!r}"
        )
    return parsed


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _order_ledger_payload(order: OrderLedgerRecord) -> dict[str, Any]:
    return {
        "intent_id": order.intent_id,
        "strategy_id": order.strategy_id,
        "strategy_name": order.strategy_name,
        "qmt_order_id": order.qmt_order_id,
        "symbol": order.symbol,
        "order_type": order.order_type,
        "order_volume": order.order_volume,
        "traded_volume": order.traded_volume,
        "order_status": order.order_status,
        "account_id": order.account_id,
        "trade_date": order.trade_date.isoformat(),
        "status_msg": order.status_msg,
    }
