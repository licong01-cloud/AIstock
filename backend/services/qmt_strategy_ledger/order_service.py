"""Managed MiniQMT order service for virtual strategies.

Phase 4 introduces the broker-calling boundary, but keeps it dependency
injected. Unit tests use fake brokers by default; real MiniQMT API exposure is
guarded at the router layer by an explicit environment switch.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from backend.execution_algos.board_lot import board_lot_rule, round_to_board_lot

from .models import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    CashEntryType,
    CashLedgerEntry,
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderIntentRecord,
    OrderLedgerRecord,
    OrderStatusEventRecord,
    VirtualAccount,
    VirtualAccountStatus,
    new_id,
)


class ManagedOrderBroker(Protocol):
    def get_positions(self) -> list[dict[str, Any]]:
        ...

    def place_order(
        self,
        *,
        stock_code: str,
        order_type: int,
        order_volume: int,
        price_type: int,
        price: float,
        strategy_name: str,
        order_remark: str,
    ) -> tuple[int, str]:
        ...

    def cancel_order(self, order_id: str) -> tuple[bool, str]:
        ...


@dataclass(frozen=True)
class ManagedOrderRequest:
    account_id: str
    strategy_name: str
    symbol: str
    side: str
    order_type: int
    quantity: int
    price_type: int
    price: Decimal
    order_remark: str
    trade_date: date
    mode: str = "SIM"
    package_id: str | None = None
    selection_run_id: str | None = None
    target_weight: Decimal | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ManagedCancelRequest:
    account_id: str
    strategy_name: str
    order_remark: str
    qmt_order_id: str
    trade_date: date
    mode: str = "SIM"


@dataclass(frozen=True)
class OrderPreflightError:
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": self.message, "context": self.context}


@dataclass(frozen=True)
class OrderPreflightResult:
    allowed: bool
    errors: tuple[OrderPreflightError, ...]
    strategy_id: str | None
    estimated_notional: Decimal
    estimated_fee: Decimal
    freeze_amount: Decimal
    available_cash: Decimal | None
    strategy_available_sell_quantity: int | None = None
    broker_can_sell: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "allowed": self.allowed,
            "errors": [error.to_dict() for error in self.errors],
            "strategy_id": self.strategy_id,
            "estimated_notional": float(self.estimated_notional),
            "estimated_fee": float(self.estimated_fee),
            "freeze_amount": float(self.freeze_amount),
            "available_cash": float(self.available_cash) if self.available_cash is not None else None,
            "strategy_available_sell_quantity": self.strategy_available_sell_quantity,
            "broker_can_sell": self.broker_can_sell,
        }


@dataclass(frozen=True)
class ManagedOrderSubmitResult:
    success: bool
    intent_id: str | None
    qmt_order_id: str | None
    broker_message: str
    preflight: OrderPreflightResult
    broker_called: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "intent_id": self.intent_id,
            "qmt_order_id": self.qmt_order_id,
            "broker_message": self.broker_message,
            "preflight": self.preflight.to_dict(),
            "broker_called": self.broker_called,
        }


@dataclass(frozen=True)
class ManagedBatchSubmitResult:
    success: bool
    total: int
    succeeded: int
    failed: int
    results: tuple[ManagedOrderSubmitResult, ...]
    compensation_required: bool
    compensation_hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "results": [result.to_dict() for result in self.results],
            "compensation_required": self.compensation_required,
            "compensation_hint": self.compensation_hint,
        }


@dataclass(frozen=True)
class ManagedCancelResult:
    success: bool
    intent_id: str | None
    qmt_order_id: str
    broker_message: str
    broker_called: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "intent_id": self.intent_id,
            "qmt_order_id": self.qmt_order_id,
            "broker_message": self.broker_message,
            "broker_called": self.broker_called,
        }


class QmtManagedOrderService:
    def __init__(self, *, repository: Any, broker: ManagedOrderBroker | None = None) -> None:
        self._repository = repository
        self._broker = broker

    def preview_order(self, request: ManagedOrderRequest) -> OrderPreflightResult:
        errors: list[OrderPreflightError] = []
        account = self._resolve_account(request, errors)
        estimated_notional = request.price * Decimal(request.quantity) if request.price > 0 else Decimal("0")
        estimated_fee = Decimal("0")
        freeze_amount = estimated_notional + estimated_fee if request.order_type == BUY_ORDER_TYPE else Decimal("0")
        available_cash = account.cash if account else None
        strategy_available_sell_quantity: int | None = None

        if not request.symbol.strip():
            errors.append(OrderPreflightError("BLANK_SYMBOL", "symbol is required"))
        if request.order_type not in {BUY_ORDER_TYPE, SELL_ORDER_TYPE}:
            errors.append(OrderPreflightError("INVALID_ORDER_TYPE", "order_type must be 23 or 24"))
        if request.side not in {"BUY", "SELL"}:
            errors.append(OrderPreflightError("INVALID_SIDE", "side must be BUY or SELL"))
        if (request.side == "BUY") != (request.order_type == BUY_ORDER_TYPE):
            errors.append(OrderPreflightError("SIDE_TYPE_MISMATCH", "side does not match MiniQMT order_type"))
        if request.quantity <= 0:
            errors.append(OrderPreflightError("INVALID_QUANTITY", "quantity must be positive"))
        if request.order_type == BUY_ORDER_TYPE and request.quantity > 0:
            board_lot_error = self._buy_board_lot_error(request)
            if board_lot_error is not None:
                errors.append(board_lot_error)
        if request.order_type == BUY_ORDER_TYPE and request.price <= 0:
            errors.append(OrderPreflightError("PRICE_REQUIRED_FOR_FREEZE", "buy order requires a positive price for cash freeze"))
        if not request.order_remark.strip():
            errors.append(OrderPreflightError("BLANK_ORDER_REMARK", "order_remark is required"))
        elif self._repository.get_order_intent_by_remark(request.account_id, request.order_remark) is not None:
            errors.append(OrderPreflightError("DUPLICATE_ORDER_REMARK", "order_remark already exists in this account"))

        if account and request.order_type == BUY_ORDER_TYPE and account.cash < freeze_amount:
            errors.append(
                OrderPreflightError(
                    "INSUFFICIENT_CASH",
                    "virtual strategy cash is insufficient",
                    {"available_cash": float(account.cash), "required_cash": float(freeze_amount)},
                )
            )
        if account and request.order_type == SELL_ORDER_TYPE:
            strategy_available_sell_quantity = sum(
                lot.available_quantity
                for lot in self._repository.list_position_lots(account.strategy_id, symbol=request.symbol)
            )
            if strategy_available_sell_quantity < request.quantity:
                errors.append(
                    OrderPreflightError(
                        "INSUFFICIENT_STRATEGY_AVAILABLE_LOT",
                        "strategy T+1 available lot quantity is insufficient",
                        {"available_quantity": strategy_available_sell_quantity, "requested_quantity": request.quantity},
                    )
                )

        return OrderPreflightResult(
            allowed=not errors,
            errors=tuple(errors),
            strategy_id=account.strategy_id if account else None,
            estimated_notional=estimated_notional,
            estimated_fee=estimated_fee,
            freeze_amount=freeze_amount,
            available_cash=available_cash,
            strategy_available_sell_quantity=strategy_available_sell_quantity,
        )

    def submit_order(self, request: ManagedOrderRequest) -> ManagedOrderSubmitResult:
        preflight = self.preview_order(request)
        if not preflight.allowed:
            return ManagedOrderSubmitResult(
                success=False,
                intent_id=None,
                qmt_order_id=None,
                broker_message="preflight failed",
                preflight=preflight,
                broker_called=False,
            )
        if self._broker is None:
            raise ValueError("broker is required for submit_order")
        account = self._account_by_strategy_name(request.account_id, request.strategy_name)

        broker_can_sell = None
        if request.order_type == SELL_ORDER_TYPE:
            broker_can_sell = self._broker_can_sell(request.symbol)
            if broker_can_sell < request.quantity:
                failed = replace(
                    preflight,
                    allowed=False,
                    broker_can_sell=broker_can_sell,
                    errors=preflight.errors
                    + (
                        OrderPreflightError(
                            "INSUFFICIENT_BROKER_CAN_SELL",
                            "MiniQMT account-level can_sell is insufficient",
                            {"broker_can_sell": broker_can_sell, "requested_quantity": request.quantity},
                        ),
                    ),
                )
                return ManagedOrderSubmitResult(False, None, None, "preflight failed", failed, False)
            preflight = replace(preflight, broker_can_sell=broker_can_sell)

        intent = self._create_intent(request, account, preflight, IntentSubmitStatus.SUBMITTED)
        freeze_applied = False
        if request.order_type == BUY_ORDER_TYPE and preflight.freeze_amount > 0:
            self._apply_cash_entry(account, request, preflight.freeze_amount, CashEntryType.FREEZE_BUY, intent.intent_id)
            freeze_applied = True

        try:
            order_id, message = self._broker.place_order(
                stock_code=request.symbol,
                order_type=request.order_type,
                order_volume=request.quantity,
                price_type=request.price_type,
                price=float(request.price),
                strategy_name=request.strategy_name,
                order_remark=request.order_remark,
            )
        except Exception as exc:  # noqa: BLE001
            if freeze_applied:
                self._release_cash_entry(account.strategy_id, request, preflight.freeze_amount, CashEntryType.UNFREEZE_REJECT, intent.intent_id)
            self._repository.set_order_intent_submit_status(intent.intent_id, IntentSubmitStatus.REJECTED, updated_at=datetime.now(UTC))
            return ManagedOrderSubmitResult(False, intent.intent_id, None, f"broker exception: {exc!r}", preflight, True)

        success = int(order_id or 0) > 0
        status = IntentSubmitStatus.ACCEPTED if success else IntentSubmitStatus.REJECTED
        self._repository.set_order_intent_submit_status(intent.intent_id, status, submitted_at=datetime.now(UTC), updated_at=datetime.now(UTC))
        if not success and freeze_applied:
            self._release_cash_entry(account.strategy_id, request, preflight.freeze_amount, CashEntryType.UNFREEZE_REJECT, intent.intent_id)
        if success:
            qmt_order_id = str(order_id)
            self._repository.upsert_order_ledger(
                OrderLedgerRecord(
                    intent_id=intent.intent_id,
                    strategy_id=account.strategy_id,
                    strategy_name=request.strategy_name,
                    qmt_order_id=qmt_order_id,
                    symbol=request.symbol,
                    order_type=request.order_type,
                    order_volume=request.quantity,
                    traded_volume=0,
                    order_status=None,
                    account_id=request.account_id,
                    trade_date=request.trade_date,
                    price_type=request.price_type,
                    price=request.price,
                    status_msg=message,
                    order_remark=request.order_remark,
                    raw_json={"source": "managed_order_submit"},
                )
            )
            self._repository.append_order_status_event(
                OrderStatusEventRecord(
                    event_id=new_id("qmtevt"),
                    intent_id=intent.intent_id,
                    qmt_order_id=qmt_order_id,
                    event_type="SUBMITTED",
                    event_time=datetime.now(UTC),
                    account_id=request.account_id,
                    status_msg=message,
                    raw_json={"source": "managed_order_submit"},
                )
            )
            return ManagedOrderSubmitResult(True, intent.intent_id, qmt_order_id, message, preflight, True)
        return ManagedOrderSubmitResult(False, intent.intent_id, None, message, preflight, True)

    def submit_batch(self, requests: list[ManagedOrderRequest]) -> ManagedBatchSubmitResult:
        results = tuple(self.submit_order(request) for request in requests)
        succeeded = sum(1 for result in results if result.success)
        failed = len(results) - succeeded
        compensation_required = succeeded > 0 and failed > 0
        return ManagedBatchSubmitResult(
            success=failed == 0,
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=compensation_required,
            compensation_hint="review accepted broker orders; no automatic cancel was issued" if compensation_required else None,
        )

    def cancel_order(self, request: ManagedCancelRequest) -> ManagedCancelResult:
        if self._broker is None:
            raise ValueError("broker is required for cancel_order")
        account = self._account_by_strategy_name(request.account_id, request.strategy_name)
        intent = self._repository.get_order_intent_by_remark(request.account_id, request.order_remark)
        if intent is None:
            return ManagedCancelResult(False, None, request.qmt_order_id, "order intent not found", False)
        if account.mode != request.mode:
            return ManagedCancelResult(False, intent.intent_id, request.qmt_order_id, "mode mismatch", False)

        success, message = self._broker.cancel_order(request.qmt_order_id)
        if success:
            release_amount = intent.estimated_notional or Decimal("0")
            if release_amount > 0:
                self._release_cash_entry(account.strategy_id, request, release_amount, CashEntryType.UNFREEZE_CANCEL, intent.intent_id)
            self._repository.set_order_intent_submit_status(intent.intent_id, IntentSubmitStatus.CANCELLED, updated_at=datetime.now(UTC))
            self._repository.append_order_status_event(
                OrderStatusEventRecord(
                    event_id=new_id("qmtevt"),
                    intent_id=intent.intent_id,
                    qmt_order_id=request.qmt_order_id,
                    event_type="CANCEL_REQUESTED",
                    event_time=datetime.now(UTC),
                    account_id=request.account_id,
                    status_msg=message,
                    raw_json={"source": "managed_order_cancel"},
                )
            )
        return ManagedCancelResult(success, intent.intent_id, request.qmt_order_id, message, True)

    def _resolve_account(self, request: ManagedOrderRequest, errors: list[OrderPreflightError]) -> VirtualAccount | None:
        if not request.account_id.strip():
            errors.append(OrderPreflightError("BLANK_ACCOUNT_ID", "account_id is required"))
            return None
        if not request.strategy_name.strip():
            errors.append(OrderPreflightError("BLANK_STRATEGY_NAME", "strategy_name is required"))
            return None
        account = self._account_by_strategy_name(request.account_id, request.strategy_name)
        if account is None:
            errors.append(OrderPreflightError("UNKNOWN_STRATEGY_NAME", "strategy_name is not registered"))
            return None
        if account.mode != request.mode:
            errors.append(OrderPreflightError("MODE_MISMATCH", "request mode does not match virtual account mode"))
        if account.status != VirtualAccountStatus.ENABLED:
            errors.append(OrderPreflightError("STRATEGY_NOT_ENABLED", "virtual strategy account is not enabled"))
        return account

    def _account_by_strategy_name(self, account_id: str, strategy_name: str) -> VirtualAccount | None:
        for account in self._repository.list_virtual_accounts(account_id=account_id):
            if account.strategy_name == strategy_name:
                return account
        return None

    def _buy_board_lot_error(self, request: ManagedOrderRequest) -> OrderPreflightError | None:
        try:
            min_quantity, increment = board_lot_rule(request.symbol)
            canonical_quantity = round_to_board_lot(request.quantity, request.symbol, side="BUY")
        except ValueError as exc:
            return OrderPreflightError(
                "BUY_BOARD_LOT",
                "buy quantity does not match the canonical A-share board-lot rule",
                {"symbol": request.symbol, "quantity": request.quantity, "reason": str(exc)},
            )

        if canonical_quantity == request.quantity:
            return None
        return OrderPreflightError(
            "BUY_BOARD_LOT",
            "buy quantity does not match the canonical A-share board-lot rule",
            {
                "symbol": request.symbol,
                "quantity": request.quantity,
                "min_quantity": min_quantity,
                "increment": increment,
                "canonical_quantity": canonical_quantity,
            },
        )

    def _create_intent(
        self,
        request: ManagedOrderRequest,
        account: VirtualAccount,
        preflight: OrderPreflightResult,
        submit_status: IntentSubmitStatus,
    ) -> OrderIntentRecord:
        return self._repository.create_order_intent(
            OrderIntentRecord(
                intent_id=new_id("qmtintent"),
                strategy_id=account.strategy_id,
                strategy_name=request.strategy_name,
                symbol=request.symbol,
                side=request.side,
                order_type=request.order_type,
                quantity=request.quantity,
                price_type=request.price_type,
                order_remark=request.order_remark,
                account_id=request.account_id,
                trade_date=request.trade_date,
                package_id=request.package_id,
                selection_run_id=request.selection_run_id,
                limit_price=request.price,
                target_weight=request.target_weight,
                estimated_notional=preflight.estimated_notional,
                estimated_fee=preflight.estimated_fee,
                preflight_status=IntentPreflightStatus.PASSED,
                submit_status=submit_status,
                metadata={"source": "managed_order_service", **request.metadata},
                submitted_at=datetime.now(UTC) if submit_status != IntentSubmitStatus.CREATED else None,
            )
        )

    def _apply_cash_entry(
        self,
        account: VirtualAccount,
        request: ManagedOrderRequest,
        amount: Decimal,
        entry_type: CashEntryType,
        intent_id: str,
    ) -> None:
        updated = replace(
            account,
            cash=account.cash - amount,
            frozen_cash=account.frozen_cash + amount,
            updated_at=datetime.now(UTC),
        )
        self._repository.update_virtual_account(updated)
        self._repository.append_cash_entry(
            CashLedgerEntry(
                cash_id=new_id("qmtcash"),
                strategy_id=account.strategy_id,
                entry_type=entry_type,
                cash_delta=-amount,
                cash_after=updated.cash,
                frozen_delta=amount,
                frozen_after=updated.frozen_cash,
                account_id=request.account_id,
                trade_date=request.trade_date,
                intent_id=intent_id,
                symbol=getattr(request, "symbol", None),
                reason=entry_type.value,
            )
        )

    def _release_cash_entry(
        self,
        strategy_id: str,
        request: ManagedOrderRequest | ManagedCancelRequest,
        amount: Decimal,
        entry_type: CashEntryType,
        intent_id: str,
    ) -> None:
        account = self._repository.get_virtual_account(strategy_id)
        release_amount = min(amount, account.frozen_cash)
        updated = replace(
            account,
            cash=account.cash + release_amount,
            frozen_cash=account.frozen_cash - release_amount,
            updated_at=datetime.now(UTC),
        )
        self._repository.update_virtual_account(updated)
        self._repository.append_cash_entry(
            CashLedgerEntry(
                cash_id=new_id("qmtcash"),
                strategy_id=strategy_id,
                entry_type=entry_type,
                cash_delta=release_amount,
                cash_after=updated.cash,
                frozen_delta=-release_amount,
                frozen_after=updated.frozen_cash,
                account_id=request.account_id,
                trade_date=request.trade_date,
                intent_id=intent_id,
                symbol=getattr(request, "symbol", None),
                reason=entry_type.value,
            )
        )

    def _broker_can_sell(self, symbol: str) -> int:
        if self._broker is None:
            return 0
        positions = self._broker.get_positions()
        for position in positions:
            position_symbol = str(position.get("stock_code") or position.get("symbol") or "").strip()
            if position_symbol == symbol:
                return int(position.get("can_sell") or position.get("can_use_volume") or 0)
        return 0


def request_from_payload(payload: dict[str, Any]) -> ManagedOrderRequest:
    order_type = int(payload.get("order_type") or 0)
    side = str(payload.get("side") or ("BUY" if order_type == BUY_ORDER_TYPE else "SELL" if order_type == SELL_ORDER_TYPE else "")).strip()
    return ManagedOrderRequest(
        account_id=str(payload.get("account_id") or "").strip(),
        strategy_name=str(payload.get("strategy_name") or "").strip(),
        symbol=str(payload.get("symbol") or payload.get("stock_code") or "").strip(),
        side=side,
        order_type=order_type,
        quantity=int(payload.get("quantity") or payload.get("order_volume") or 0),
        price_type=int(payload.get("price_type") or 0),
        price=_decimal(payload.get("price")),
        order_remark=str(payload.get("order_remark") or "").strip(),
        trade_date=_date(payload.get("trade_date")),
        mode=str(payload.get("mode") or "SIM").strip().upper(),
        package_id=payload.get("package_id"),
        selection_run_id=payload.get("selection_run_id"),
        target_weight=_optional_decimal(payload.get("target_weight")),
        metadata=dict(payload.get("metadata") or {}),
    )


def cancel_request_from_payload(payload: dict[str, Any]) -> ManagedCancelRequest:
    return ManagedCancelRequest(
        account_id=str(payload.get("account_id") or "").strip(),
        strategy_name=str(payload.get("strategy_name") or "").strip(),
        order_remark=str(payload.get("order_remark") or "").strip(),
        qmt_order_id=str(payload.get("qmt_order_id") or payload.get("order_id") or "").strip(),
        trade_date=_date(payload.get("trade_date")),
        mode=str(payload.get("mode") or "SIM").strip().upper(),
    )


def _date(value: Any) -> date:
    if value is None or str(value).strip() == "":
        return date.today()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    return _decimal(value)
