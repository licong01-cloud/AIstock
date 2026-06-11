"""Managed MiniQMT order service for virtual strategies.

Phase 4 introduces the broker-calling boundary, but keeps it dependency
injected. Unit tests use fake brokers by default; real MiniQMT API exposure is
guarded at the router layer by an explicit environment switch.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime
from decimal import ROUND_CEILING, Decimal, InvalidOperation
from hashlib import sha1
from typing import Any, Callable, Protocol

from backend.execution_algos.board_lot import board_lot_rule, round_to_board_lot

from .lot_availability import (
    DbTradingCalendarProvider,
    TradingCalendarProvider,
    effective_strategy_available_sell_quantity,
)
from .models import (
    BUY_ORDER_TYPE,
    MINIQMT_ACCOUNT_GROUP_ALLOCATION_MODE,
    MINIQMT_ACCOUNT_GROUP_METADATA_KEY,
    SELL_ORDER_TYPE,
    CashEntryType,
    CashLedgerEntry,
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderBatchRecord,
    OrderBatchStatus,
    OrderIntentRecord,
    OrderLedgerRecord,
    OrderStatusEventRecord,
    VirtualAccount,
    VirtualAccountStatus,
    new_id,
)

_DEPENDENT_BUY_PROCEEDS_ERROR_CODES = frozenset(
    {
        "SELL_PROCEEDS_REQUIRED",
        "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED",
    }
)
_CASH_SHRINK_REASON_KEY = "miniqmt_cash_preflight_shrink_reason"


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
    pending_sell_quantity: int | None = None
    broker_can_sell: int | None = None

    @property
    def primary_error(self) -> OrderPreflightError | None:
        """Stable operator-facing blocker; full errors remain for diagnostics."""

        return self.errors[0] if self.errors else None

    def to_dict(self) -> dict[str, Any]:
        primary_error = self.primary_error
        return {
            "allowed": self.allowed,
            "primary_error": primary_error.to_dict() if primary_error is not None else None,
            "primary_error_code": primary_error.code if primary_error is not None else None,
            "errors": [error.to_dict() for error in self.errors],
            "strategy_id": self.strategy_id,
            "estimated_notional": float(self.estimated_notional),
            "estimated_fee": float(self.estimated_fee),
            "freeze_amount": float(self.freeze_amount),
            "available_cash": float(self.available_cash) if self.available_cash is not None else None,
            "strategy_available_sell_quantity": self.strategy_available_sell_quantity,
            "pending_sell_quantity": self.pending_sell_quantity,
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
    batch_id: str | None = None
    batch_status: str | None = None
    preflight_passed: bool = True
    retry_of_batch_id: str | None = None
    compensation_actions: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "batch_id": self.batch_id,
            "batch_status": self.batch_status,
            "preflight_passed": self.preflight_passed,
            "retry_of_batch_id": self.retry_of_batch_id,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "results": [result.to_dict() for result in self.results],
            "compensation_required": self.compensation_required,
            "compensation_hint": self.compensation_hint,
            "compensation_actions": list(self.compensation_actions),
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
    def __init__(
        self,
        *,
        repository: Any,
        broker: ManagedOrderBroker | None = None,
        calendar_provider: TradingCalendarProvider | None = None,
    ) -> None:
        self._repository = repository
        self._broker = broker
        self._calendar_provider = calendar_provider or DbTradingCalendarProvider()

    def preview_order(self, request: ManagedOrderRequest) -> OrderPreflightResult:
        errors: list[OrderPreflightError] = []
        account = self._resolve_account(request, errors)
        estimated_notional = request.price * Decimal(request.quantity) if request.price > 0 else Decimal("0")
        estimated_fee = Decimal("0")
        freeze_amount = estimated_notional + estimated_fee if request.order_type == BUY_ORDER_TYPE else Decimal("0")
        available_cash = account.cash if account else None
        strategy_available_sell_quantity: int | None = None
        pending_sell_quantity_value: int | None = None

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
            lots = self._repository.list_position_lots(account.strategy_id, symbol=request.symbol)
            pending_intents = self._repository.list_open_sell_intents(
                account.strategy_id,
                symbol=request.symbol,
                trade_date=request.trade_date,
            )
            pending_sell_quantity_value = sum(max(int(intent.quantity), 0) for intent in pending_intents)
            strategy_available_sell_quantity = effective_strategy_available_sell_quantity(
                lots=lots,
                pending_sell_intents=pending_intents,
                as_of_date=request.trade_date,
                calendar=self._calendar_provider,
            )
            if strategy_available_sell_quantity < request.quantity:
                errors.append(
                    OrderPreflightError(
                        "INSUFFICIENT_STRATEGY_AVAILABLE_LOT",
                        "strategy T+1 available lot quantity is insufficient",
                        {
                            "available_quantity": strategy_available_sell_quantity,
                            "pending_sell_quantity": pending_sell_quantity_value,
                            "requested_quantity": request.quantity,
                        },
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
            pending_sell_quantity=pending_sell_quantity_value,
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
        requests = _batch_submission_order(requests)
        requests = _shrink_near_cash_overshoot_requests(requests, preview_order=self.preview_order)
        batch_id = _batch_id_for_requests(requests)
        deferred_batch = self._existing_dependent_buy_batch(batch_id)
        if deferred_batch is not None:
            return self._retry_dependent_buy_batch(batch_id, requests, deferred_batch)
        deferred_batch = self._find_dependent_buy_batch_by_logical_key(requests)
        if deferred_batch is not None:
            return self._retry_dependent_buy_batch(deferred_batch.batch_id, _requests_from_batch(deferred_batch), deferred_batch)
        retry_result = self._existing_batch_result(batch_id, len(requests))
        if retry_result is not None:
            return retry_result

        preflight_results = tuple(self._batch_preflight(requests))
        hard_preflight_failed = any(
            not preflight.allowed and not _is_dependent_buy_proceeds_deferred(request, preflight)
            for request, preflight in zip(requests, preflight_results, strict=True)
        )
        if hard_preflight_failed:
            results = tuple(
                ManagedOrderSubmitResult(False, None, None, "batch preflight failed", preflight, False)
                for preflight in preflight_results
            )
            failed = len(results)
            self._upsert_batch_record(
                batch_id=batch_id,
                requests=requests,
                status=OrderBatchStatus.PREFLIGHT_FAILED,
                results=results,
                metadata={"reason": "full_batch_preflight_failed"},
                completed=True,
            )
            return ManagedBatchSubmitResult(
                success=False,
                batch_id=batch_id,
                batch_status=OrderBatchStatus.PREFLIGHT_FAILED.value,
                preflight_passed=False,
                total=len(results),
                succeeded=0,
                failed=failed,
                results=results,
                compensation_required=False,
                compensation_hint=None,
            )

        self._upsert_batch_record(
            batch_id=batch_id,
            requests=requests,
            status=OrderBatchStatus.SUBMITTING,
            results=(),
            metadata={"preflight_passed": True},
        )

        results_list: list[ManagedOrderSubmitResult] = []
        for request, preflight in zip(requests, preflight_results, strict=True):
            if not preflight.allowed and _is_dependent_buy_proceeds_deferred(request, preflight):
                results_list.append(
                    ManagedOrderSubmitResult(
                        False,
                        None,
                        None,
                        "dependent buy deferred until same-batch sell proceeds are reconciled",
                        preflight,
                        False,
                    )
                )
                continue
            results_list.append(self._submit_preflighted_order(request, preflight, batch_id=batch_id))
        results = tuple(results_list)
        succeeded = sum(1 for result in results if result.success)
        failed = len(results) - succeeded
        status = OrderBatchStatus.SUCCEEDED if failed == 0 else OrderBatchStatus.PARTIAL if succeeded > 0 else OrderBatchStatus.FAILED
        deferred_failed = sum(
            1
            for request, result in zip(requests, results, strict=True)
            if not result.success and _is_dependent_buy_proceeds_deferred(request, result.preflight)
        )
        compensation_required = status == OrderBatchStatus.PARTIAL and failed != deferred_failed
        compensation_actions = tuple(_compensation_actions(results)) if compensation_required else ()
        compensation_hint = (
            "partial broker submission: review accepted qmt_order_id values and call managed cancel for compensation if needed"
            if compensation_required
            else None
        )
        self._upsert_batch_record(
            batch_id=batch_id,
            requests=requests,
            status=status,
            results=results,
            metadata={
                "preflight_passed": deferred_failed == 0,
                "dependent_buy_deferred": deferred_failed > 0,
                "dependent_buy_count": deferred_failed,
                "compensation_required": compensation_required,
                "compensation_actions": list(compensation_actions),
            },
            completed=True,
        )
        return ManagedBatchSubmitResult(
            success=failed == 0,
            batch_id=batch_id,
            batch_status=status.value,
            preflight_passed=deferred_failed == 0,
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=compensation_required,
            compensation_hint=compensation_hint,
            compensation_actions=compensation_actions,
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
        *,
        batch_id: str | None = None,
    ) -> OrderIntentRecord:
        return self._repository.create_order_intent(
            OrderIntentRecord(
                intent_id=new_id("qmtintent"),
                batch_id=batch_id,
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

    def _batch_preflight(self, requests: list[ManagedOrderRequest]) -> list[OrderPreflightResult]:
        base_results = [self.preview_order(request) for request in requests]
        errors_by_index: list[list[OrderPreflightError]] = [list(result.errors) for result in base_results]
        seen_remarks: dict[tuple[str, str], int] = {}
        buy_freeze_by_account_strategy: dict[tuple[str, str], Decimal] = {}
        buy_freeze_by_account_group: dict[tuple[str, str], Decimal] = {}
        sell_proceeds_by_account_strategy: dict[tuple[str, str], Decimal] = {}
        sell_proceeds_by_account_group: dict[tuple[str, str], Decimal] = {}
        sell_requests_by_account_strategy: dict[tuple[str, str], list[ManagedOrderRequest]] = {}
        sell_requests_by_account_group: dict[tuple[str, str], list[ManagedOrderRequest]] = {}
        sell_quantity_by_account_strategy_symbol: dict[tuple[str, str, str], int] = {}
        broker_sell_quantity_by_account_symbol: dict[tuple[str, str], int] = {}

        for index, request in enumerate(requests):
            remark_key = (request.account_id, request.order_remark)
            if request.order_remark:
                previous = seen_remarks.get(remark_key)
                if previous is not None:
                    error = OrderPreflightError(
                        "BATCH_DUPLICATE_ORDER_REMARK",
                        "order_remark is duplicated inside this batch",
                        {"order_remark": request.order_remark, "first_index": previous, "duplicate_index": index},
                    )
                    errors_by_index[previous].append(error)
                    errors_by_index[index].append(error)
                else:
                    seen_remarks[remark_key] = index

            if request.order_type == BUY_ORDER_TYPE and base_results[index].strategy_id:
                key = (request.account_id, request.strategy_name)
                buy_freeze_by_account_strategy[key] = buy_freeze_by_account_strategy.get(key, Decimal("0")) + base_results[index].freeze_amount
                account = self._account_by_strategy_name(request.account_id, request.strategy_name)
                group_limit = _account_group_cash_limit(account) if account is not None else None
                if group_limit is not None:
                    group_key, cash_limit, context = group_limit
                    buy_freeze_by_account_group[group_key] = (
                        buy_freeze_by_account_group.get(group_key, Decimal("0")) + base_results[index].freeze_amount
                    )
            if request.order_type == SELL_ORDER_TYPE:
                key = (request.account_id, request.strategy_name, request.symbol)
                sell_quantity_by_account_strategy_symbol[key] = (
                    sell_quantity_by_account_strategy_symbol.get(key, 0) + max(int(request.quantity), 0)
                )
                strategy_key = (request.account_id, request.strategy_name)
                sell_proceeds = max(base_results[index].estimated_notional, Decimal("0"))
                sell_proceeds_by_account_strategy[strategy_key] = (
                    sell_proceeds_by_account_strategy.get(strategy_key, Decimal("0")) + sell_proceeds
                )
                sell_requests_by_account_strategy.setdefault(strategy_key, []).append(request)
                account = self._account_by_strategy_name(request.account_id, request.strategy_name)
                group_limit = _account_group_cash_limit(account) if account is not None else None
                if group_limit is not None:
                    group_key, _cash_limit, _context = group_limit
                    sell_proceeds_by_account_group[group_key] = (
                        sell_proceeds_by_account_group.get(group_key, Decimal("0")) + sell_proceeds
                    )
                    sell_requests_by_account_group.setdefault(group_key, []).append(request)
                broker_key = (request.account_id, request.symbol)
                broker_sell_quantity_by_account_symbol[broker_key] = (
                    broker_sell_quantity_by_account_symbol.get(broker_key, 0) + max(int(request.quantity), 0)
                )

        cumulative_buy_freeze_by_account_strategy: dict[tuple[str, str], Decimal] = {}
        cumulative_buy_freeze_by_account_group: dict[tuple[str, str], Decimal] = {}
        for index, request in enumerate(requests):
            result = base_results[index]
            if request.order_type == BUY_ORDER_TYPE and result.available_cash is not None and result.strategy_id:
                strategy_key = (request.account_id, request.strategy_name)
                total_freeze = buy_freeze_by_account_strategy[strategy_key]
                same_batch_sell_proceeds = sell_proceeds_by_account_strategy.get(strategy_key, Decimal("0"))
                effective_cash = result.available_cash + same_batch_sell_proceeds
                cumulative_freeze = (
                    cumulative_buy_freeze_by_account_strategy.get(strategy_key, Decimal("0")) + result.freeze_amount
                )
                cumulative_buy_freeze_by_account_strategy[strategy_key] = cumulative_freeze
                if total_freeze <= effective_cash:
                    if cumulative_freeze > result.available_cash:
                        errors_by_index[index] = _without_preflight_error_codes(
                            errors_by_index[index],
                            {"INSUFFICIENT_CASH", "BATCH_INSUFFICIENT_CASH"},
                        )
                        errors_by_index[index].append(
                            _sell_proceeds_required_error(
                                request=request,
                                result=result,
                                sell_requests=sell_requests_by_account_strategy.get(strategy_key, []),
                                same_batch_sell_proceeds=same_batch_sell_proceeds,
                                effective_cash=effective_cash,
                                batch_required_cash=total_freeze,
                                cumulative_required_cash=cumulative_freeze,
                            )
                        )
                    else:
                        errors_by_index[index] = _without_preflight_error_codes(
                            errors_by_index[index],
                            {"INSUFFICIENT_CASH"},
                        )
                else:
                    errors_by_index[index].append(
                        OrderPreflightError(
                            "BATCH_INSUFFICIENT_CASH",
                            "batch aggregate buy freeze exceeds virtual strategy cash plus same-batch sell proceeds",
                            {
                                "available_cash": float(result.available_cash),
                                "same_batch_estimated_sell_proceeds": float(same_batch_sell_proceeds),
                                "effective_cash": float(effective_cash),
                                "batch_required_cash": float(total_freeze),
                            },
                        )
                    )
                account = self._account_by_strategy_name(request.account_id, request.strategy_name)
                group_limit = _account_group_cash_limit(account) if account is not None else None
                if group_limit is not None:
                    group_key, cash_limit, context = group_limit
                    group_total_freeze = buy_freeze_by_account_group.get(group_key, Decimal("0"))
                    group_sell_proceeds = sell_proceeds_by_account_group.get(group_key, Decimal("0"))
                    group_effective_cash_limit = cash_limit + group_sell_proceeds
                    group_cumulative_freeze = (
                        cumulative_buy_freeze_by_account_group.get(group_key, Decimal("0")) + result.freeze_amount
                    )
                    cumulative_buy_freeze_by_account_group[group_key] = group_cumulative_freeze
                    if group_total_freeze > group_effective_cash_limit:
                        errors_by_index[index].append(
                            OrderPreflightError(
                                "BATCH_INSUFFICIENT_ACCOUNT_GROUP_CASH",
                                "batch aggregate buy freeze exceeds MiniQMT account-group cash limit plus same-batch sell proceeds",
                                {
                                    **context,
                                    "account_group_cash_limit": float(cash_limit),
                                    "same_batch_estimated_sell_proceeds": float(group_sell_proceeds),
                                    "effective_account_group_cash_limit": float(group_effective_cash_limit),
                                    "batch_required_cash": float(group_total_freeze),
                                },
                            )
                        )
                    elif group_cumulative_freeze > cash_limit:
                        errors_by_index[index] = _without_preflight_error_codes(
                            errors_by_index[index],
                            {"BATCH_INSUFFICIENT_ACCOUNT_GROUP_CASH"},
                        )
                        errors_by_index[index].append(
                            _account_group_sell_proceeds_required_error(
                                request=request,
                                result=result,
                                sell_requests=sell_requests_by_account_group.get(group_key, []),
                                group_context=context,
                                account_group_cash_limit=cash_limit,
                                same_batch_sell_proceeds=group_sell_proceeds,
                                effective_account_group_cash_limit=group_effective_cash_limit,
                                batch_required_cash=group_total_freeze,
                                cumulative_required_cash=group_cumulative_freeze,
                            )
                        )
            if request.order_type == SELL_ORDER_TYPE and result.strategy_available_sell_quantity is not None:
                total_sell = sell_quantity_by_account_strategy_symbol[(request.account_id, request.strategy_name, request.symbol)]
                if total_sell > result.strategy_available_sell_quantity:
                    errors_by_index[index].append(
                        OrderPreflightError(
                            "BATCH_INSUFFICIENT_STRATEGY_AVAILABLE_LOT",
                            "batch aggregate sell quantity exceeds strategy available lot",
                            {
                                "available_quantity": result.strategy_available_sell_quantity,
                                "batch_requested_quantity": total_sell,
                                "symbol": request.symbol,
                            },
                        )
                    )
                broker_key = (request.account_id, request.symbol)
                broker_requested_sell = broker_sell_quantity_by_account_symbol[broker_key]
                broker_can_sell = self._broker_can_sell(request.symbol)
                if broker_can_sell < broker_requested_sell:
                    errors_by_index[index].append(
                        OrderPreflightError(
                            "BATCH_INSUFFICIENT_BROKER_CAN_SELL",
                            "batch aggregate sell quantity exceeds MiniQMT account-level can_sell",
                            {
                                "broker_can_sell": broker_can_sell,
                                "batch_requested_quantity": broker_requested_sell,
                                "symbol": request.symbol,
                            },
                        )
                    )
                    result = replace(result, broker_can_sell=broker_can_sell)
                    base_results[index] = result

        return [
            replace(result, allowed=not errors_by_index[index], errors=tuple(errors_by_index[index]))
            for index, result in enumerate(base_results)
        ]

    def _submit_preflighted_order(
        self,
        request: ManagedOrderRequest,
        preflight: OrderPreflightResult,
        *,
        batch_id: str | None = None,
    ) -> ManagedOrderSubmitResult:
        if not preflight.allowed:
            return ManagedOrderSubmitResult(False, None, None, "batch preflight failed", preflight, False)
        if self._broker is None:
            raise ValueError("broker is required for submit_order")
        account = self._account_by_strategy_name(request.account_id, request.strategy_name)
        intent = self._create_intent(request, account, preflight, IntentSubmitStatus.SUBMITTED, batch_id=batch_id)
        freeze_applied = False
        applied_freeze_amount = Decimal("0")
        if request.order_type == BUY_ORDER_TYPE and preflight.freeze_amount > 0:
            # Batch preflight may allow a rebalance BUY using same-batch SELL proceeds.
            applied_freeze_amount = min(preflight.freeze_amount, account.cash)
            if applied_freeze_amount > 0:
                self._apply_cash_entry(account, request, applied_freeze_amount, CashEntryType.FREEZE_BUY, intent.intent_id)
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
                self._release_cash_entry(
                    account.strategy_id,
                    request,
                    applied_freeze_amount,
                    CashEntryType.UNFREEZE_REJECT,
                    intent.intent_id,
                )
            self._repository.set_order_intent_submit_status(intent.intent_id, IntentSubmitStatus.REJECTED, updated_at=datetime.now(UTC))
            return ManagedOrderSubmitResult(False, intent.intent_id, None, f"broker exception: {exc!r}", preflight, True)

        success = int(order_id or 0) > 0
        status = IntentSubmitStatus.ACCEPTED if success else IntentSubmitStatus.REJECTED
        self._repository.set_order_intent_submit_status(intent.intent_id, status, submitted_at=datetime.now(UTC), updated_at=datetime.now(UTC))
        if not success and freeze_applied:
            self._release_cash_entry(
                account.strategy_id,
                request,
                applied_freeze_amount,
                CashEntryType.UNFREEZE_REJECT,
                intent.intent_id,
            )
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
                    raw_json={"source": "managed_order_submit", "batch_id": batch_id},
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
                    raw_json={"source": "managed_order_submit", "batch_id": batch_id},
                )
            )
            return ManagedOrderSubmitResult(True, intent.intent_id, qmt_order_id, message, preflight, True)
        return ManagedOrderSubmitResult(False, intent.intent_id, None, message, preflight, True)

    def _existing_dependent_buy_batch(self, batch_id: str) -> OrderBatchRecord | None:
        get_batch = getattr(self._repository, "get_order_batch", None)
        if get_batch is None:
            return None
        batch = get_batch(batch_id)
        if batch is None or batch.batch_status != OrderBatchStatus.PARTIAL:
            return None
        if not isinstance(batch.metadata, dict) or not batch.metadata.get("dependent_buy_deferred"):
            return None
        return batch

    def _find_dependent_buy_batch_by_logical_key(
        self,
        requests: list[ManagedOrderRequest],
    ) -> OrderBatchRecord | None:
        get_batch = getattr(self._repository, "get_order_batch", None)
        if get_batch is None:
            return None
        logical_batch_id = _logical_batch_id_for_requests(requests)
        if logical_batch_id == _batch_id_for_requests(requests):
            return None
        for request, remark in ((request, request.order_remark) for request in requests if request.order_remark):
            intent = self._repository.get_order_intent_by_remark(request.account_id, remark)
            if intent is None or not intent.batch_id:
                continue
            batch = get_batch(intent.batch_id)
            if batch is None:
                continue
            if _logical_batch_id_for_batch(batch) != logical_batch_id:
                continue
            existing = self._existing_dependent_buy_batch(batch.batch_id)
            if existing is not None:
                return existing
        return None

    def _retry_dependent_buy_batch(
        self,
        batch_id: str,
        requests: list[ManagedOrderRequest],
        batch: OrderBatchRecord,
    ) -> ManagedBatchSubmitResult:
        stored_results = tuple(
            _result_from_dict(item)
            for item in (batch.result_json or {}).get("results", ())
            if isinstance(item, dict)
        )
        if len(stored_results) != len(requests):
            retry_result = self._existing_batch_result(batch_id, len(requests))
            if retry_result is not None:
                return retry_result

        results_list = list(stored_results)
        deferred_indexes = [
            index
            for index, (request, result) in enumerate(zip(requests, stored_results, strict=True))
            if _is_dependent_buy_retry_candidate(request, result)
        ]
        deferred_requests = [requests[index] for index in deferred_indexes]
        deferred_preflights = self._batch_preflight(deferred_requests)
        pending_deferred_indexes: set[int] = set()
        retry_preflight_passed = True
        for relative_index, preflight in enumerate(deferred_preflights):
            index = deferred_indexes[relative_index]
            request = requests[index]
            if preflight.allowed:
                results_list[index] = self._submit_preflighted_order(request, preflight, batch_id=batch_id)
                continue
            retry_preflight_passed = False
            if not _is_retry_waiting_for_cash(preflight):
                results_list[index] = ManagedOrderSubmitResult(
                    False,
                    None,
                    None,
                    "dependent buy retry preflight failed",
                    preflight,
                    False,
                )
                continue
            pending_deferred_indexes.add(index)
            results_list[index] = ManagedOrderSubmitResult(
                False,
                None,
                None,
                "dependent buy still waiting for reconciled sell proceeds",
                stored_results[index].preflight,
                False,
            )

        results = tuple(results_list)
        succeeded = sum(1 for result in results if result.success)
        failed = len(results) - succeeded
        status = OrderBatchStatus.SUCCEEDED if failed == 0 else OrderBatchStatus.PARTIAL if succeeded > 0 else OrderBatchStatus.FAILED
        still_deferred = bool(pending_deferred_indexes)
        self._upsert_batch_record(
            batch_id=batch_id,
            requests=requests,
            status=status,
            results=results,
            metadata={
                **(batch.metadata if isinstance(batch.metadata, dict) else {}),
                "preflight_passed": retry_preflight_passed and not still_deferred,
                "dependent_buy_deferred": still_deferred,
                "dependent_buy_count": len(pending_deferred_indexes),
                "dependent_buy_retry": True,
                "compensation_required": False,
                "compensation_actions": [],
            },
            completed=True,
        )
        return ManagedBatchSubmitResult(
            success=failed == 0,
            batch_id=batch_id,
            batch_status=status.value,
            preflight_passed=retry_preflight_passed and not still_deferred,
            retry_of_batch_id=batch_id,
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=False,
            compensation_hint=None,
            compensation_actions=(),
        )

    def _existing_batch_result(self, batch_id: str, request_count: int) -> ManagedBatchSubmitResult | None:
        get_batch = getattr(self._repository, "get_order_batch", None)
        list_intents = getattr(self._repository, "list_order_intents_by_batch", None)
        if get_batch is None or list_intents is None:
            return None
        batch = get_batch(batch_id)
        if batch is None:
            return None
        if batch.batch_status == OrderBatchStatus.PREFLIGHT_FAILED:
            return None
        intents = list_intents(batch_id)
        stored_results = tuple(
            _result_from_dict(item)
            for item in (batch.result_json or {}).get("results", ())
            if isinstance(item, dict)
        )
        results = stored_results or tuple(_result_from_existing_intent(intent) for intent in intents)
        succeeded = sum(1 for result in results if result.success)
        failed = (
            request_count
            if batch.batch_status == OrderBatchStatus.PREFLIGHT_FAILED and not results
            else sum(1 for result in results if not result.success) + max(request_count - len(results), 0)
        )
        compensation_required = batch.batch_status == OrderBatchStatus.PARTIAL
        if isinstance(batch.metadata, dict):
            compensation_required = bool(batch.metadata.get("compensation_required", compensation_required))
        preflight_passed = batch.batch_status != OrderBatchStatus.PREFLIGHT_FAILED
        if isinstance(batch.metadata, dict):
            preflight_passed = bool(batch.metadata.get("preflight_passed", preflight_passed))
        return ManagedBatchSubmitResult(
            success=batch.batch_status == OrderBatchStatus.SUCCEEDED,
            batch_id=batch_id,
            batch_status=batch.batch_status.value,
            preflight_passed=preflight_passed,
            retry_of_batch_id=batch_id,
            total=request_count,
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=compensation_required,
            compensation_hint=batch.result_json.get("compensation_hint") if isinstance(batch.result_json, dict) else None,
            compensation_actions=tuple((batch.result_json or {}).get("compensation_actions") or ()),
        )

    def _upsert_batch_record(
        self,
        *,
        batch_id: str,
        requests: list[ManagedOrderRequest],
        status: OrderBatchStatus,
        results: tuple[ManagedOrderSubmitResult, ...],
        metadata: dict[str, Any],
        completed: bool = False,
    ) -> None:
        upsert = getattr(self._repository, "upsert_order_batch", None)
        if upsert is None:
            return
        request_json = {"orders": [_request_signature(request) for request in requests]}
        result_json = {
            "results": [result.to_dict() for result in results],
            "compensation_actions": list(metadata.get("compensation_actions") or ()),
            "compensation_hint": (
                "partial broker submission: review accepted qmt_order_id values and call managed cancel for compensation if needed"
                if metadata.get("compensation_required")
                else None
            ),
        }
        strategy_ids = sorted({result.preflight.strategy_id for result in results if result.preflight.strategy_id})
        account_id = requests[0].account_id if requests else ""
        mode = requests[0].mode if requests else "SIM"
        now = datetime.now(UTC)
        existing = getattr(self._repository, "get_order_batch", lambda _batch_id: None)(batch_id)
        created_at = existing.created_at if existing is not None else now
        upsert(
            OrderBatchRecord(
                batch_id=batch_id,
                strategy_id=strategy_ids[0] if len(strategy_ids) == 1 else None,
                account_id=account_id,
                mode=mode,
                batch_status=status,
                request_json=request_json,
                result_json=result_json,
                metadata=metadata,
                created_at=created_at,
                submitted_at=now if status in {OrderBatchStatus.SUBMITTING, OrderBatchStatus.SUCCEEDED, OrderBatchStatus.PARTIAL, OrderBatchStatus.FAILED} else None,
                completed_at=now if completed else None,
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


def _account_group_cash_limit(account: VirtualAccount) -> tuple[tuple[str, str], Decimal, dict[str, Any]] | None:
    metadata = account.metadata or {}
    if metadata.get("allocation_mode") != MINIQMT_ACCOUNT_GROUP_ALLOCATION_MODE:
        return None
    group_meta = metadata.get(MINIQMT_ACCOUNT_GROUP_METADATA_KEY)
    if not isinstance(group_meta, dict):
        return None
    raw_cash_limit = group_meta.get("cash_limit")
    if raw_cash_limit in (None, ""):
        return None
    try:
        cash_limit = Decimal(str(raw_cash_limit))
    except (InvalidOperation, ValueError):
        return None
    account_group_id = str(group_meta.get("account_group_id") or "").strip()
    if not account_group_id:
        return None
    context = {
        "account_id": account.account_id,
        "account_group_id": account_group_id,
        "broker_backend": group_meta.get("broker_backend"),
        "broker_mode": group_meta.get("broker_mode"),
    }
    return (account.account_id, account_group_id), cash_limit, context


def _batch_id_for_requests(requests: list[ManagedOrderRequest]) -> str:
    signatures = [_request_signature(request) for request in requests]
    payload = json.dumps(signatures, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"qmtbatch_{sha1(payload.encode('utf-8')).hexdigest()[:24]}"


def _logical_batch_id_for_requests(requests: list[ManagedOrderRequest]) -> str:
    signatures = [_logical_request_signature(request) for request in requests]
    payload = json.dumps(signatures, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"qmtbatch_{sha1(payload.encode('utf-8')).hexdigest()[:24]}"


def _logical_batch_id_for_batch(batch: OrderBatchRecord) -> str | None:
    orders = batch.request_json.get("orders") if isinstance(batch.request_json, dict) else None
    if not isinstance(orders, list):
        return None
    requests = [request_from_payload(order) for order in orders if isinstance(order, dict)]
    if not requests:
        return None
    return _logical_batch_id_for_requests(requests)


def _requests_from_batch(batch: OrderBatchRecord) -> list[ManagedOrderRequest]:
    orders = batch.request_json.get("orders") if isinstance(batch.request_json, dict) else None
    if not isinstance(orders, list):
        return []
    return _batch_submission_order([request_from_payload(order) for order in orders if isinstance(order, dict)])


def _batch_submission_order(requests: list[ManagedOrderRequest]) -> list[ManagedOrderRequest]:
    return [
        request
        for _index, request in sorted(
            enumerate(requests),
            key=lambda item: (0 if item[1].order_type == SELL_ORDER_TYPE else 1, item[0]),
        )
    ]


def _shrink_near_cash_overshoot_requests(
    requests: list[ManagedOrderRequest],
    *,
    preview_order: Callable[[ManagedOrderRequest], OrderPreflightResult],
) -> list[ManagedOrderRequest]:
    """Apply deterministic board-lot shrink only when the plan explicitly opts in."""

    adjusted = list(requests)
    buy_indexes_by_key: dict[tuple[str, str], list[int]] = {}
    for index, request in enumerate(adjusted):
        if request.order_type != BUY_ORDER_TYPE or not _cash_shrink_enabled(request):
            continue
        buy_indexes_by_key.setdefault((request.account_id, request.strategy_name), []).append(index)

    for key, indexes in buy_indexes_by_key.items():
        if any(
            request.order_type == SELL_ORDER_TYPE and (request.account_id, request.strategy_name) == key
            for request in adjusted
        ):
            continue
        preflights = [preview_order(adjusted[index]) for index in indexes]
        total_freeze = sum(preflight.freeze_amount for preflight in preflights)
        available_cash = _shared_available_cash(preflights)
        if available_cash is None or total_freeze <= available_cash:
            continue
        shrink_metadata = adjusted[indexes[0]].metadata
        overshoot = total_freeze - available_cash
        max_tolerance = min(
            _metadata_decimal(shrink_metadata, "miniqmt_cash_shrink_max_overshoot") or Decimal("0"),
            available_cash * (
                _metadata_decimal(shrink_metadata, "miniqmt_cash_shrink_max_overshoot_ratio") or Decimal("0")
            ),
        )
        if max_tolerance <= Decimal("0") or overshoot > max_tolerance:
            continue
        proposed_adjusted = list(adjusted)
        remaining = overshoot
        shrink_events: list[dict[str, Any]] = []
        adjusted_indexes: set[int] = set()
        freeze_by_index = {index: preflight.freeze_amount for index, preflight in zip(indexes, preflights, strict=True)}
        for index in sorted(indexes, key=lambda item: (freeze_by_index[item], item), reverse=True):
            if remaining <= Decimal("0"):
                break
            request = proposed_adjusted[index]
            if request.price <= Decimal("0"):
                continue
            try:
                min_qty, increment = board_lot_rule(request.symbol)
            except ValueError:
                continue
            if request.quantity <= min_qty:
                continue
            steps_needed = int((remaining / (request.price * Decimal(increment))).to_integral_value(rounding=ROUND_CEILING))
            shrink_qty = max(increment, steps_needed * increment)
            max_shrink_qty = max(((request.quantity - min_qty) // increment) * increment, 0)
            shrink_qty = min(shrink_qty, max_shrink_qty)
            if shrink_qty <= 0:
                continue
            new_quantity = request.quantity - shrink_qty
            proposed_adjusted[index] = _shrink_request_quantity(
                request,
                new_quantity=new_quantity,
                original_batch_required_cash=total_freeze,
                available_cash=available_cash,
            )
            adjusted_indexes.add(index)
            reduction = request.price * Decimal(shrink_qty)
            remaining -= reduction
            shrink_events.append(
                {
                    "order_remark": request.order_remark,
                    "symbol": request.symbol,
                    "original_quantity": request.quantity,
                    "adjusted_quantity": new_quantity,
                    "reduced_cash": float(reduction),
                }
            )
        if remaining > Decimal("0"):
            continue
        adjusted = [
            _attach_group_shrink_summary(proposed_adjusted[index], shrink_events=shrink_events)
            if index in adjusted_indexes
            else proposed_adjusted[index]
            for index in range(len(proposed_adjusted))
        ]
    return adjusted


def _cash_shrink_enabled(request: ManagedOrderRequest) -> bool:
    raw = request.metadata.get("miniqmt_cash_preflight_shrink_enabled")
    if isinstance(raw, bool):
        return raw
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y"}


def _shared_available_cash(preflights: list[OrderPreflightResult]) -> Decimal | None:
    available_values = [preflight.available_cash for preflight in preflights if preflight.available_cash is not None]
    if len(available_values) != len(preflights):
        return None
    first = available_values[0] if available_values else None
    if first is None or any(value != first for value in available_values):
        return None
    return first


def _metadata_decimal(metadata: dict[str, Any], key: str) -> Decimal | None:
    raw = metadata.get(key)
    if raw in (None, ""):
        return None
    try:
        return Decimal(str(raw))
    except (InvalidOperation, ValueError):
        return None


def _shrink_request_quantity(
    request: ManagedOrderRequest,
    *,
    new_quantity: int,
    original_batch_required_cash: Decimal,
    available_cash: Decimal,
) -> ManagedOrderRequest:
    metadata = dict(request.metadata)
    metadata.update(
        {
            "miniqmt_cash_preflight_shrunk": True,
            "miniqmt_cash_preflight_original_quantity": request.quantity,
            "miniqmt_cash_preflight_adjusted_quantity": new_quantity,
            "miniqmt_cash_preflight_original_freeze": str(request.price * Decimal(request.quantity)),
            "miniqmt_cash_preflight_adjusted_freeze": str(request.price * Decimal(new_quantity)),
            "miniqmt_cash_preflight_available_cash": str(available_cash),
            "miniqmt_cash_preflight_original_batch_required_cash": str(original_batch_required_cash),
            _CASH_SHRINK_REASON_KEY: "near_cash_overshoot_within_configured_safety_buffer",
        }
    )
    return replace(request, quantity=new_quantity, metadata=metadata)


def _attach_group_shrink_summary(
    request: ManagedOrderRequest,
    *,
    shrink_events: list[dict[str, Any]],
) -> ManagedOrderRequest:
    metadata = dict(request.metadata)
    metadata["miniqmt_cash_preflight_shrink_events"] = list(shrink_events)
    return replace(request, metadata=metadata)


def _without_preflight_error_codes(
    errors: list[OrderPreflightError],
    codes: set[str],
) -> list[OrderPreflightError]:
    return [error for error in errors if error.code not in codes]


def _is_dependent_buy_proceeds_deferred(
    request: ManagedOrderRequest,
    preflight: OrderPreflightResult,
) -> bool:
    if request.order_type != BUY_ORDER_TYPE or preflight.allowed:
        return False
    error_codes = {error.code for error in preflight.errors}
    return bool(error_codes) and error_codes <= _DEPENDENT_BUY_PROCEEDS_ERROR_CODES


def _is_dependent_buy_retry_candidate(
    request: ManagedOrderRequest,
    result: ManagedOrderSubmitResult,
) -> bool:
    error_codes = {error.code for error in result.preflight.errors}
    return (
        request.order_type == BUY_ORDER_TYPE
        and not result.success
        and not result.broker_called
        and result.intent_id is None
        and bool(error_codes)
        and error_codes <= _DEPENDENT_BUY_PROCEEDS_ERROR_CODES
    )


def _is_retry_waiting_for_cash(preflight: OrderPreflightResult) -> bool:
    error_codes = {error.code for error in preflight.errors}
    return bool(error_codes) and error_codes <= {
        "INSUFFICIENT_CASH",
        "BATCH_INSUFFICIENT_CASH",
        "BATCH_INSUFFICIENT_ACCOUNT_GROUP_CASH",
        *_DEPENDENT_BUY_PROCEEDS_ERROR_CODES,
    }


def _sell_proceeds_required_error(
    *,
    request: ManagedOrderRequest,
    result: OrderPreflightResult,
    sell_requests: list[ManagedOrderRequest],
    same_batch_sell_proceeds: Decimal,
    effective_cash: Decimal,
    batch_required_cash: Decimal,
    cumulative_required_cash: Decimal,
) -> OrderPreflightError:
    return OrderPreflightError(
        "SELL_PROCEEDS_REQUIRED",
        "dependent buy requires same-batch sell proceeds that are not reconciled yet",
        {
            "account_id": request.account_id,
            "strategy_name": request.strategy_name,
            "buy_order_remark": request.order_remark,
            "symbol": request.symbol,
            "available_cash": float(result.available_cash or Decimal("0")),
            "same_batch_estimated_sell_proceeds": float(same_batch_sell_proceeds),
            "effective_cash": float(effective_cash),
            "batch_required_cash": float(batch_required_cash),
            "cumulative_required_cash": float(cumulative_required_cash),
            "required_cash": float(result.freeze_amount),
            "dependent_sell_orders": _sell_request_summaries(sell_requests),
            "next_action": "submit SELL first; resubmit BUY only after sell fill reconciles cash",
        },
    )


def _account_group_sell_proceeds_required_error(
    *,
    request: ManagedOrderRequest,
    result: OrderPreflightResult,
    sell_requests: list[ManagedOrderRequest],
    group_context: dict[str, Any],
    account_group_cash_limit: Decimal,
    same_batch_sell_proceeds: Decimal,
    effective_account_group_cash_limit: Decimal,
    batch_required_cash: Decimal,
    cumulative_required_cash: Decimal,
) -> OrderPreflightError:
    return OrderPreflightError(
        "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED",
        "dependent buy requires same-batch account-group sell proceeds that are not reconciled yet",
        {
            **group_context,
            "strategy_name": request.strategy_name,
            "buy_order_remark": request.order_remark,
            "symbol": request.symbol,
            "account_group_cash_limit": float(account_group_cash_limit),
            "same_batch_estimated_sell_proceeds": float(same_batch_sell_proceeds),
            "effective_account_group_cash_limit": float(effective_account_group_cash_limit),
            "batch_required_cash": float(batch_required_cash),
            "cumulative_required_cash": float(cumulative_required_cash),
            "required_cash": float(result.freeze_amount),
            "dependent_sell_orders": _sell_request_summaries(sell_requests),
            "next_action": "submit SELL first; resubmit BUY only after sell fill reconciles account-group cash",
        },
    )


def _sell_request_summaries(requests: list[ManagedOrderRequest]) -> list[dict[str, Any]]:
    return [
        {
            "strategy_name": request.strategy_name,
            "symbol": request.symbol,
            "quantity": request.quantity,
            "price": str(request.price),
            "estimated_proceeds": str(max(request.price * Decimal(request.quantity), Decimal("0"))),
            "order_remark": request.order_remark,
        }
        for request in requests
        if request.order_type == SELL_ORDER_TYPE
    ]


def _request_signature(request: ManagedOrderRequest) -> dict[str, Any]:
    return {
        "account_id": request.account_id,
        "strategy_name": request.strategy_name,
        "symbol": request.symbol,
        "side": request.side,
        "order_type": request.order_type,
        "quantity": request.quantity,
        "price_type": request.price_type,
        "price": str(request.price),
        "order_remark": request.order_remark,
        "trade_date": request.trade_date.isoformat(),
        "mode": request.mode,
        "package_id": request.package_id,
        "selection_run_id": request.selection_run_id,
        "target_weight": str(request.target_weight) if request.target_weight is not None else None,
        "metadata": _stable_request_metadata(request.metadata),
    }


def _logical_request_signature(request: ManagedOrderRequest) -> dict[str, Any]:
    return {
        **_request_signature(request),
        "metadata": _logical_request_metadata(request.metadata),
    }


def _stable_request_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    stable = _json_safe(metadata)
    if not isinstance(stable, dict):
        return {}
    stable.pop("vnpy_action_id", None)
    stable.pop("vnpy_vt_orderid", None)
    # The full vn.py diagnostic contains generated order ids and timestamps;
    # order intent metadata still preserves it, but batch identity must be stable.
    stable.pop("vnpy_execution_diagnostic", None)
    vnpy_action = stable.get("vnpy_action")
    if isinstance(vnpy_action, dict):
        stable["vnpy_action"] = {
            key: value
            for key, value in vnpy_action.items()
            if key not in {"action_id", "vt_orderid"}
        }
    return stable


def _logical_request_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    stable = _stable_request_metadata(metadata)
    for key in (
        "runtime_algo_instance_id",
        "runtime_child_order_id",
        "runtime_parent_intent_id",
    ):
        stable.pop(key, None)
    return stable


def _compensation_actions(results: tuple[ManagedOrderSubmitResult, ...]) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    for result in results:
        if not result.success or not result.qmt_order_id:
            continue
        actions.append(
            {
                "action": "MANAGED_CANCEL",
                "endpoint": "/api/v1/qmt/virtual-strategies/orders/cancel",
                "intent_id": result.intent_id,
                "qmt_order_id": result.qmt_order_id,
                "reason": "broker accepted this item before another batch item failed",
            }
        )
    return actions


def _result_from_existing_intent(intent: OrderIntentRecord) -> ManagedOrderSubmitResult:
    success = intent.submit_status == IntentSubmitStatus.ACCEPTED
    preflight = OrderPreflightResult(
        allowed=intent.preflight_status == IntentPreflightStatus.PASSED,
        errors=(),
        strategy_id=intent.strategy_id,
        estimated_notional=intent.estimated_notional or Decimal("0"),
        estimated_fee=intent.estimated_fee or Decimal("0"),
        freeze_amount=(
            (intent.estimated_notional or Decimal("0")) + (intent.estimated_fee or Decimal("0"))
            if intent.order_type == BUY_ORDER_TYPE
            else Decimal("0")
        ),
        available_cash=None,
    )
    return ManagedOrderSubmitResult(
        success=success,
        intent_id=intent.intent_id,
        qmt_order_id=None,
        broker_message="existing batch retry",
        preflight=preflight,
        broker_called=False,
    )


def _result_from_dict(value: dict[str, Any]) -> ManagedOrderSubmitResult:
    preflight_payload = dict(value.get("preflight") or {})
    errors = tuple(
        OrderPreflightError(
            code=str(error.get("code") or ""),
            message=str(error.get("message") or ""),
            context=dict(error.get("context") or {}),
        )
        for error in preflight_payload.get("errors") or []
    )
    preflight = OrderPreflightResult(
        allowed=bool(preflight_payload.get("allowed")),
        errors=errors,
        strategy_id=preflight_payload.get("strategy_id"),
        estimated_notional=_decimal(preflight_payload.get("estimated_notional")),
        estimated_fee=_decimal(preflight_payload.get("estimated_fee")),
        freeze_amount=_decimal(preflight_payload.get("freeze_amount")),
        available_cash=(
            _decimal(preflight_payload.get("available_cash"))
            if preflight_payload.get("available_cash") is not None
            else None
        ),
        strategy_available_sell_quantity=preflight_payload.get("strategy_available_sell_quantity"),
        pending_sell_quantity=preflight_payload.get("pending_sell_quantity"),
        broker_can_sell=preflight_payload.get("broker_can_sell"),
    )
    return ManagedOrderSubmitResult(
        success=bool(value.get("success")),
        intent_id=value.get("intent_id"),
        qmt_order_id=value.get("qmt_order_id"),
        broker_message=str(value.get("broker_message") or "existing batch retry"),
        preflight=preflight,
        broker_called=bool(value.get("broker_called")),
    )


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value
