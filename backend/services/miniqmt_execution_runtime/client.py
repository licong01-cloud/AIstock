"""Runtime client facades for MiniQMT product entry points."""

from __future__ import annotations

import hashlib
import json
import logging
import math
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Callable, Mapping

from backend.execution_algos.board_lot import board_lot_rule
from backend.execution_algos.vnpy_style import is_vnpy_style_algo
from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandleStatus
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderBatchRecord,
    OrderBatchStatus,
    OrderIntentRecord,
    OrderLedgerRecord,
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_REJECTED,
    is_partial_order_status,
    is_terminal_order_status,
)
from backend.services.qmt_strategy_ledger.order_service import (
    _batch_id_for_requests,
    _batch_submission_order,
    _is_capacity_residual_skipped,
    _is_dependent_buy_proceeds_deferred,
    _is_non_compensating_batch_residual,
    _request_signature,
    _result_from_dict,
    _shrink_near_cash_overshoot_requests,
    ManagedBatchSubmitResult,
    ManagedOrderRequest,
    ManagedOrderSubmitResult,
    OrderPreflightError,
    OrderPreflightResult,
    QmtManagedOrderService,
)
from backend.services.qmt_strategy_ledger.repository import QmtStrategyLedgerRepository
from backend.services.trading_core.errors import BrokerSubmitError, DataUnavailableError
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTAlgoExecutionResult,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

from .b0_quote_v2 import (
    B0QuoteV2Controller,
    B0QuoteV2ControllerFactory,
    B0QuoteV2RevisionV1,
    ParentQuoteControlAssignmentV1,
    QuoteControlBindingV1,
    source_build_manifest,
)
from .config import MiniQMTExecutionRuntimeKind
from .gateway import MiniQMTGateway, MiniQMTGatewayCancelAck, MiniQMTGatewayOrderAck, QmtClientMiniQMTEventLoopGateway
from .models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOperatorCommandResult,
)
from .repository import MiniQMTExecutionRuntimeRepository, default_miniqmt_execution_runtime_repository
from .runtime import MiniQMTExecutionRuntime

RUNTIME_OWNER = "MiniQMTExecutionRuntime"
MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE = "MINIQMT_REALTIME.broker_quote"
LOGGER = logging.getLogger(__name__)
DEFAULT_MARKETABLE_LIMIT_CROSS_TICKS = 1
DEFAULT_MARKETABLE_LIMIT_PROTECTION_BAND_PCT = 0.02
DEFAULT_A_SHARE_PRICE_TICK = 0.01


def _normalize_runtime_kind(runtime_kind: MiniQMTExecutionRuntimeKind | str | None) -> MiniQMTExecutionRuntimeKind:
    if runtime_kind is None:
        return MiniQMTExecutionRuntimeKind.EVENT_LOOP
    kind = MiniQMTExecutionRuntimeKind(runtime_kind)
    if kind == MiniQMTExecutionRuntimeKind.COMPILER:
        raise BrokerSubmitError(
            "MiniQMT SIM compiler runtime is retired; instantiate event_loop only",
            context={
                "reason_code": "MINIQMT_SIM_COMPILER_ROUTE_RETIRED",
                "stage": "MINIQMT_RUNTIME_KIND_REJECTED",
                "runtime_kind": kind.value,
                "allowed_runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
            },
        )
    return MiniQMTExecutionRuntimeKind.EVENT_LOOP


@dataclass(frozen=True)
class MiniQMTRuntimeEvidence:
    runtime_id: str
    runtime_owner: str
    account_group_id: str
    trade_date: date
    event_count: int
    algo_instance_ids: tuple[str, ...]
    child_order_ids: tuple[str, ...]
    submitted_child_count: int
    rejected_child_count: int
    source: str
    active_algo_count: int = 0
    completed_algo_count: int = 0
    pending_algo_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "runtime_id": self.runtime_id,
            "runtime_owner": self.runtime_owner,
            "account_group_id": self.account_group_id,
            "trade_date": self.trade_date.isoformat(),
            "event_count": self.event_count,
            "algo_instance_ids": list(self.algo_instance_ids),
            "child_order_ids": list(self.child_order_ids),
            "submitted_child_count": self.submitted_child_count,
            "rejected_child_count": self.rejected_child_count,
            "active_algo_count": self.active_algo_count,
            "completed_algo_count": self.completed_algo_count,
            "pending_algo_count": self.pending_algo_count,
            "source": self.source,
        }


@dataclass(frozen=True)
class MiniQMTPlanPreviewResult:
    requests: tuple[ManagedOrderRequest, ...]
    preflights: tuple[OrderPreflightResult, ...]
    runtime_evidence: MiniQMTRuntimeEvidence


@dataclass(frozen=True)
class MiniQMTRuntimeManagedBatchSubmitResult:
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
    runtime_evidence: MiniQMTRuntimeEvidence | None = None

    @classmethod
    def from_managed_result(
        cls,
        result: ManagedBatchSubmitResult,
        *,
        runtime_evidence: MiniQMTRuntimeEvidence,
    ) -> "MiniQMTRuntimeManagedBatchSubmitResult":
        return cls(
            success=result.success,
            total=result.total,
            succeeded=result.succeeded,
            failed=result.failed,
            results=tuple(result.results),
            compensation_required=result.compensation_required,
            compensation_hint=result.compensation_hint,
            batch_id=result.batch_id,
            batch_status=result.batch_status,
            preflight_passed=result.preflight_passed,
            retry_of_batch_id=result.retry_of_batch_id,
            compensation_actions=tuple(result.compensation_actions),
            runtime_evidence=runtime_evidence,
        )

    def to_dict(self) -> dict[str, Any]:
        pending = sum(1 for result in self.results if _is_event_loop_pending_result(result))
        payload = {
            "success": self.success,
            "batch_id": self.batch_id,
            "batch_status": self.batch_status,
            "preflight_passed": self.preflight_passed,
            "retry_of_batch_id": self.retry_of_batch_id,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "pending": pending,
            "triggered_child_order_count": self.succeeded,
            "pending_child_trigger_count": pending,
            "results": [result.to_dict() for result in self.results],
            "compensation_required": self.compensation_required,
            "compensation_hint": self.compensation_hint,
            "compensation_actions": list(self.compensation_actions),
            "runtime_evidence": self.runtime_evidence.to_dict() if self.runtime_evidence else None,
        }
        if _all_results_preview_only(self.results):
            payload["preview_only"] = True
            payload["broker_called"] = any(result.broker_called for result in self.results)
        return payload


@dataclass(frozen=True)
class MiniQMTRuntimeManagedVnpyBuildResult:
    requests: tuple[ManagedOrderRequest, ...]
    runtime_evidence: MiniQMTRuntimeEvidence
    child_order_id_by_order_remark: dict[str, str]


@dataclass(frozen=True)
class MiniQMTEventLoopPreflightResult:
    batch_id: str
    retry_of_batch_id: str | None
    requests: tuple[ManagedOrderRequest, ...]
    results: tuple[ManagedOrderSubmitResult, ...]
    request_by_parent_intent_id: dict[str, ManagedOrderRequest]
    submit_parent_intent_ids: frozenset[str]


@dataclass(frozen=True)
class MiniQMTRuntimeTickDriveResult:
    runtime_id: str
    source: str
    symbols: tuple[str, ...]
    quote_count: int
    tick_event_count: int
    child_order_count_before: int
    child_order_count_after: int
    triggered_child_order_count: int
    submitted_child_count: int
    rejected_child_count: int
    active_algo_count: int
    completed_algo_count: int
    pending_algo_count: int
    pending_parent_intent_ids: tuple[str, ...]
    failed_quote_symbols: tuple[str, ...]
    errors: tuple[dict[str, Any], ...]
    batch_results: dict[str, dict[str, Any]]
    runtime_evidence: MiniQMTRuntimeEvidence

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_event_loop_tick_driver_v1",
            "runtime_id": self.runtime_id,
            "source": self.source,
            "symbols": list(self.symbols),
            "quote_count": self.quote_count,
            "tick_event_count": self.tick_event_count,
            "child_order_count_before": self.child_order_count_before,
            "child_order_count_after": self.child_order_count_after,
            "triggered_child_order_count": self.triggered_child_order_count,
            "submitted_child_count": self.submitted_child_count,
            "rejected_child_count": self.rejected_child_count,
            "active_algo_count": self.active_algo_count,
            "completed_algo_count": self.completed_algo_count,
            "pending_algo_count": self.pending_algo_count,
            "pending_parent_intent_ids": list(self.pending_parent_intent_ids),
            "failed_quote_symbols": list(self.failed_quote_symbols),
            "errors": [dict(item) for item in self.errors],
            "batch_results": self.batch_results,
            "runtime_evidence": self.runtime_evidence.to_dict(),
        }


class MiniQMTExecutionRuntimeClient:
    """Facade used by product paths to enter the canonical runtime owner."""

    def __init__(
        self,
        *,
        repository: MiniQMTExecutionRuntimeRepository | None = None,
        strategy_ledger_repository: Any | None = None,
        runtime_kind: MiniQMTExecutionRuntimeKind | str | None = None,
        b0_quote_v2_controller_factory: B0QuoteV2ControllerFactory | None = None,
    ) -> None:
        self.repository = repository or default_miniqmt_execution_runtime_repository()
        self.runtime_kind = _normalize_runtime_kind(runtime_kind)
        self.strategy_ledger_repository = strategy_ledger_repository
        self.b0_quote_v2_controller_factory = b0_quote_v2_controller_factory
        if self.strategy_ledger_repository is None:
            self.strategy_ledger_repository = QmtStrategyLedgerRepository()

    def preview_managed_order_requests(
        self,
        *,
        managed_order_service: QmtManagedOrderService,
        requests: list[ManagedOrderRequest],
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str | None = None,
        source: str = "simulation_runtime_preview",
    ) -> MiniQMTPlanPreviewResult:
        self._reject_event_loop_compiler_lifecycle(source=source, operation="preview_managed_order_requests")
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=_PreviewOnlyRuntimeGateway(),
        )
        runtime.start()
        runtime.record_operator_command(
            command_id=f"opcmd_preview_{_short_hash([runtime.config.runtime_id, len(requests)])}",
            command_type="AUDIT_RUNTIME_STATE",
            reason="miniqmt_runtime_preview_preflight",
            payload={"request_count": len(requests), "source": source},
        )
        preflights = tuple(managed_order_service.preview_order(request) for request in requests)
        return MiniQMTPlanPreviewResult(
            requests=tuple(requests),
            preflights=preflights,
            runtime_evidence=self._evidence(runtime, source=source),
        )

    def submit_managed_order_requests(
        self,
        *,
        managed_order_service: QmtManagedOrderService,
        requests: list[ManagedOrderRequest],
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str | None = None,
        source: str = "simulation_runtime_submit",
        algo_code: str = "MINIQMT_MANAGED_ORDER",
    ) -> MiniQMTRuntimeManagedBatchSubmitResult:
        self._reject_event_loop_compiler_lifecycle(source=source, operation="submit_managed_order_requests")
        if not requests:
            raise BrokerSubmitError("MiniQMTExecutionRuntime requires at least one managed order request")
        materialized_requests = tuple(requests)
        gateway = _ManagedOrderRequestRuntimeGateway(
            managed_request_factory=lambda _child, index: materialized_requests[index - 1]
        )
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=gateway,
        )
        runtime.start()
        for request in materialized_requests:
            parent_intent_id = str(request.metadata.get("execution_plan_intent_id") or request.order_remark)
            instance = runtime.create_algo_instance(
                parent_intent_id=parent_intent_id,
                strategy_slot_id=str(request.metadata.get("strategy_slot_id") or request.strategy_name),
                symbol=request.symbol,
                side=OrderSide.BUY if request.side == "BUY" else OrderSide.SELL,
                target_quantity=int(request.quantity),
                algo_code=algo_code,
                metadata={
                    "source": source,
                    "managed_order_request": _managed_request_signature(request),
                    "execution_plan_id": request.metadata.get("execution_plan_id"),
                    "execution_plan_intent_id": request.metadata.get("execution_plan_intent_id"),
                },
            )
            runtime.submit_child_order(
                algo_instance_id=instance.algo_instance_id,
                quantity=int(request.quantity),
                price=float(request.price),
                price_type=int(request.price_type),
                metadata={
                    "source": source,
                    "order_remark": request.order_remark,
                    "managed_order_request": _managed_request_signature(request),
                },
            )

        managed_result = gateway.submit_managed_batch(order_service=managed_order_service)
        for request, item in zip(materialized_requests, managed_result.results, strict=True):
            child_order_id = gateway.child_order_id_by_order_remark.get(request.order_remark)
            if child_order_id:
                self._sync_managed_child_result(
                    runtime_id=runtime.config.runtime_id,
                    child_order_id=child_order_id,
                    managed_result=item,
                    ledger_order=_ledger_order_for_managed_result(managed_order_service, request, item),
                    source=source,
                )
        return MiniQMTRuntimeManagedBatchSubmitResult.from_managed_result(
            managed_result,
            runtime_evidence=self._evidence(runtime, source=source),
        )

    def build_managed_vnpy_order_requests(
        self,
        *,
        parent_intents: list[OrderIntent],
        policy_context: dict[str, Any],
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str | None,
        strategy_slot_id: str,
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest],
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        source: str = "simulation_runtime_vnpy_request_build",
    ) -> MiniQMTRuntimeManagedVnpyBuildResult:
        self._reject_event_loop_compiler_lifecycle(
            source=source,
            operation="build_managed_vnpy_order_requests",
            parent_intent_count=len(parent_intents),
        )
        if not parent_intents:
            raise BrokerSubmitError("MiniQMTExecutionRuntime requires at least one vn.py parent intent")
        policy_json = policy_context.get("policy_json") if isinstance(policy_context, dict) else None
        if not isinstance(policy_json, dict):
            raise BrokerSubmitError("MiniQMTExecutionRuntime managed vn.py path requires policy_json")
        algo_code = str(policy_json.get("algo_code") or "").strip().upper()
        if not is_vnpy_style_algo(algo_code):
            raise BrokerSubmitError(
                "MiniQMTExecutionRuntime managed vn.py path requires approved MiniQMT algo",
                context={"algo_code": algo_code},
            )
        gateway = _ManagedOrderRequestRuntimeGateway(managed_request_factory=managed_request_factory)
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=gateway,
            metadata={"source": source, "algo_code": algo_code},
        )
        runtime.start()
        for intent in parent_intents:
            min_volume, volume_increment = _board_lot_for_runtime(intent.symbol)
            runtime.create_vnpy_algo_instance(
                parent_intent_id=intent.intent_id,
                strategy_slot_id=strategy_slot_id,
                symbol=intent.symbol,
                side=intent.side,
                target_quantity=int(intent.quantity),
                algo_code=algo_code,
                limit_price=_limit_price_for_runtime(
                    intent=intent,
                    quote_provider=quote_provider,
                    algo_config=dict(policy_json.get("algo_config") or {}),
                ),
                algo_config=dict(policy_json.get("algo_config") or {}),
                min_volume=min_volume,
                volume_increment=volume_increment,
                metadata={
                    "source": source,
                    "runtime_child_context": _managed_vnpy_child_metadata(
                        intent=intent,
                        trade_date=trade_date,
                        source=source,
                        execution_policy_context=policy_context,
                    ),
                    "execution_policy_id": policy_context.get("validated_execution_policy_id"),
                    "execution_policy_sha256": policy_context.get("policy_sha256"),
                },
            )
            tick_payload = _tick_payload_for_runtime(intent=intent, quote_provider=quote_provider)
            runtime.on_tick(symbol=intent.symbol, price=float(tick_payload["price"]), payload=tick_payload)
            for index in range(_timer_iterations(algo_code, dict(policy_json.get("algo_config") or {}))):
                runtime.on_timer(timer_name=f"simulation_runtime_{algo_code.lower()}_{index + 1}")
        return MiniQMTRuntimeManagedVnpyBuildResult(
            requests=tuple(gateway.requests),
            runtime_evidence=self._evidence(runtime, source=source),
            child_order_id_by_order_remark=dict(gateway.child_order_id_by_order_remark),
        )

    def preview_managed_vnpy_order_requests(
        self,
        *,
        managed_order_service: QmtManagedOrderService,
        parent_intents: list[OrderIntent],
        policy_context: dict[str, Any],
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str | None,
        strategy_slot_id: str,
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest],
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        source: str = "simulation_runtime_vnpy_preview",
    ) -> MiniQMTPlanPreviewResult:
        build = self.build_managed_vnpy_order_requests(
            parent_intents=parent_intents,
            policy_context=policy_context,
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            strategy_slot_id=strategy_slot_id,
            managed_request_factory=managed_request_factory,
            quote_provider=quote_provider,
            source=source,
        )
        preflights = tuple(managed_order_service.preview_order(request) for request in build.requests)
        return MiniQMTPlanPreviewResult(
            requests=build.requests,
            preflights=preflights,
            runtime_evidence=build.runtime_evidence,
        )

    def submit_managed_vnpy_order_requests(
        self,
        *,
        managed_order_service: QmtManagedOrderService,
        parent_intents: list[OrderIntent],
        policy_context: dict[str, Any],
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str | None,
        strategy_slot_id: str,
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest],
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        source: str = "simulation_runtime_vnpy_submit",
    ) -> MiniQMTRuntimeManagedBatchSubmitResult:
        build = self.build_managed_vnpy_order_requests(
            parent_intents=parent_intents,
            policy_context=policy_context,
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            strategy_slot_id=strategy_slot_id,
            managed_request_factory=managed_request_factory,
            quote_provider=quote_provider,
            source=source,
        )
        if not build.requests:
            raise BrokerSubmitError("MiniQMTExecutionRuntime vn.py path generated no managed order requests")
        managed_result = _ManagedOrderRequestRuntimeGateway.from_requests(build.requests).submit_managed_batch(
            order_service=managed_order_service
        )
        for request, item in zip(build.requests, managed_result.results, strict=True):
            child_order_id = build.child_order_id_by_order_remark.get(request.order_remark)
            if child_order_id:
                self._sync_managed_child_result(
                    runtime_id=build.runtime_evidence.runtime_id,
                    child_order_id=child_order_id,
                    managed_result=item,
                    ledger_order=_ledger_order_for_managed_result(managed_order_service, request, item),
                    source=source,
                )
        return MiniQMTRuntimeManagedBatchSubmitResult.from_managed_result(
            managed_result,
            runtime_evidence=self.evidence_for_runtime(build.runtime_evidence.runtime_id, source=source),
        )

    def submit_event_loop_vnpy_parent_intents(
        self,
        *,
        parent_intents: list[OrderIntent],
        policy_context: dict[str, Any],
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str | None,
        strategy_slot_id: str,
        qmt_client: Any,
        strategy_name: str,
        order_remark_prefix: str,
        account_id: str | None = None,
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        child_context_factory: Callable[[OrderIntent, int], dict[str, Any]] | None = None,
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest] | None = None,
        managed_order_service: QmtManagedOrderService | None = None,
        source: str = "simulation_runtime_event_loop_submit",
    ) -> MiniQMTRuntimeManagedBatchSubmitResult:
        """Submit a SIM scope through the real event-loop gateway only."""

        if self.runtime_kind != MiniQMTExecutionRuntimeKind.EVENT_LOOP:
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires runtime_kind=event_loop",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_RUNTIME_KIND_REQUIRED",
                    "stage": "MINIQMT_EVENT_LOOP_RUNTIME_KIND_GATE",
                    "source": source,
                    "runtime_kind": self.runtime_kind.value,
                },
            )
        if self.strategy_ledger_repository is None:
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires qmt_strategy ledger authority",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_LEDGER_AUTHORITY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_LEDGER_AUTHORITY_GATE",
                    "source": source,
                    "runtime_id": runtime_id,
                    "account_group_id": account_group_id,
                },
            )
        if not parent_intents:
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires at least one parent intent",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_PARENT_INTENTS_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_PARENT_INTENT_GATE",
                    "source": source,
                },
            )
        policy_json = policy_context.get("policy_json") if isinstance(policy_context, dict) else None
        if not isinstance(policy_json, dict):
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires policy_json",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_POLICY_JSON_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_POLICY_GATE",
                    "source": source,
                },
            )
        algo_code = str(policy_json.get("algo_code") or "").strip().upper()
        if not is_vnpy_style_algo(algo_code):
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires approved MiniQMT vn.py algo",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_ALGO_UNSUPPORTED",
                    "stage": "MINIQMT_EVENT_LOOP_POLICY_GATE",
                    "source": source,
                    "algo_code": algo_code,
                },
            )
        quote_revision, quote_assignments = _b0_quote_v2_assignments(
            policy_context=policy_context,
            parent_intent_ids={intent.intent_id for intent in parent_intents},
        )
        if quote_revision is not None and self.b0_quote_v2_controller_factory is None:
            raise BrokerSubmitError(
                "B0_QUOTE_V2 submit requires the scheduler-owned controller factory",
                context={
                    "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                    "stage": "ADAPTER",
                    "runtime_id": runtime_id,
                    "broker_called": False,
                },
            )
        if quote_revision is not None:
            assert self.b0_quote_v2_controller_factory is not None
            assert_new_assignment = getattr(
                self.b0_quote_v2_controller_factory,
                "assert_accepts_new_assignments",
                None,
            )
            if not callable(assert_new_assignment):
                raise BrokerSubmitError(
                    "B0_QUOTE_V2 controller factory lacks the required admission contract",
                    context={
                        "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                        "stage": "ADAPTER",
                        "runtime_id": runtime_id,
                        "broker_called": False,
                    },
                )
            assert_new_assignment()
        gateway = QmtClientMiniQMTEventLoopGateway(
            qmt_client=qmt_client,
            strategy_name=strategy_name,
            order_remark_prefix=order_remark_prefix,
        )
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=gateway,
            account_id=account_id,
            metadata={
                "source": source,
                "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
                "gateway_class": "QmtClientMiniQMTEventLoopGateway",
                "oms_authority": "qmt_strategy_ledger",
                "algo_code": algo_code,
                "account_id": account_id,
                **(
                    {
                        "quote_control": dict(policy_context["quote_control"]),
                    }
                    if quote_revision is not None
                    else {}
                ),
            },
        )
        runtime.start()
        runtime.recover()
        controller: B0QuoteV2Controller | None = None
        if quote_revision is not None:
            assert self.b0_quote_v2_controller_factory is not None
            controller = self.b0_quote_v2_controller_factory.get(runtime.config.runtime_id)
            if controller is not None:
                existing_assignments = {
                    parent_id: assignment.canonical_payload()
                    for parent_id, assignment in controller.assignments.items()
                }
                incoming_assignments = {
                    parent_id: assignment.canonical_payload()
                    for parent_id, assignment in quote_assignments.items()
                }
                if existing_assignments != incoming_assignments:
                    active_algos = self.repository.list_algo_instances(
                        runtime.config.runtime_id,
                        active_only=True,
                    )
                    child_orders = self.repository.list_child_orders(
                        runtime.config.runtime_id,
                        active_only=False,
                    )
                    if active_algos or child_orders:
                        raise BrokerSubmitError(
                            "B0_QUOTE_V2 assignment revision cannot replace non-empty durable runtime state",
                            context={
                                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                                "stage": "ADAPTER",
                                "runtime_id": runtime.config.runtime_id,
                                "existing_parent_intent_ids": sorted(existing_assignments),
                                "incoming_parent_intent_ids": sorted(incoming_assignments),
                                "active_algo_count": len(active_algos),
                                "child_order_count": len(child_orders),
                                "broker_called": False,
                                "legacy_fallback": False,
                            },
                        )
                    controller.close()
                    controller = None
            if controller is None:
                controller = self.b0_quote_v2_controller_factory.create(
                    runtime=runtime,
                    assignments=quote_assignments,
                    symbols=tuple(sorted({intent.symbol for intent in parent_intents})),
                )
                runtime.bind_b0_quote_v2_controller(controller)
        existing_child_ids = {
            child.child_order_id
            for child in self.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
        }
        request_by_parent_intent_id: dict[str, ManagedOrderRequest] = {}
        preflight_result = self._event_loop_preflight_parent_intents(
            runtime=runtime,
            parent_intents=parent_intents,
            policy_context=policy_context,
            trade_date=trade_date,
            strategy_slot_id=strategy_slot_id,
            quote_provider=quote_provider,
            qmt_client=qmt_client,
            child_context_factory=child_context_factory,
            managed_request_factory=managed_request_factory,
            managed_order_service=managed_order_service,
            source=source,
        )
        batch_id = preflight_result.batch_id
        request_by_parent_intent_id.update(preflight_result.request_by_parent_intent_id)
        submitted_parent_ids = set(preflight_result.submit_parent_intent_ids)
        results_by_parent_id = {
            parent_id: result
            for request, result in zip(preflight_result.requests, preflight_result.results, strict=False)
            if (parent_id := _parent_id_from_request(request))
        }
        for index, intent in enumerate(parent_intents, start=1):
            if intent.intent_id not in submitted_parent_ids:
                continue
            request = request_by_parent_intent_id.get(intent.intent_id)
            result = results_by_parent_id.get(intent.intent_id)
            if request is None or result is None:
                raise BrokerSubmitError(
                    "MiniQMT event_loop submit lost qmt_strategy order intent preflight before broker call",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PREFLIGHT_MISSING",
                        "stage": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PERSIST",
                        "runtime_id": runtime.config.runtime_id,
                        "parent_intent_id": intent.intent_id,
                        "qmt_batch_id": batch_id,
                        "broker_called": False,
                    },
                )
            _event_loop_prepare_order_intent(
                repository=self.strategy_ledger_repository,
                request=request,
                preflight=result.preflight,
                source=source,
            )
            tick_payload = (
                _required_event_loop_tick_payload(
                    intent=intent,
                    quote_provider=quote_provider,
                    qmt_client=qmt_client,
                    source=source,
                )
                if controller is None
                else {}
            )
            child_context = (
                child_context_factory(intent, index)
                if child_context_factory is not None
                else _event_loop_child_metadata(intent=intent, trade_date=trade_date, source=source, index=index)
            )
            child_context = {**dict(child_context), "qmt_batch_id": batch_id}
            runtime.create_vnpy_algo_instance(
                parent_intent_id=intent.intent_id,
                strategy_slot_id=strategy_slot_id,
                symbol=intent.symbol,
                side=intent.side,
                target_quantity=int(intent.quantity),
                algo_code=algo_code,
                limit_price=(
                    _limit_price_for_event_loop(
                        intent=intent,
                        tick_payload=tick_payload,
                        algo_config=dict(policy_json.get("algo_config") or {}),
                    )
                    if controller is None
                    else _b0_quote_v2_initial_limit_price(intent)
                ),
                algo_config=dict(policy_json.get("algo_config") or {}),
                min_volume=1 if intent.side == OrderSide.SELL else None,
                volume_increment=1 if intent.side == OrderSide.SELL else None,
                metadata={
                    "source": source,
                    "runtime_child_context": child_context,
                    "execution_policy_id": policy_context.get("validated_execution_policy_id"),
                    "execution_policy_sha256": policy_context.get("policy_sha256"),
                    "event_loop_submit": True,
                    "qmt_batch_id": batch_id,
                    "quote_source": (
                        tick_payload.get("source") or tick_payload.get("quote_source")
                        if controller is None
                        else "B0_QUOTE_V2_NORMALIZED"
                    ),
                    "marketable_limit_reference_price": (
                        _execution_reference_price(intent=intent, tick_payload=tick_payload)
                        if controller is None
                        else _b0_quote_v2_initial_limit_price(intent)
                    ),
                    "marketable_limit_policy": dict(policy_json.get("algo_config") or {}),
                },
            )
            if controller is None:
                gateway.on_tick(tick_payload)
        if controller is not None:
            controller.lifecycle_tick()
        runtime_evidence = self._evidence(runtime, source=source)
        new_children = tuple(
            child
            for child in self.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
            if child.child_order_id not in existing_child_ids
        )
        child_by_parent: dict[str, list[MiniQMTChildOrder]] = {}
        for child in new_children:
            child_by_parent.setdefault(child.parent_intent_id, []).append(child)
        algo_by_parent = {
            instance.parent_intent_id: instance
            for instance in self.repository.list_algo_instances(runtime.config.runtime_id, active_only=False)
        }
        dispatch_failed_intents: list[OrderIntent] = []
        for intent in parent_intents:
            if intent.intent_id not in submitted_parent_ids:
                continue
            children = child_by_parent.get(intent.intent_id) or []
            if not children:
                request = request_by_parent_intent_id.get(intent.intent_id)
                result = results_by_parent_id.get(intent.intent_id)
                algo_instance = algo_by_parent.get(intent.intent_id)
                if request is not None and result is not None and _is_event_loop_pending_algo(algo_instance):
                    results_by_parent_id[intent.intent_id] = _event_loop_pending_algo_result(
                        intent=intent,
                        request=request,
                        preflight=result.preflight,
                        algo_instance=algo_instance,
                        source=source,
                    )
                    continue
                dispatch_failed_intents.append(intent)
                continue
            accepted_child = _accepted_event_loop_child(children)
            request = request_by_parent_intent_id.get(intent.intent_id)
            if request is None:
                continue
            results_by_parent_id[intent.intent_id] = _event_loop_child_submit_result(
                child=accepted_child,
                request=request,
                preflight=results_by_parent_id[intent.intent_id].preflight,
                repository=self.strategy_ledger_repository,
                source=source,
            )
        if dispatch_failed_intents:
            _raise_event_loop_no_child_order(
                missing_child_intents=dispatch_failed_intents,
                parent_intents=parent_intents,
                new_children=new_children,
                runtime_evidence=runtime_evidence,
                runtime_id=runtime.config.runtime_id,
                strategy_slot_id=strategy_slot_id,
                source=source,
            )
        results = tuple(
            results_by_parent_id[intent.intent_id]
            for intent in parent_intents
            if intent.intent_id in results_by_parent_id
        )
        results = self._event_loop_results_with_unsubmitted_residuals(
            requests=preflight_result.requests,
            results=results,
        )
        self._upsert_event_loop_batch_record(
            batch_id=batch_id,
            requests=preflight_result.requests,
            results=results,
            runtime_evidence=runtime_evidence,
            source=source,
        )
        succeeded = sum(1 for item in results if item.success)
        pending = sum(1 for item in results if _is_event_loop_pending_result(item))
        failed = len(results) - succeeded - pending
        batch_status = _event_loop_batch_status(preflight_result.requests, results)
        managed_result = ManagedBatchSubmitResult(
            success=failed == 0 and (succeeded > 0 or pending > 0),
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=_event_loop_compensation_required(batch_status, preflight_result.requests, results),
            compensation_hint=None if failed == 0 else "inspect event_loop child result reason_code",
            batch_id=batch_id,
            batch_status=batch_status.value,
            preflight_passed=_event_loop_preflight_passed(preflight_result.requests, results),
            retry_of_batch_id=preflight_result.retry_of_batch_id,
            compensation_actions=(),
        )
        return MiniQMTRuntimeManagedBatchSubmitResult.from_managed_result(
            managed_result,
            runtime_evidence=runtime_evidence,
        )

    def drive_event_loop_ticks(
        self,
        *,
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        runtime_id: str,
        qmt_client: Any,
        strategy_name: str,
        order_remark_prefix: str,
        account_id: str | None = None,
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        policy_context: dict[str, Any] | None = None,
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest] | None = None,
        managed_order_service: QmtManagedOrderService | None = None,
        as_of_time: datetime | None = None,
        source: str = "simulation_runtime_event_loop_tick_driver",
    ) -> MiniQMTRuntimeTickDriveResult:
        """Drive one non-blocking quote batch into active event-loop algos."""

        if self.runtime_kind != MiniQMTExecutionRuntimeKind.EVENT_LOOP:
            raise BrokerSubmitError(
                "MiniQMT event_loop tick driver requires runtime_kind=event_loop",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_RUNTIME_KIND_REQUIRED",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_GATE",
                    "runtime_id": runtime_id,
                    "runtime_kind": self.runtime_kind.value,
                },
            )
        if self.strategy_ledger_repository is None:
            raise BrokerSubmitError(
                "MiniQMT event_loop tick driver requires qmt_strategy ledger authority",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_LEDGER_AUTHORITY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_GATE",
                    "runtime_id": runtime_id,
                },
            )
        runtime_record = self.repository.get_runtime(runtime_id)
        if runtime_record is None:
            raise BrokerSubmitError(
                "MiniQMT event_loop tick driver found no durable runtime",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_RUNTIME_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_RUNTIME_LOAD",
                    "runtime_id": runtime_id,
                    "account_group_id": account_group_id,
                    "trade_date": trade_date.isoformat(),
                },
            )
        gateway = QmtClientMiniQMTEventLoopGateway(
            qmt_client=qmt_client,
            strategy_name=strategy_name,
            order_remark_prefix=order_remark_prefix,
        )
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=gateway,
            account_id=account_id,
            metadata={
                **dict(runtime_record.metadata or {}),
                "source": source,
                "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
                "gateway_class": "QmtClientMiniQMTEventLoopGateway",
                "oms_authority": "qmt_strategy_ledger",
                "account_id": account_id,
            },
        )
        all_runtime_instances = tuple(self.repository.list_algo_instances(runtime_id, active_only=False))
        quote_revision, quote_assignments = _b0_quote_v2_assignments(
            policy_context=policy_context or {},
            parent_intent_ids={instance.parent_intent_id for instance in all_runtime_instances},
        )
        controller: B0QuoteV2Controller | None = None
        if quote_revision is not None:
            if self.b0_quote_v2_controller_factory is None:
                raise BrokerSubmitError(
                    "B0_QUOTE_V2 tick driver requires the scheduler-owned controller factory",
                    context={
                        "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                        "stage": "ADAPTER",
                        "runtime_id": runtime_id,
                        "broker_called": False,
                    },
                )
            controller = self.b0_quote_v2_controller_factory.get(runtime_id)
            if controller is None:
                recovering_active = _b0_quote_v2_recovering_active(
                    algo_instances=all_runtime_instances,
                    active_child_orders=tuple(self.repository.list_child_orders(runtime_id, active_only=True)),
                )
                controller = self.b0_quote_v2_controller_factory.create(
                    runtime=runtime,
                    assignments=quote_assignments,
                    symbols=tuple(sorted({instance.symbol for instance in all_runtime_instances})),
                    recovering_active=recovering_active,
                )
                runtime.bind_b0_quote_v2_controller(controller)
        before_children = tuple(self.repository.list_child_orders(runtime_id, active_only=False))
        before_child_ids = {child.child_order_id for child in before_children}
        active_instances = tuple(
            instance
            for instance in self.repository.list_algo_instances(runtime_id, active_only=True)
            if is_vnpy_style_algo(instance.algo_code) and _is_event_loop_pending_algo(instance)
        )
        instance_by_symbol: dict[str, MiniQMTExecutionAlgoInstance] = {}
        for instance in active_instances:
            instance_by_symbol.setdefault(instance.symbol, instance)
        quote_count = 0
        tick_event_count = 0
        failed_quote_symbols: list[str] = []
        errors: list[dict[str, Any]] = []
        if controller is not None:
            controller.lifecycle_tick()
            quote_count = len(instance_by_symbol)
            tick_event_count = len(instance_by_symbol)
        for symbol in () if controller is not None else sorted(instance_by_symbol):
            instance = instance_by_symbol[symbol]
            try:
                probe_intent = _event_loop_probe_intent_for_algo(instance, trade_date=trade_date)
                tick_payload = _required_event_loop_tick_payload(
                    intent=probe_intent,
                    quote_provider=quote_provider,
                    qmt_client=qmt_client,
                    source=source,
                )
                policy = dict(
                    instance.metadata.get("marketable_limit_policy") or instance.metadata.get("algo_config") or {}
                )
                tail_sweep = _event_loop_tail_sweep_enabled(policy, as_of_time=as_of_time)
                cross_ticks = (
                    _positive_int_config(
                        policy,
                        "tail_sweep_cross_ticks",
                        max(
                            2,
                            _positive_int_config(
                                policy, "marketable_limit_cross_ticks", DEFAULT_MARKETABLE_LIMIT_CROSS_TICKS
                            ),
                        ),
                    )
                    if tail_sweep
                    else None
                )
                limit_price = _marketable_limit_price(
                    intent=probe_intent,
                    tick_payload=tick_payload,
                    algo_config=policy,
                    stage=("MINIQMT_EVENT_LOOP_TAIL_SWEEP" if tail_sweep else "MINIQMT_EVENT_LOOP_TICK_REPRICE"),
                    cross_ticks_override=cross_ticks,
                )
                runtime.reprice_pending_vnpy_algo(
                    algo_instance_id=instance.algo_instance_id,
                    limit_price=limit_price,
                    reason_code=(
                        "MINIQMT_EVENT_LOOP_TAIL_SWEEP_MARKETABLE_LIMIT"
                        if tail_sweep
                        else "MINIQMT_EVENT_LOOP_TICK_MARKETABLE_REPRICE"
                    ),
                    stage=("MINIQMT_EVENT_LOOP_TAIL_SWEEP" if tail_sweep else "MINIQMT_EVENT_LOOP_TICK_REPRICE"),
                    metadata={
                        "as_of_time": as_of_time.isoformat() if isinstance(as_of_time, datetime) else None,
                        "tail_sweep": tail_sweep,
                        "quote_source": tick_payload.get("quote_source"),
                    },
                )
                gateway.on_tick(tick_payload)
                quote_count += 1
                tick_event_count += 1
            except Exception as exc:  # noqa: BLE001
                failed_quote_symbols.append(symbol)
                errors.append(
                    {
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SYMBOL_FAILED",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_DISPATCH",
                        "runtime_id": runtime_id,
                        "symbol": symbol,
                        "algo_instance_id": instance.algo_instance_id,
                        "parent_intent_id": instance.parent_intent_id,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                        "error_context": getattr(exc, "context", None),
                    }
                )
        after_children = tuple(self.repository.list_child_orders(runtime_id, active_only=False))
        new_children = tuple(child for child in after_children if child.child_order_id not in before_child_ids)
        batch_results = self._sync_event_loop_triggered_children_to_batches(
            runtime_id=runtime_id,
            trade_date=trade_date,
            new_children=new_children,
            managed_request_factory=managed_request_factory,
            managed_order_service=managed_order_service,
            source=source,
        )
        evidence = self.evidence_for_runtime(runtime_id, source=source)
        all_instances = tuple(self.repository.list_algo_instances(runtime_id, active_only=False))
        all_children = tuple(self.repository.list_child_orders(runtime_id, active_only=False))
        child_parent_ids = {child.parent_intent_id for child in all_children}
        pending_parent_ids = tuple(
            sorted(
                instance.parent_intent_id
                for instance in all_instances
                if _is_event_loop_pending_algo(instance) and instance.parent_intent_id not in child_parent_ids
            )
        )
        return MiniQMTRuntimeTickDriveResult(
            runtime_id=runtime_id,
            source=source,
            symbols=tuple(sorted(instance_by_symbol)),
            quote_count=quote_count,
            tick_event_count=tick_event_count,
            child_order_count_before=len(before_children),
            child_order_count_after=len(after_children),
            triggered_child_order_count=len(new_children),
            submitted_child_count=evidence.submitted_child_count,
            rejected_child_count=evidence.rejected_child_count,
            active_algo_count=evidence.active_algo_count,
            completed_algo_count=evidence.completed_algo_count,
            pending_algo_count=evidence.pending_algo_count,
            pending_parent_intent_ids=pending_parent_ids,
            failed_quote_symbols=tuple(failed_quote_symbols),
            errors=tuple(errors),
            batch_results=batch_results,
            runtime_evidence=evidence,
        )

    def _sync_event_loop_triggered_children_to_batches(
        self,
        *,
        runtime_id: str,
        trade_date: date,
        new_children: tuple[MiniQMTChildOrder, ...],
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest] | None,
        managed_order_service: QmtManagedOrderService | None,
        source: str,
    ) -> dict[str, dict[str, Any]]:
        if not new_children:
            return {}
        results_by_batch_parent: dict[str, dict[str, ManagedOrderSubmitResult]] = {}
        for index, child in enumerate(new_children, start=1):
            request_index = _event_loop_child_request_index(child, fallback=index)
            request = (
                managed_request_factory(child, request_index)
                if managed_request_factory is not None
                else _managed_request_from_event_loop_child(child, index=request_index, trade_date=trade_date)
            )
            batch_id = str(child.metadata.get("qmt_batch_id") or request.metadata.get("qmt_batch_id") or "").strip()
            if not batch_id:
                raise BrokerSubmitError(
                    "MiniQMT event_loop tick driver cannot sync child without qmt_batch_id",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_ID_MISSING",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_SYNC",
                        "runtime_id": runtime_id,
                        "child_order_id": child.child_order_id,
                        "parent_intent_id": child.parent_intent_id,
                        "symbol": child.symbol,
                    },
                )
            preflight = self._event_loop_stored_batch_preflight(
                batch_id=batch_id,
                parent_intent_id=child.parent_intent_id,
            )
            if preflight is None:
                preflight = self._event_loop_preview_order(
                    request,
                    managed_order_service=managed_order_service,
                )
            result = _event_loop_child_submit_result(
                child=child,
                request=request,
                preflight=preflight,
                repository=self.strategy_ledger_repository,
                source=source,
            )
            results_by_batch_parent.setdefault(batch_id, {})[child.parent_intent_id] = result

        updated: dict[str, dict[str, Any]] = {}
        get_batch = getattr(self.strategy_ledger_repository, "get_order_batch", None)
        if not callable(get_batch):
            raise BrokerSubmitError(
                "MiniQMT event_loop tick driver requires get_order_batch for batch sync",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_REPOSITORY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_SYNC",
                    "runtime_id": runtime_id,
                },
            )
        for batch_id, parent_results in results_by_batch_parent.items():
            batch = get_batch(batch_id)
            if batch is None:
                raise BrokerSubmitError(
                    "MiniQMT event_loop tick driver cannot find qmt_strategy batch",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_MISSING",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_SYNC",
                        "runtime_id": runtime_id,
                        "qmt_batch_id": batch_id,
                    },
                )
            requests = tuple(_event_loop_requests_from_batch(batch))
            stored_results = tuple(
                _result_from_dict(item)
                for item in (batch.result_json or {}).get("results", ())
                if isinstance(item, dict)
            )
            results_by_parent = {
                parent_id: result
                for request, result in zip(requests, stored_results, strict=False)
                if (parent_id := _parent_id_from_request(request))
            }
            results_by_parent.update(parent_results)
            ordered_results = tuple(
                results_by_parent.get(_parent_id_from_request(request))
                or ManagedOrderSubmitResult(
                    False,
                    _parent_id_from_request(request) or None,
                    None,
                    "event_loop algo dispatched and running; child order pending tick trigger",
                    self._event_loop_preview_order(request, managed_order_service=managed_order_service),
                    False,
                )
                for request in requests
            )
            evidence = self.evidence_for_runtime(runtime_id, source=source)
            self._upsert_event_loop_batch_record(
                batch_id=batch_id,
                requests=requests,
                results=ordered_results,
                runtime_evidence=evidence,
                source=source,
            )
            latest = get_batch(batch_id)
            updated[batch_id] = {
                "batch_id": batch_id,
                "batch_status": latest.batch_status.value if latest is not None else None,
                "result_json": dict(latest.result_json or {}) if latest is not None else {},
                "metadata": dict(latest.metadata or {}) if latest is not None else {},
            }
        return updated

    def _event_loop_stored_batch_preflight(
        self,
        *,
        batch_id: str,
        parent_intent_id: str,
    ) -> OrderPreflightResult | None:
        get_batch = getattr(self.strategy_ledger_repository, "get_order_batch", None)
        if not callable(get_batch):
            return None
        batch = get_batch(batch_id)
        if batch is None:
            return None
        requests = tuple(_event_loop_requests_from_batch(batch))
        stored_results = tuple(
            _result_from_dict(item) for item in (batch.result_json or {}).get("results", ()) if isinstance(item, dict)
        )
        for request, result in zip(requests, stored_results, strict=False):
            if _parent_id_from_request(request) == parent_intent_id:
                return result.preflight
        return None

    def _event_loop_preflight_parent_intents(
        self,
        *,
        runtime: MiniQMTExecutionRuntime,
        parent_intents: list[OrderIntent],
        policy_context: dict[str, Any],
        trade_date: date,
        strategy_slot_id: str,
        quote_provider: Callable[[str], dict[str, Any] | None] | None,
        qmt_client: Any,
        child_context_factory: Callable[[OrderIntent, int], dict[str, Any]] | None,
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest] | None,
        managed_order_service: QmtManagedOrderService | None,
        source: str,
    ) -> MiniQMTEventLoopPreflightResult:
        parent_by_id = {intent.intent_id: intent for intent in parent_intents}
        arrival_capture_context_by_parent: dict[str, dict[str, Any]] = {}
        built_requests: list[ManagedOrderRequest] = []
        for index, intent in enumerate(parent_intents, start=1):
            built_requests.append(
                self._event_loop_parent_request(
                    runtime=runtime,
                    intent=intent,
                    policy_context=policy_context,
                    trade_date=trade_date,
                    strategy_slot_id=strategy_slot_id,
                    quote_provider=quote_provider,
                    qmt_client=qmt_client,
                    child_context=(
                        child_context_factory(intent, index)
                        if child_context_factory is not None
                        else _event_loop_child_metadata(
                            intent=intent,
                            trade_date=trade_date,
                            source=source,
                            index=index,
                        )
                    ),
                    managed_request_factory=managed_request_factory,
                    source=source,
                    index=index,
                    arrival_capture_context_by_parent=arrival_capture_context_by_parent,
                )
            )
        requests = _batch_submission_order(built_requests)
        request_quantity_before_cash_by_parent = {
            _parent_id_from_request(request): int(request.quantity)
            for request in requests
            if _parent_id_from_request(request)
        }
        requests = _shrink_near_cash_overshoot_requests(
            requests,
            preview_order=lambda request: self._event_loop_preview_order(
                request,
                managed_order_service=managed_order_service,
            ),
        )
        batch_id = _batch_id_for_requests(requests)
        requests_with_batch = tuple(
            replace(request, metadata={**dict(request.metadata or {}), "qmt_batch_id": batch_id})
            for request in requests
        )
        retry = self._event_loop_existing_batch_result(
            batch_id=batch_id,
            requests=list(requests_with_batch),
            request_count=len(requests_with_batch),
            managed_order_service=managed_order_service,
        )
        if retry is not None:
            return retry
        preflights = self._event_loop_batch_preflight(
            list(requests_with_batch),
            managed_order_service=managed_order_service,
        )
        hard_failed = any(
            not preflight.allowed and not _is_non_compensating_batch_residual(request, preflight)
            for request, preflight in zip(requests_with_batch, preflights, strict=True)
        )
        if hard_failed:
            results = tuple(
                ManagedOrderSubmitResult(False, None, None, "event_loop preflight failed", preflight, False)
                for preflight in preflights
            )
            self._upsert_event_loop_batch_record(
                batch_id=batch_id,
                requests=requests_with_batch,
                results=results,
                runtime_evidence=self._evidence(runtime, source=source),
                source=source,
            )
            _persist_event_loop_tca_batch_observations(
                repository=self.strategy_ledger_repository,
                batch_id=batch_id,
                requests=requests_with_batch,
                results=results,
                arrival_capture_context_by_parent=arrival_capture_context_by_parent,
                request_quantity_before_cash_by_parent=request_quantity_before_cash_by_parent,
                policy_context=policy_context,
            )
            return MiniQMTEventLoopPreflightResult(
                batch_id=batch_id,
                retry_of_batch_id=None,
                requests=requests_with_batch,
                results=results,
                request_by_parent_intent_id=_request_by_parent(parent_by_id, list(requests_with_batch)),
                submit_parent_intent_ids=frozenset(),
            )
        results_list: list[ManagedOrderSubmitResult] = []
        submit_parent_ids: set[str] = set()
        for request, preflight in zip(requests_with_batch, preflights, strict=True):
            parent_id = _parent_id_from_request(request)
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
            if not preflight.allowed and _is_capacity_residual_skipped(request, preflight):
                results_list.append(
                    ManagedOrderSubmitResult(
                        False,
                        None,
                        None,
                        "buy skipped by funds-only capacity allocator",
                        preflight,
                        False,
                    )
                )
                continue
            results_list.append(
                ManagedOrderSubmitResult(
                    False,
                    parent_id,
                    None,
                    "event_loop preflight passed; algo dispatch pending tick trigger",
                    preflight,
                    False,
                )
            )
            if parent_id:
                submit_parent_ids.add(parent_id)
        results = tuple(results_list)
        self._upsert_event_loop_batch_record(
            batch_id=batch_id,
            requests=requests_with_batch,
            results=results,
            runtime_evidence=self._evidence(runtime, source=source),
            source=source,
        )
        _persist_event_loop_tca_batch_observations(
            repository=self.strategy_ledger_repository,
            batch_id=batch_id,
            requests=requests_with_batch,
            results=results,
            arrival_capture_context_by_parent=arrival_capture_context_by_parent,
            request_quantity_before_cash_by_parent=request_quantity_before_cash_by_parent,
            policy_context=policy_context,
        )
        return MiniQMTEventLoopPreflightResult(
            batch_id=batch_id,
            retry_of_batch_id=None,
            requests=requests_with_batch,
            results=results,
            request_by_parent_intent_id=_request_by_parent(parent_by_id, list(requests_with_batch)),
            submit_parent_intent_ids=frozenset(submit_parent_ids),
        )

    def _event_loop_parent_request(
        self,
        *,
        runtime: MiniQMTExecutionRuntime,
        intent: OrderIntent,
        policy_context: dict[str, Any],
        trade_date: date,
        strategy_slot_id: str,
        quote_provider: Callable[[str], dict[str, Any] | None] | None,
        qmt_client: Any,
        child_context: dict[str, Any],
        managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest] | None,
        source: str,
        index: int,
        arrival_capture_context_by_parent: dict[str, dict[str, Any]],
    ) -> ManagedOrderRequest:
        arrival_time = datetime.now(UTC)
        tick_payload = _required_event_loop_tick_payload(
            intent=intent,
            quote_provider=quote_provider,
            qmt_client=qmt_client,
            source=source,
        )
        arrival_capture_context_by_parent[intent.intent_id] = {
            "arrival_time": arrival_time,
            "arrival_quote_received_at": datetime.now(UTC),
            "tick_payload": dict(tick_payload),
        }
        policy_json = policy_context.get("policy_json") if isinstance(policy_context, dict) else None
        price = _limit_price_for_event_loop(
            intent=intent,
            tick_payload=tick_payload,
            algo_config=dict(policy_json.get("algo_config") or {}) if isinstance(policy_json, dict) else {},
        )
        child_metadata = {
            **dict(child_context),
            "source": source,
            "runtime_owner": RUNTIME_OWNER,
            "runtime_id": runtime.config.runtime_id,
            "account_group_id": runtime.config.account_group_id,
            "runtime_parent_intent_id": intent.intent_id,
            "execution_policy_id": policy_context.get("validated_execution_policy_id"),
            "execution_policy_sha256": policy_context.get("policy_sha256"),
            "event_loop_preflight": True,
            "broker_quote_source": tick_payload.get("source") or tick_payload.get("quote_source"),
        }
        synthetic_child = MiniQMTChildOrder(
            runtime_id=runtime.config.runtime_id,
            algo_instance_id=f"mqalgo_preflight_{_short_hash([runtime.config.runtime_id, intent.intent_id])}",
            parent_intent_id=intent.intent_id,
            strategy_slot_id=strategy_slot_id,
            symbol=intent.symbol,
            side=intent.side,
            quantity=int(intent.quantity),
            price=price,
            price_type=int(child_context.get("price_type") or 5),
            metadata=child_metadata,
        )
        if managed_request_factory is not None:
            request = managed_request_factory(synthetic_child, index)
        else:
            request = _managed_request_from_event_loop_child(
                synthetic_child,
                index=index,
                trade_date=trade_date,
            )
        return replace(
            request,
            metadata={
                **dict(request.metadata or {}),
                "source": source,
                "runtime_owner": RUNTIME_OWNER,
                "runtime_id": runtime.config.runtime_id,
                "runtime_parent_intent_id": intent.intent_id,
                "runtime_child_order_id": synthetic_child.child_order_id,
                "runtime_algo_instance_id": synthetic_child.algo_instance_id,
                "event_loop_preflight": True,
                "event_loop_submit": True,
                "compiler_route_retired": True,
            },
        )

    def _event_loop_preview_order(
        self,
        request: ManagedOrderRequest,
        *,
        managed_order_service: QmtManagedOrderService | None,
    ) -> OrderPreflightResult:
        if managed_order_service is not None:
            return managed_order_service.preview_order(request)
        repository = self.strategy_ledger_repository
        if repository is None:
            raise BrokerSubmitError(
                "MiniQMT event_loop preflight requires qmt_strategy repository",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_LEDGER_AUTHORITY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT",
                    "order_remark": request.order_remark,
                },
            )
        return QmtManagedOrderService(repository=repository, broker=None).preview_order(request)

    def _event_loop_batch_preflight(
        self,
        requests: list[ManagedOrderRequest],
        *,
        managed_order_service: QmtManagedOrderService | None,
    ) -> list[OrderPreflightResult]:
        if managed_order_service is not None:
            helper = getattr(managed_order_service, "_batch_preflight", None)
            if callable(helper):
                return list(helper(requests))
            return [managed_order_service.preview_order(request) for request in requests]
        return [self._event_loop_preview_order(request, managed_order_service=None) for request in requests]

    def _event_loop_existing_batch_result(
        self,
        *,
        batch_id: str,
        requests: list[ManagedOrderRequest],
        request_count: int,
        managed_order_service: QmtManagedOrderService | None,
    ) -> MiniQMTEventLoopPreflightResult | None:
        repository = self.strategy_ledger_repository
        get_batch = getattr(repository, "get_order_batch", None)
        list_intents = getattr(repository, "list_order_intents_by_batch", None)
        if not callable(get_batch) or not callable(list_intents):
            return None
        batch = get_batch(batch_id)
        if batch is None:
            batch = self._event_loop_find_dependent_buy_batch_by_logical_key(requests)
        if batch is None:
            batch = self._event_loop_find_owned_failed_batch_by_remarks(requests)
        if batch is None:
            return None
        stored_requests = _event_loop_requests_from_batch(batch)
        if not stored_requests:
            return None
        stored_results = tuple(
            _result_from_dict(item) for item in (batch.result_json or {}).get("results", ()) if isinstance(item, dict)
        )
        metadata = batch.metadata if isinstance(batch.metadata, dict) else {}
        effective_batch_id = batch.batch_id
        if batch.batch_status == OrderBatchStatus.PARTIAL and metadata.get("dependent_buy_deferred"):
            return self._event_loop_dependent_buy_retry_result(
                batch_id=effective_batch_id,
                requests=stored_requests,
                batch=batch,
                managed_order_service=managed_order_service,
            )
        intents = list_intents(effective_batch_id)
        if batch.batch_status == OrderBatchStatus.PREFLIGHT_FAILED and intents:
            return self._event_loop_restore_owned_parent_intents(
                batch_id=effective_batch_id,
                requests=requests,
                stored_requests=stored_requests,
                intents=intents,
            )
        if batch.batch_status == OrderBatchStatus.PREFLIGHT_FAILED or (
            batch.batch_status == OrderBatchStatus.FAILED
            and metadata.get("capacity_residual_skipped")
            and stored_results
            and not any(result.broker_called for result in stored_results)
        ):
            return None
        results = stored_results or tuple(_event_loop_result_from_existing_intent(intent) for intent in intents)
        if not results:
            return None
        total = max(request_count, len(results))
        if len(results) != total:
            results = tuple(results) + tuple(
                ManagedOrderSubmitResult(
                    False,
                    None,
                    None,
                    "stored event_loop batch result missing item",
                    OrderPreflightResult(False, (), None, Decimal("0"), Decimal("0"), Decimal("0"), None),
                    False,
                )
                for _ in range(total - len(results))
            )
        return MiniQMTEventLoopPreflightResult(
            batch_id=effective_batch_id,
            retry_of_batch_id=effective_batch_id,
            requests=tuple(stored_requests),
            results=tuple(results),
            request_by_parent_intent_id={
                str(request.metadata.get("runtime_parent_intent_id") or request.order_remark): request
                for request in stored_requests
            },
            submit_parent_intent_ids=frozenset(),
        )

    def _event_loop_restore_owned_parent_intents(
        self,
        *,
        batch_id: str,
        requests: list[ManagedOrderRequest],
        stored_requests: list[ManagedOrderRequest],
        intents: list[Any],
    ) -> MiniQMTEventLoopPreflightResult:
        """Restore an exact, runtime-owned preflight without rerunning mutable checks.

        A B0 failure can happen after the parent intents have been durably written but
        before any broker call.  Re-previewing that same batch makes its own order
        remarks and pending sell reservations look foreign.  Recovery is therefore
        allowed only when the incoming request, stored batch request, remark index,
        and parent intent form one exact ownership chain with no broker side effect.
        """

        request_mismatches = _event_loop_owned_retry_request_mismatches(
            requests=requests,
            stored_requests=stored_requests,
        )
        if request_mismatches:
            raise BrokerSubmitError(
                "MiniQMT event_loop retry request differs from the durable failed batch",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_REQUEST_MISMATCH",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                    "qmt_batch_id": batch_id,
                    "mismatches": request_mismatches,
                    "broker_called": False,
                },
            )
        by_parent = {str(getattr(intent, "intent_id", "")): intent for intent in intents}
        expected_parent_ids = [_parent_id_from_request(request) for request in stored_requests]
        if (
            any(not parent_id for parent_id in expected_parent_ids)
            or len(expected_parent_ids) != len(set(expected_parent_ids))
            or len(intents) != len(expected_parent_ids)
            or set(by_parent) != set(expected_parent_ids)
        ):
            raise BrokerSubmitError(
                "MiniQMT event_loop failed batch has an incomplete durable parent-intent set",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_PARENT_SET_MISMATCH",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                    "qmt_batch_id": batch_id,
                    "expected_parent_intent_ids": expected_parent_ids,
                    "stored_parent_intent_ids": sorted(by_parent),
                    "broker_called": False,
                },
            )
        runtime_ids = {
            str(request.metadata.get("runtime_id") or "").strip()
            for request in stored_requests
        }
        if len(runtime_ids) != 1 or not next(iter(runtime_ids)):
            raise BrokerSubmitError(
                "MiniQMT event_loop owned retry requires one exact runtime identity",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_RUNTIME_SET_MISMATCH",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                    "qmt_batch_id": batch_id,
                    "runtime_ids": sorted(runtime_ids),
                    "broker_called": False,
                },
            )
        runtime_id = next(iter(runtime_ids))
        existing_children = [
            child
            for child in self.repository.list_child_orders(runtime_id, active_only=False)
            if child.parent_intent_id in set(expected_parent_ids)
        ]
        list_order_ledger = getattr(self.strategy_ledger_repository, "list_order_ledger", None)
        if not callable(list_order_ledger):
            raise BrokerSubmitError(
                "MiniQMT event_loop owned retry requires order-ledger side-effect readback",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_ORDER_LEDGER_AUTHORITY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                    "qmt_batch_id": batch_id,
                    "runtime_id": runtime_id,
                    "broker_called": False,
                },
            )
        existing_ledger_orders = list_order_ledger(batch_id=batch_id)
        if existing_children or existing_ledger_orders:
            raise BrokerSubmitError(
                "MiniQMT event_loop owned retry found broker-side-effect evidence and will not resubmit",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_SIDE_EFFECT_PRESENT",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                    "qmt_batch_id": batch_id,
                    "runtime_id": runtime_id,
                    "child_order_ids": [child.child_order_id for child in existing_children],
                    "qmt_order_ids": [str(order.qmt_order_id) for order in existing_ledger_orders],
                    "broker_called": True,
                },
            )
        get_by_remark = getattr(self.strategy_ledger_repository, "get_order_intent_by_remark", None)
        if not callable(get_by_remark):
            raise BrokerSubmitError(
                "MiniQMT event_loop owned retry requires durable order-remark lookup",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_REMARK_AUTHORITY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                    "qmt_batch_id": batch_id,
                    "broker_called": False,
                },
            )

        results: list[ManagedOrderSubmitResult] = []
        submit_parent_ids: set[str] = set()
        for request in stored_requests:
            parent_id = _parent_id_from_request(request)
            intent = by_parent[parent_id]
            mismatches = _event_loop_owned_parent_intent_mismatches(
                batch_id=batch_id,
                request=request,
                intent=intent,
            )
            remark_intent = get_by_remark(request.account_id, request.order_remark)
            if remark_intent is None or str(getattr(remark_intent, "intent_id", "")) != parent_id:
                mismatches["order_remark_owner"] = {
                    "expected": parent_id,
                    "actual": getattr(remark_intent, "intent_id", None),
                }
            if mismatches:
                raise BrokerSubmitError(
                    "MiniQMT event_loop failed batch is not an exact runtime-owned retry",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_OWNED_RETRY_IDENTITY_MISMATCH",
                        "stage": "MINIQMT_EVENT_LOOP_PREFLIGHT_RESTORE",
                        "qmt_batch_id": batch_id,
                        "parent_intent_id": parent_id,
                        "order_remark": request.order_remark,
                        "mismatches": mismatches,
                        "broker_called": False,
                    },
                )
            preflight = OrderPreflightResult(
                allowed=True,
                errors=(),
                strategy_id=str(getattr(intent, "strategy_id", "")) or None,
                estimated_notional=getattr(intent, "estimated_notional", None) or Decimal("0"),
                estimated_fee=getattr(intent, "estimated_fee", None) or Decimal("0"),
                freeze_amount=Decimal("0"),
                available_cash=None,
            )
            results.append(
                ManagedOrderSubmitResult(
                    False,
                    parent_id,
                    None,
                    "restored exact runtime-owned parent intent; child order remains pending a true tick",
                    preflight,
                    False,
                )
            )
            submit_parent_ids.add(parent_id)

        return MiniQMTEventLoopPreflightResult(
            batch_id=batch_id,
            retry_of_batch_id=batch_id,
            requests=tuple(stored_requests),
            results=tuple(results),
            request_by_parent_intent_id={
                _parent_id_from_request(request): request for request in stored_requests
            },
            submit_parent_intent_ids=frozenset(submit_parent_ids),
        )

    def _event_loop_find_owned_failed_batch_by_remarks(
        self,
        requests: list[ManagedOrderRequest],
    ) -> OrderBatchRecord | None:
        repository = self.strategy_ledger_repository
        get_batch = getattr(repository, "get_order_batch", None)
        get_by_remark = getattr(repository, "get_order_intent_by_remark", None)
        if not callable(get_batch) or not callable(get_by_remark) or not requests:
            return None
        batch_ids: set[str] = set()
        for request in requests:
            if not request.order_remark:
                return None
            intent = get_by_remark(request.account_id, request.order_remark)
            stored_batch_id = str(getattr(intent, "batch_id", "") or "") if intent is not None else ""
            if not stored_batch_id:
                return None
            batch_ids.add(stored_batch_id)
        if len(batch_ids) != 1:
            return None
        batch = get_batch(next(iter(batch_ids)))
        return batch if batch is not None and batch.batch_status == OrderBatchStatus.PREFLIGHT_FAILED else None

    def _event_loop_find_dependent_buy_batch_by_logical_key(
        self,
        requests: list[ManagedOrderRequest],
    ) -> OrderBatchRecord | None:
        repository = self.strategy_ledger_repository
        get_batch = getattr(repository, "get_order_batch", None)
        get_by_remark = getattr(repository, "get_order_intent_by_remark", None)
        if not callable(get_batch) or not callable(get_by_remark):
            return None
        logical_batch_id = _event_loop_logical_batch_id_for_requests(requests)
        if logical_batch_id == _batch_id_for_requests(requests):
            return None
        for request in requests:
            if not request.order_remark:
                continue
            intent = get_by_remark(request.account_id, request.order_remark)
            if intent is None or not getattr(intent, "batch_id", None):
                continue
            batch = get_batch(intent.batch_id)
            if batch is None or batch.batch_status != OrderBatchStatus.PARTIAL:
                continue
            metadata = batch.metadata if isinstance(batch.metadata, dict) else {}
            if not (metadata.get("dependent_buy_deferred") or metadata.get("capacity_residual_skipped")):
                continue
            if _logical_batch_id_for_event_loop_batch(batch) == logical_batch_id:
                return batch
        return None

    def _event_loop_dependent_buy_retry_result(
        self,
        *,
        batch_id: str,
        requests: list[ManagedOrderRequest],
        batch: OrderBatchRecord,
        managed_order_service: QmtManagedOrderService | None,
    ) -> MiniQMTEventLoopPreflightResult | None:
        stored_results = tuple(
            _result_from_dict(item) for item in (batch.result_json or {}).get("results", ()) if isinstance(item, dict)
        )
        if len(stored_results) != len(requests):
            return None
        results = list(stored_results)
        retry_indexes = [
            index
            for index, (request, result) in enumerate(zip(requests, stored_results, strict=True))
            if _is_dependent_buy_proceeds_deferred(request, result.preflight)
        ]
        submit_parent_ids: set[str] = set()
        if retry_indexes:
            retry_requests = [requests[index] for index in retry_indexes]
            retry_preflights = self._event_loop_batch_preflight(
                retry_requests,
                managed_order_service=managed_order_service,
            )
            for relative_index, preflight in enumerate(retry_preflights):
                index = retry_indexes[relative_index]
                request = requests[index]
                parent_id = _parent_id_from_request(request)
                if preflight.allowed:
                    results[index] = ManagedOrderSubmitResult(
                        False,
                        parent_id,
                        None,
                        "event_loop dependent buy retry preflight passed; algo dispatch pending tick trigger",
                        preflight,
                        False,
                    )
                    if parent_id:
                        submit_parent_ids.add(parent_id)
                    continue
                results[index] = ManagedOrderSubmitResult(
                    False,
                    None,
                    None,
                    (
                        "dependent buy still waiting for reconciled sell proceeds"
                        if _is_dependent_buy_proceeds_deferred(request, preflight)
                        else "dependent buy retry preflight failed"
                    ),
                    preflight,
                    False,
                )
        return MiniQMTEventLoopPreflightResult(
            batch_id=batch_id,
            retry_of_batch_id=batch_id,
            requests=tuple(requests),
            results=tuple(results),
            request_by_parent_intent_id=_request_by_parent({}, requests),
            submit_parent_intent_ids=frozenset(submit_parent_ids),
        )

    def _event_loop_results_with_unsubmitted_residuals(
        self,
        *,
        requests: tuple[ManagedOrderRequest, ...],
        results: tuple[ManagedOrderSubmitResult, ...],
    ) -> tuple[ManagedOrderSubmitResult, ...]:
        if len(results) == len(requests):
            return results
        results_by_parent = {
            parent_id: result
            for request, result in zip(requests, results, strict=False)
            if (parent_id := _parent_id_from_request(request))
        }
        ordered: list[ManagedOrderSubmitResult] = []
        for request in requests:
            parent_id = _parent_id_from_request(request)
            result = results_by_parent.get(parent_id) if parent_id else None
            if result is not None:
                ordered.append(result)
                continue
            preflight = self._event_loop_preview_order(request, managed_order_service=None)
            ordered.append(
                ManagedOrderSubmitResult(
                    False,
                    None,
                    None,
                    "event_loop residual intent was not submitted to broker",
                    preflight,
                    False,
                )
            )
        return tuple(ordered)

    def _upsert_event_loop_batch_record(
        self,
        *,
        batch_id: str,
        requests: tuple[ManagedOrderRequest, ...],
        results: tuple[ManagedOrderSubmitResult, ...],
        runtime_evidence: MiniQMTRuntimeEvidence,
        source: str,
    ) -> None:
        repository = self.strategy_ledger_repository
        upsert = getattr(repository, "upsert_order_batch", None)
        if not callable(upsert):
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires qmt_strategy order batch persistence",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_BATCH_REPOSITORY_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_BATCH_PERSIST",
                    "batch_id": batch_id,
                },
            )
        now = datetime.now(UTC)
        existing = getattr(repository, "get_order_batch", lambda _batch_id: None)(batch_id)
        created_at = existing.created_at if existing is not None else now
        status = _event_loop_batch_status(requests, results)
        metadata = _event_loop_batch_metadata(
            requests=requests,
            results=results,
            runtime_evidence=runtime_evidence,
            source=source,
        )
        if (
            existing is not None
            and isinstance(existing.metadata, dict)
            and existing.metadata.get("dependent_buy_deferred")
        ):
            metadata["dependent_buy_retry"] = True
        upsert(
            OrderBatchRecord(
                batch_id=batch_id,
                strategy_id=_single_strategy_id(results),
                account_id=requests[0].account_id if requests else "",
                mode=requests[0].mode if requests else "SIM",
                batch_status=status,
                request_json={"orders": [_request_signature(request) for request in requests]},
                result_json={
                    "results": [result.to_dict() for result in results],
                    "compensation_actions": [],
                    "compensation_hint": (
                        "partial event_loop broker submission; inspect accepted qmt_order_id values"
                        if _event_loop_compensation_required(status, requests, results)
                        else None
                    ),
                    "runtime_evidence": runtime_evidence.to_dict(),
                },
                metadata=metadata,
                created_at=created_at,
                submitted_at=now
                if status
                in {
                    OrderBatchStatus.SUBMITTING,
                    OrderBatchStatus.SUCCEEDED,
                    OrderBatchStatus.PARTIAL,
                    OrderBatchStatus.FAILED,
                }
                else None,
                completed_at=now,
            )
        )

    def execute_paper_vnpy_intent(
        self,
        *,
        portfolio: Any,
        run: Any,
        trade_date: date,
        intent: OrderIntent,
        broker: BrokerBackend,
        execution_policy_context: dict[str, Any],
        runtime_config_hash: str,
        account_group_id: str,
        strategy_slot_id: str,
        quote_provider: Callable[[str], dict[str, Any] | None] | None = None,
        source: str = "paper_v2_vnpy_miniqmt",
    ) -> MiniQMTAlgoExecutionResult:
        self._reject_event_loop_compiler_lifecycle(source=source, operation="execute_paper_vnpy_intent")

    def _runtime(
        self,
        *,
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        gateway: MiniQMTGateway,
        runtime_id: str | None = None,
        account_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MiniQMTExecutionRuntime:
        return MiniQMTExecutionRuntime(
            config=MiniQMTExecutionRuntimeConfig(
                runtime_id=runtime_id
                or f"mqrt_{_short_hash([account_group_id, trade_date.isoformat(), runtime_config_hash])}",
                account_group_id=account_group_id,
                trade_date=trade_date,
                runtime_config_hash=runtime_config_hash,
                metadata=dict(metadata or {}),
            ),
            repository=self.repository,
            gateway=gateway,
            strategy_ledger_repository=(
                self.strategy_ledger_repository if self.runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP else None
            ),
            account_id=account_id or account_group_id,
        )

    def _reject_event_loop_compiler_lifecycle(self, *, source: str, operation: str, **context: Any) -> None:
        if self.runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP:
            raise BrokerSubmitError(
                "MiniQMT event_loop runtime requires real gateway callbacks and refuses compiler-style "
                "managed runtime lifecycle; reason_code=MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS",
                    "stage": "MINIQMT_COMPILER_LIFECYCLE_REJECTED",
                    "source": source,
                    "operation": operation,
                    **context,
                },
            )
        raise BrokerSubmitError(
            "MiniQMT SIM compiler runtime lifecycle is retired; reason_code=MINIQMT_SIM_COMPILER_ROUTE_RETIRED",
            context={
                "reason_code": "MINIQMT_SIM_COMPILER_ROUTE_RETIRED",
                "stage": "MINIQMT_COMPILER_LIFECYCLE_REJECTED",
                "source": source,
                "operation": operation,
                "runtime_kind": self.runtime_kind.value,
                "allowed_runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
                **context,
            },
        )

    def evidence_for_runtime(self, runtime_id: str, *, source: str) -> MiniQMTRuntimeEvidence:
        runtime_record = self.repository.get_runtime(runtime_id)
        if runtime_record is None:
            raise BrokerSubmitError("MiniQMT runtime evidence is missing", context={"runtime_id": runtime_id})
        child_orders = tuple(self.repository.list_child_orders(runtime_id, active_only=False))
        algo_instances = tuple(self.repository.list_algo_instances(runtime_id, active_only=False))
        return MiniQMTRuntimeEvidence(
            runtime_id=runtime_id,
            runtime_owner=RUNTIME_OWNER,
            account_group_id=runtime_record.account_group_id,
            trade_date=runtime_record.trade_date,
            event_count=len(self.repository.list_events(runtime_id)),
            algo_instance_ids=tuple(item.algo_instance_id for item in algo_instances),
            child_order_ids=tuple(item.child_order_id for item in child_orders),
            submitted_child_count=_submitted_child_count(child_orders),
            rejected_child_count=sum(1 for item in child_orders if item.status == MiniQMTChildOrderStatus.REJECTED),
            active_algo_count=sum(1 for item in algo_instances if item.status == MiniQMTAlgoInstanceStatus.ACTIVE),
            completed_algo_count=sum(
                1 for item in algo_instances if item.status == MiniQMTAlgoInstanceStatus.COMPLETED
            ),
            pending_algo_count=_pending_algo_count(algo_instances, child_orders),
            source=source,
        )

    def execute_operator_command(
        self,
        *,
        account_group_id: str,
        trade_date: date,
        runtime_config_hash: str,
        command_id: str,
        command_type: str,
        reason: str,
        gateway: MiniQMTGateway,
        runtime_id: str | None = None,
        payload: dict[str, Any] | None = None,
        source: str = "operator_command",
    ) -> tuple[MiniQMTOperatorCommandResult, MiniQMTRuntimeEvidence]:
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=gateway,
            metadata={"source": source},
        )
        runtime.start()
        result = runtime.execute_operator_command(
            command_id=command_id,
            command_type=command_type,
            reason=reason,
            payload=dict(payload or {}),
        )
        return result, self._evidence(runtime, source=source)

    def _evidence(self, runtime: MiniQMTExecutionRuntime, *, source: str) -> MiniQMTRuntimeEvidence:
        runtime_id = runtime.config.runtime_id
        child_orders = tuple(self.repository.list_child_orders(runtime_id, active_only=False))
        algo_instances = tuple(self.repository.list_algo_instances(runtime_id, active_only=False))
        return MiniQMTRuntimeEvidence(
            runtime_id=runtime_id,
            runtime_owner=RUNTIME_OWNER,
            account_group_id=runtime.config.account_group_id,
            trade_date=runtime.config.trade_date,
            event_count=len(self.repository.list_events(runtime_id)),
            algo_instance_ids=tuple(item.algo_instance_id for item in algo_instances),
            child_order_ids=tuple(item.child_order_id for item in child_orders),
            submitted_child_count=_submitted_child_count(child_orders),
            rejected_child_count=sum(1 for item in child_orders if item.status == MiniQMTChildOrderStatus.REJECTED),
            active_algo_count=sum(1 for item in algo_instances if item.status == MiniQMTAlgoInstanceStatus.ACTIVE),
            completed_algo_count=sum(
                1 for item in algo_instances if item.status == MiniQMTAlgoInstanceStatus.COMPLETED
            ),
            pending_algo_count=_pending_algo_count(algo_instances, child_orders),
            source=source,
        )

    def _sync_managed_child_result(
        self,
        *,
        runtime_id: str,
        child_order_id: str,
        managed_result: ManagedOrderSubmitResult,
        ledger_order: OrderLedgerRecord | None = None,
        source: str,
    ) -> MiniQMTChildOrder | None:
        child = _find_child_order(self.repository, runtime_id=runtime_id, child_order_id=child_order_id)
        if child is None:
            return None
        status = _runtime_status_from_managed_result(managed_result, ledger_order=ledger_order)
        updated_child = child.model_copy(
            update={
                "status": status,
                "broker_order_id": managed_result.qmt_order_id or child.broker_order_id,
                "submitted_at": datetime.now(UTC)
                if managed_result.success and child.submitted_at is None
                else child.submitted_at,
                "metadata": {
                    **dict(child.metadata),
                    "source": source,
                    "managed_order_result": managed_result.to_dict(),
                    "broker_called": managed_result.broker_called,
                    "broker_synced_child_status": status.value,
                    **(
                        {"broker_order_ledger": _ledger_order_payload(ledger_order)} if ledger_order is not None else {}
                    ),
                },
            }
        )
        stored = self.repository.upsert_child_order(updated_child)
        self._append_managed_gateway_sync_event(
            runtime_id=runtime_id,
            child_order_id=child_order_id,
            managed_result=managed_result,
            source=source,
        )
        return stored

    def _append_managed_gateway_sync_event(
        self,
        *,
        runtime_id: str,
        child_order_id: str,
        managed_result: ManagedOrderSubmitResult,
        source: str,
    ) -> MiniQMTExecutionEvent:
        return self.repository.append_event(
            MiniQMTExecutionEvent(
                runtime_id=runtime_id,
                sequence=self.repository.next_event_sequence(runtime_id),
                event_type=(
                    MiniQMTExecutionEventType.ORDER_EVENT
                    if managed_result.success
                    else MiniQMTExecutionEventType.CHILD_ORDER_REJECTED
                ),
                source="gateway",
                payload={
                    "child_order_id": child_order_id,
                    "broker_order_id": managed_result.qmt_order_id,
                    "accepted": managed_result.success,
                    "message": managed_result.broker_message,
                    "broker_called": managed_result.broker_called,
                    "managed_gateway_sync": True,
                    "source": source,
                },
            )
        )


class _PreviewOnlyRuntimeGateway:
    def connect(self, *, runtime_id: str) -> None:  # noqa: ARG002
        return None

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def submit_child_order(self, order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck:  # noqa: ARG002
        return MiniQMTGatewayOrderAck(False, None, "preview gateway cannot submit child orders")

    def cancel_child_order(self, order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck:  # noqa: ARG002
        return MiniQMTGatewayCancelAck(False, order.broker_order_id, "preview gateway cannot cancel child orders")


class _ManagedOrderRequestRuntimeGateway:
    """Runtime gateway that owns the legacy managed-order adapter boundary."""

    def __init__(self, *, managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest]) -> None:
        self.managed_request_factory = managed_request_factory
        self.connected_runtime_ids: list[str] = []
        self.requests: list[ManagedOrderRequest] = []
        self.child_order_id_by_order_remark: dict[str, str] = {}

    @classmethod
    def from_requests(
        cls,
        requests: tuple[ManagedOrderRequest, ...] | list[ManagedOrderRequest],
    ) -> "_ManagedOrderRequestRuntimeGateway":
        materialized = tuple(requests)
        gateway = cls(managed_request_factory=lambda _child, index: materialized[index - 1])
        gateway.requests.extend(materialized)
        return gateway

    def connect(self, *, runtime_id: str) -> None:
        self.connected_runtime_ids.append(runtime_id)

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def submit_child_order(self, order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck:
        request = self.managed_request_factory(order, len(self.requests) + 1)
        self.requests.append(request)
        self.child_order_id_by_order_remark[request.order_remark] = order.child_order_id
        return MiniQMTGatewayOrderAck(
            accepted=True,
            broker_order_id=f"managed_request_{request.order_remark}",
            message="managed order request generated by canonical MiniQMT runtime",
            raw={
                "gateway": "managed_order_request_runtime_gateway",
                "broker_called": False,
                "order_remark": request.order_remark,
            },
        )

    def submit_managed_batch(self, *, order_service: QmtManagedOrderService) -> ManagedBatchSubmitResult:
        return order_service.submit_batch(list(self.requests))

    def cancel_child_order(self, order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck:  # noqa: ARG002
        return MiniQMTGatewayCancelAck(
            False,
            order.broker_order_id,
            "managed order request is not a broker order before submit_batch",
            {"gateway": "managed_order_request_runtime_gateway", "reason": reason},
        )


def _managed_vnpy_child_metadata(
    *,
    intent: OrderIntent,
    trade_date: date,
    source: str,
    execution_policy_context: dict[str, Any],
) -> dict[str, Any]:
    return {
        "source": source,
        "managed_parent_intent_id": intent.intent_id,
        "account_id": str(intent.metadata.get("account_id") or intent.metadata.get("broker_account_id") or "").strip(),
        "package_id": intent.package_id,
        "portfolio_id": intent.portfolio_id,
        "target_weight": intent.metadata.get("target_weight"),
        "order_type": OrderType.LIMIT.value,
        "limit_price": intent.limit_price,
        "target_trade_date": trade_date.isoformat(),
        "parent_intent_metadata": dict(intent.metadata or {}),
        "execution_policy_id": execution_policy_context.get("validated_execution_policy_id"),
        "execution_policy_sha256": execution_policy_context.get("policy_sha256"),
    }


def _event_loop_child_metadata(*, intent: OrderIntent, trade_date: date, source: str, index: int) -> dict[str, Any]:
    prefix = str(intent.metadata.get("order_remark_prefix") or "eventloop").strip()[:20] or "eventloop"
    order_remark = str(intent.metadata.get("order_remark") or f"{prefix}-{_short_hash([intent.intent_id, index])[:12]}")
    strategy_name = str(
        intent.metadata.get("strategy_name") or intent.metadata.get("strategy_id") or intent.portfolio_id
    )
    return {
        "source": source,
        "managed_parent_intent_id": intent.intent_id,
        "package_id": intent.package_id,
        "portfolio_id": intent.portfolio_id,
        "strategy_id": str(intent.metadata.get("strategy_id") or intent.portfolio_id),
        "strategy_name": strategy_name,
        "order_remark": order_remark,
        "target_weight": intent.metadata.get("target_weight"),
        "order_type": OrderType.LIMIT.value,
        "limit_price": intent.limit_price,
        "target_trade_date": trade_date.isoformat(),
        "parent_intent_metadata": dict(intent.metadata or {}),
        "event_loop_submit": True,
    }


def _managed_request_signature(request: ManagedOrderRequest) -> dict[str, Any]:
    return {
        "account_id": request.account_id,
        "strategy_name": request.strategy_name,
        "symbol": request.symbol,
        "side": request.side,
        "order_type": int(request.order_type),
        "quantity": int(request.quantity),
        "price_type": int(request.price_type),
        "price": str(request.price),
        "order_remark": request.order_remark,
        "trade_date": request.trade_date.isoformat(),
        "mode": request.mode,
        "package_id": request.package_id,
        "selection_run_id": request.selection_run_id,
        "target_weight": str(request.target_weight) if request.target_weight is not None else None,
        "metadata": _json_safe(request.metadata),
    }


def _submit_error_message(error: dict[str, Any]) -> str:
    context = error.get("context") if isinstance(error, dict) else None
    if isinstance(context, dict):
        for key in ("message", "reason"):
            value = context.get(key)
            if value:
                return str(value)
    return str(error.get("message") or "MiniQMT child order submit failed")


def _rejected_status_from_error(error: dict[str, Any], *, child_order_id: str) -> OrderHandleStatus:
    return OrderHandleStatus(
        handle_id=child_order_id,
        state="rejected",
        filled_quantity=0,
        avg_fill_price=None,
        last_event_at=datetime.now(UTC),
        rejection_reason=str(error.get("message") or "MiniQMT submit failed"),
        raw_status="submit_error",
        status_msg=str(error.get("message") or "MiniQMT submit failed"),
        raw=dict(error),
    )


def _limit_price_for_runtime(
    *,
    intent: OrderIntent,
    quote_provider: Callable[[str], dict[str, Any] | None] | None,
    algo_config: dict[str, Any] | None = None,
) -> float:
    quote = quote_provider(intent.symbol) if quote_provider is not None else None
    if quote:
        return _marketable_limit_price(
            intent=intent,
            tick_payload=quote,
            algo_config=algo_config or {},
            stage="MINIQMT_RUNTIME_MARKETABLE_LIMIT",
        )
    if intent.limit_price is not None:
        return float(intent.limit_price)
    raise BrokerSubmitError("MiniQMTExecutionRuntime vn.py client requires quote or limit_price")


def _limit_price_for_event_loop(
    *,
    intent: OrderIntent,
    tick_payload: dict[str, Any],
    algo_config: dict[str, Any] | None = None,
) -> float:
    return _marketable_limit_price(
        intent=intent,
        tick_payload=tick_payload,
        algo_config=algo_config or {},
        stage="MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT",
    )


def _b0_quote_v2_initial_limit_price(intent: OrderIntent) -> float:
    metadata = dict(intent.metadata or {})
    for candidate in (
        intent.limit_price,
        metadata.get("target_reference_price"),
        metadata.get("reference_price"),
    ):
        if candidate is None:
            continue
        try:
            value = float(candidate)
        except (TypeError, ValueError) as exc:
            raise BrokerSubmitError(
                "B0_QUOTE_V2 parent reference price is invalid",
                context={
                    "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                    "stage": "ADAPTER",
                    "parent_intent_id": intent.intent_id,
                    "broker_called": False,
                },
            ) from exc
        if math.isfinite(value) and value > 0:
            return value
    raise BrokerSubmitError(
        "B0_QUOTE_V2 parent requires an immutable positive reference or limit price",
        context={
            "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
            "stage": "ADAPTER",
            "parent_intent_id": intent.intent_id,
            "broker_called": False,
        },
    )


def _b0_quote_v2_assignments(
    *,
    policy_context: Mapping[str, Any],
    parent_intent_ids: set[str],
) -> tuple[B0QuoteV2RevisionV1 | None, dict[str, ParentQuoteControlAssignmentV1]]:
    raw = policy_context.get("quote_control")
    if raw is None:
        return None, {}
    if not isinstance(raw, Mapping) or set(raw) != {"binding", "revision", "assignments"}:
        raise BrokerSubmitError(
            "execution plan quote_control payload is not exact",
            context={
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "stage": "ADAPTER",
                "broker_called": False,
            },
        )
    binding_payload = raw.get("binding")
    if not isinstance(binding_payload, Mapping):
        raise BrokerSubmitError(
            "execution plan quote_control binding is missing",
            context={
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "stage": "ADAPTER",
                "broker_called": False,
            },
        )
    binding = QuoteControlBindingV1.from_binding_config({"miniqmt_quote_control": binding_payload})
    if binding.control_revision.value == "LEGACY_B0":
        if raw.get("revision") is not None:
            raise BrokerSubmitError(
                "LEGACY_B0 quote_control cannot carry a B0_QUOTE_V2 revision",
                context={
                    "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                    "stage": "ADAPTER",
                    "broker_called": False,
                },
            )
        return None, {}
    revision_payload = raw.get("revision")
    assignments_payload = raw.get("assignments")
    if not isinstance(revision_payload, Mapping) or not isinstance(assignments_payload, list):
        raise BrokerSubmitError(
            "B0_QUOTE_V2 quote_control requires frozen revision and assignments",
            context={
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "stage": "ADAPTER",
                "broker_called": False,
            },
        )
    revision = B0QuoteV2RevisionV1.from_payload(revision_payload)
    manifest = source_build_manifest()
    expected_hashes = {
        "adapter_sha256": manifest.adapter_sha256,
        "code_sha256": manifest.code_sha256,
        "evidence_schema_sha256": manifest.evidence_schema_sha256,
    }
    conflicts = {
        field_name: {"expected": expected, "received": getattr(revision, field_name)}
        for field_name, expected in expected_hashes.items()
        if getattr(revision, field_name) != expected
    }
    if conflicts:
        raise BrokerSubmitError(
            "B0_QUOTE_V2 frozen build/schema manifest differs from runtime readback",
            context={
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "stage": "ADAPTER",
                "manifest_conflicts": conflicts,
                "broker_called": False,
            },
        )
    assignments: dict[str, ParentQuoteControlAssignmentV1] = {}
    for payload in assignments_payload:
        if not isinstance(payload, Mapping):
            raise BrokerSubmitError(
                "B0_QUOTE_V2 assignment list contains a non-object",
                context={
                    "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                    "stage": "ADAPTER",
                    "broker_called": False,
                },
            )
        assignment = ParentQuoteControlAssignmentV1.from_plan_payload(payload, revision=revision)
        if assignment.parent_intent_id in assignments:
            raise BrokerSubmitError(
                "B0_QUOTE_V2 plan contains duplicate parent assignments",
                context={
                    "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                    "stage": "ADAPTER",
                    "parent_intent_id": assignment.parent_intent_id,
                    "broker_called": False,
                },
            )
        assignments[assignment.parent_intent_id] = assignment
    if set(assignments) != parent_intent_ids:
        raise BrokerSubmitError(
            "B0_QUOTE_V2 assignment parent set differs from runtime parent set",
            context={
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "stage": "ADAPTER",
                "missing_parent_intent_ids": sorted(parent_intent_ids - set(assignments)),
                "unknown_parent_intent_ids": sorted(set(assignments) - parent_intent_ids),
                "broker_called": False,
            },
        )
    return revision, assignments


def _marketable_limit_price(
    *,
    intent: OrderIntent,
    tick_payload: dict[str, Any],
    algo_config: dict[str, Any],
    stage: str,
    cross_ticks_override: int | None = None,
) -> float:
    """Return a valid, protected marketable-limit price for an A-share L1 quote.

    The protection cap deliberately wins over immediacy: a quote outside the
    configured band is converted into a passive cap so this tick cannot submit
    a bad order; the next broker tick will re-evaluate it.
    """
    cross_ticks = (
        _positive_int_config(
            algo_config,
            "marketable_limit_cross_ticks",
            DEFAULT_MARKETABLE_LIMIT_CROSS_TICKS,
        )
        if cross_ticks_override is None
        else cross_ticks_override
    )
    protection_band_pct = _positive_float_config(
        algo_config,
        "marketable_limit_protection_band_pct",
        DEFAULT_MARKETABLE_LIMIT_PROTECTION_BAND_PCT,
    )
    tick_size = _positive_tick_size(tick_payload, algo_config)
    reference_price = _execution_reference_price(intent=intent, tick_payload=tick_payload)
    opposite_price = _positive_quote_price(
        tick_payload.get("ask_price_1") if intent.side == OrderSide.BUY else tick_payload.get("bid_price_1"),
        intent=intent,
        stage=stage,
    )
    candidate = (
        opposite_price + tick_size * cross_ticks
        if intent.side == OrderSide.BUY
        else opposite_price - tick_size * cross_ticks
    )
    protected_cap = (
        reference_price * (1.0 + protection_band_pct)
        if intent.side == OrderSide.BUY
        else reference_price * (1.0 - protection_band_pct)
    )
    out_of_band = candidate > protected_cap if intent.side == OrderSide.BUY else candidate < protected_cap
    price = protected_cap if out_of_band else candidate
    price = _apply_exchange_price_bounds(price=price, side=intent.side, tick_payload=tick_payload)
    price = _round_to_a_share_tick(price=price, tick_size=tick_size, side=intent.side)
    if price <= 0 or not math.isfinite(price):
        raise BrokerSubmitError(
            "MiniQMT marketable-limit price is invalid after A-share constraints",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT_PRICE_INVALID",
                "stage": stage,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "price": price,
                "tick_size": tick_size,
            },
        )
    if out_of_band:
        LOGGER.warning(
            "reason_code=MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT_PROTECTION_BAND stage=%s intent_id=%s symbol=%s side=%s reference_price=%s opposite_price=%s protected_cap=%s; skip aggressive crossing until next tick",
            stage,
            intent.intent_id,
            intent.symbol,
            intent.side.value,
            reference_price,
            opposite_price,
            price,
        )
    return price


def _execution_reference_price(*, intent: OrderIntent, tick_payload: dict[str, Any]) -> float:
    metadata = dict(intent.metadata or {})
    for value in (
        metadata.get("reference_price"),
        metadata.get("pre_close"),
        intent.limit_price,
        tick_payload.get("reference_price"),
        tick_payload.get("pre_close"),
        tick_payload.get("price"),
        tick_payload.get("last_price"),
    ):
        try:
            price = float(value)
        except (TypeError, ValueError):
            continue
        if price > 0 and math.isfinite(price):
            return price
    raise BrokerSubmitError(
        "MiniQMT marketable-limit requires a positive reference price",
        context={
            "reason_code": "MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT_REFERENCE_PRICE_MISSING",
            "stage": "MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT",
            "intent_id": intent.intent_id,
            "symbol": intent.symbol,
            "side": intent.side.value,
        },
    )


def _positive_quote_price(value: Any, *, intent: OrderIntent, stage: str) -> float:
    try:
        price = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError(
            "MiniQMT marketable-limit requires a positive opposite-side L1 price",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT_QUOTE_INVALID",
                "stage": stage,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
            },
        ) from exc
    if price <= 0 or not math.isfinite(price):
        raise BrokerSubmitError(
            "MiniQMT marketable-limit requires a positive opposite-side L1 price",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_MARKETABLE_LIMIT_QUOTE_INVALID",
                "stage": stage,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "quote_price": price,
            },
        )
    return price


def _positive_int_config(config: dict[str, Any], key: str, default: int) -> int:
    value = config.get(key, default)
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError(f"MiniQMT execution policy {key} must be a positive integer") from exc
    if parsed < 1:
        raise BrokerSubmitError(f"MiniQMT execution policy {key} must be a positive integer")
    return parsed


def _positive_float_config(config: dict[str, Any], key: str, default: float) -> float:
    value = config.get(key, default)
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError(f"MiniQMT execution policy {key} must be a positive finite number") from exc
    if parsed <= 0 or not math.isfinite(parsed):
        raise BrokerSubmitError(f"MiniQMT execution policy {key} must be a positive finite number")
    return parsed


def _positive_tick_size(tick_payload: dict[str, Any], algo_config: dict[str, Any]) -> float:
    for value in (algo_config.get("price_tick"), tick_payload.get("price_tick"), tick_payload.get("tick_size")):
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        if parsed > 0 and math.isfinite(parsed):
            return parsed
    return DEFAULT_A_SHARE_PRICE_TICK


def _apply_exchange_price_bounds(*, price: float, side: OrderSide, tick_payload: dict[str, Any]) -> float:
    upper_keys = ("up_limit", "upper_limit", "limit_up", "high_limit")
    lower_keys = ("down_limit", "lower_limit", "limit_down", "low_limit")
    keys = upper_keys if side == OrderSide.BUY else lower_keys
    for key in keys:
        try:
            bound = float(tick_payload.get(key))
        except (TypeError, ValueError):
            continue
        if bound > 0 and math.isfinite(bound):
            return min(price, bound) if side == OrderSide.BUY else max(price, bound)
    return price


def _round_to_a_share_tick(*, price: float, tick_size: float, side: OrderSide) -> float:
    units = price / tick_size
    rounded_units = math.ceil(units - 1e-9) if side == OrderSide.BUY else math.floor(units + 1e-9)
    return round(rounded_units * tick_size, 8)


def _required_event_loop_tick_payload(
    *,
    intent: OrderIntent,
    quote_provider: Callable[[str], dict[str, Any] | None] | None,
    qmt_client: Any,
    source: str,
) -> dict[str, Any]:
    quote_origin = ""
    quote = quote_provider(intent.symbol) if quote_provider is not None else None
    if isinstance(quote, dict):
        quote_origin = "quote_provider"
        quote_source = str(quote.get("source") or quote.get("quote_source") or "").strip()
        if not quote_source:
            raise BrokerSubmitError(
                "MiniQMT event_loop route refuses quote_provider payload without broker quote source",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE_MISSING",
                    "source": source,
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "required_quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
                },
            )
        if quote_source != MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE:
            raise BrokerSubmitError(
                "MiniQMT event_loop route refuses non-broker quote source",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE_INVALID",
                    "source": source,
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "quote_source": quote_source,
                    "required_quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
                },
            )
    if quote is None:
        query_quote = getattr(qmt_client, "query_quote", None)
        if callable(query_quote):
            quote = query_quote(intent.symbol)
            if isinstance(quote, dict):
                quote_origin = "qmt_client.query_quote"
    if quote is None:
        get_full_tick = getattr(qmt_client, "get_full_tick", None)
        if callable(get_full_tick):
            try:
                payload = get_full_tick([intent.symbol])
            except Exception as exc:  # noqa: BLE001
                raise BrokerSubmitError(
                    "MiniQMT event_loop broker quote fetch failed",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_FETCH_FAILED",
                        "source": source,
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                ) from exc
            if not isinstance(payload, dict):
                raise BrokerSubmitError(
                    "MiniQMT event_loop broker quote payload is invalid",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_PAYLOAD_INVALID",
                        "source": source,
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
                        "payload_type": type(payload).__name__,
                    },
                )
            row = payload.get(intent.symbol)
            if row is None:
                raw_code = str(intent.symbol).split(".")[0]
                for key, value in payload.items():
                    if str(key).split(".")[0] == raw_code:
                        row = value
                        break
            if isinstance(row, dict):
                quote = row
                quote_origin = "qmt_client.get_full_tick"
    if not isinstance(quote, dict):
        raise BrokerSubmitError(
            "MiniQMT event_loop submit requires broker quote before routing parent intent",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_MISSING",
                "source": source,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
            },
        )
    payload = dict(quote)
    raw_quote_source = str(payload.get("source") or payload.get("quote_source") or "").strip()
    if raw_quote_source and raw_quote_source != MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE:
        raise BrokerSubmitError(
            "MiniQMT event_loop route refuses non-broker quote source",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE_INVALID",
                "source": source,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "quote_source": raw_quote_source,
                "required_quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
            },
        )
    if quote_origin == "quote_provider" and not raw_quote_source:
        raise BrokerSubmitError(
            "MiniQMT event_loop route refuses quote_provider payload without broker quote source",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE_MISSING",
                "source": source,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "required_quote_source": MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE,
            },
        )

    def _first_value(*keys: str) -> Any:
        for key in keys:
            value = payload.get(key)
            if value is not None:
                return value
        return None

    def _first_level(value: Any) -> Any:
        if isinstance(value, (list, tuple)) and value:
            return value[0]
        return value

    payload.setdefault("symbol", intent.symbol)
    payload.setdefault("stock_code", intent.symbol)
    payload.setdefault("source", MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE)
    payload.setdefault("quote_source", MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE)
    price = _first_value("price", "last_price", "lastPrice", "close")
    ask = _first_value("ask_price_1", "askPrice1", "ask_price", "ask")
    bid = _first_value("bid_price_1", "bidPrice1", "bid_price", "bid")
    if ask is None:
        ask = _first_level(payload.get("askPrice"))
    if bid is None:
        bid = _first_level(payload.get("bidPrice"))
    ask_volume = _first_value("ask_volume_1", "askVolume1", "askVol1", "ask_volume", "askVolume")
    bid_volume = _first_value("bid_volume_1", "bidVolume1", "bidVol1", "bid_volume", "bidVolume")
    if ask_volume is None:
        ask_volume = _first_level(payload.get("askVol"))
    if bid_volume is None:
        bid_volume = _first_level(payload.get("bidVol"))
    if price is None:
        price = ask if intent.side == OrderSide.BUY else bid
    if ask is not None:
        payload["ask_price_1"] = ask
    if bid is not None:
        payload["bid_price_1"] = bid
    if ask_volume is not None:
        payload["ask_volume_1"] = ask_volume
    if bid_volume is not None:
        payload["bid_volume_1"] = bid_volume
    payload["price"] = price
    try:
        if float(payload["price"]) <= 0:
            raise ValueError("non-positive")
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError(
            "MiniQMT event_loop broker quote has invalid price",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_PRICE_INVALID",
                "source": source,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "quote_source": payload.get("quote_source"),
            },
        ) from exc
    required_depth = (
        ("ask_price_1", payload.get("ask_price_1"), "ask_volume_1", payload.get("ask_volume_1"))
        if intent.side == OrderSide.BUY
        else ("bid_price_1", payload.get("bid_price_1"), "bid_volume_1", payload.get("bid_volume_1"))
    )
    price_field, depth_price, volume_field, depth_volume = required_depth
    if depth_price is None or depth_volume is None:
        raise BrokerSubmitError(
            "MiniQMT event_loop broker quote is missing required L1 depth for runtime tick",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_DEPTH_MISSING",
                "source": source,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "missing_fields": [
                    field
                    for field, value in ((price_field, depth_price), (volume_field, depth_volume))
                    if value is None
                ],
                "quote_source": payload.get("quote_source"),
            },
        )
    try:
        if float(depth_price) <= 0:
            raise ValueError(f"{price_field} non-positive")
        if int(depth_volume) < 0:
            raise ValueError(f"{volume_field} negative")
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError(
            "MiniQMT event_loop broker quote has invalid L1 depth for runtime tick",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_BROKER_QUOTE_DEPTH_INVALID",
                "source": source,
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                price_field: depth_price,
                volume_field: depth_volume,
                "quote_source": payload.get("quote_source"),
            },
        ) from exc
    return payload


def _tick_payload_for_runtime(
    *,
    intent: OrderIntent,
    quote_provider: Callable[[str], dict[str, Any] | None] | None,
) -> dict[str, Any]:
    quote = quote_provider(intent.symbol) if quote_provider is not None else None
    if quote:
        payload = dict(quote)
        payload.setdefault("symbol", intent.symbol)
        payload.setdefault(
            "price", payload.get("last_price") or payload.get("ask_price_1") or payload.get("bid_price_1")
        )
        return payload
    price = float(intent.limit_price or 0.0)
    if price <= 0:
        raise BrokerSubmitError("MiniQMTExecutionRuntime vn.py client requires quote or positive limit_price")
    return {
        "symbol": intent.symbol,
        "price": price,
        "bid_price_1": price,
        "bid_volume_1": int(intent.quantity),
        "ask_price_1": price,
        "ask_volume_1": int(intent.quantity),
        "source": "runtime_synthetic_limit_quote",
    }


def _managed_request_from_event_loop_child(
    child: MiniQMTChildOrder,
    *,
    index: int,
    trade_date: date,
) -> ManagedOrderRequest:
    metadata = dict(child.metadata or {})
    parent_metadata = dict(metadata.get("parent_intent_metadata") or {})
    order_type = BUY_ORDER_TYPE if child.side == OrderSide.BUY else SELL_ORDER_TYPE
    account_id = str(metadata.get("account_id") or "").strip()
    strategy_name = str(metadata.get("strategy_name") or metadata.get("strategy_id") or child.strategy_slot_id).strip()
    if not account_id:
        account_id = str(metadata.get("broker_account_id") or metadata.get("account_group_id") or "").strip()
    if not account_id:
        raise BrokerSubmitError(
            "MiniQMT event_loop managed request requires account_id",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_ACCOUNT_ID_MISSING",
                "stage": "MINIQMT_EVENT_LOOP_REQUEST_BUILD",
                "child_order_id": child.child_order_id,
                "parent_intent_id": child.parent_intent_id,
            },
        )
    return ManagedOrderRequest(
        account_id=account_id,
        strategy_name=strategy_name,
        symbol=child.symbol,
        side=child.side.value,
        order_type=order_type,
        quantity=int(child.quantity),
        price_type=int(metadata.get("price_type") or child.price_type),
        price=Decimal(str(child.price or 0)),
        order_remark=str(metadata.get("order_remark") or child.child_order_id),
        trade_date=trade_date,
        mode=str(metadata.get("mode") or "SIM").strip().upper(),
        package_id=_optional_str(metadata.get("package_id") or parent_metadata.get("package_id")),
        selection_run_id=_optional_str(metadata.get("selection_run_id") or parent_metadata.get("selection_run_id")),
        target_weight=Decimal(str(metadata.get("target_weight")))
        if metadata.get("target_weight") is not None
        else None,
        metadata={
            **parent_metadata,
            **metadata,
            "runtime_child_order_id": child.child_order_id,
            "runtime_algo_instance_id": child.algo_instance_id,
            "runtime_parent_intent_id": child.parent_intent_id,
            "event_loop_request_index": index,
        },
    )


def _request_by_parent(
    parent_by_id: dict[str, OrderIntent],
    requests: list[ManagedOrderRequest],
) -> dict[str, ManagedOrderRequest]:
    del parent_by_id
    result: dict[str, ManagedOrderRequest] = {}
    for request in requests:
        parent_id = str(
            request.metadata.get("runtime_parent_intent_id") or request.metadata.get("execution_plan_intent_id") or ""
        ).strip()
        if parent_id:
            result[parent_id] = request
    return result


def _parent_id_from_request(request: ManagedOrderRequest) -> str:
    return str(
        request.metadata.get("runtime_parent_intent_id") or request.metadata.get("execution_plan_intent_id") or ""
    ).strip()


def _event_loop_requests_from_batch(batch: OrderBatchRecord) -> list[ManagedOrderRequest]:
    orders = batch.request_json.get("orders") if isinstance(batch.request_json, dict) else None
    if not isinstance(orders, list):
        return []
    return _batch_submission_order(
        [_managed_request_from_payload(order) for order in orders if isinstance(order, dict)]
    )


def _logical_batch_id_for_event_loop_batch(batch: OrderBatchRecord) -> str | None:
    requests = _event_loop_requests_from_batch(batch)
    if not requests:
        return None
    return _event_loop_logical_batch_id_for_requests(requests)


def _event_loop_logical_batch_id_for_requests(requests: list[ManagedOrderRequest]) -> str:
    signatures: list[dict[str, Any]] = []
    for request in requests:
        signature = _request_signature(request)
        metadata = dict(signature.get("metadata") if isinstance(signature.get("metadata"), dict) else {})
        for key in (
            "runtime_algo_instance_id",
            "runtime_child_order_id",
            "runtime_parent_intent_id",
            "qmt_batch_id",
        ):
            metadata.pop(key, None)
        signature["metadata"] = metadata
        signatures.append(signature)
    payload = json.dumps(signatures, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"qmtbatch_{hashlib.sha1(payload.encode('utf-8')).hexdigest()[:24]}"


def _managed_request_from_payload(payload: dict[str, Any]) -> ManagedOrderRequest:
    return ManagedOrderRequest(
        account_id=str(payload.get("account_id") or ""),
        strategy_name=str(payload.get("strategy_name") or ""),
        symbol=str(payload.get("symbol") or ""),
        side=str(payload.get("side") or ""),
        order_type=int(payload.get("order_type") or 0),
        quantity=int(payload.get("quantity") or 0),
        price_type=int(payload.get("price_type") or 0),
        price=Decimal(str(payload.get("price") or "0")),
        order_remark=str(payload.get("order_remark") or ""),
        trade_date=date.fromisoformat(str(payload.get("trade_date"))),
        mode=str(payload.get("mode") or "SIM"),
        package_id=_optional_str(payload.get("package_id")),
        selection_run_id=_optional_str(payload.get("selection_run_id")),
        target_weight=Decimal(str(payload.get("target_weight"))) if payload.get("target_weight") is not None else None,
        metadata=dict(payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}),
    )


def _event_loop_result_from_existing_intent(intent: Any) -> ManagedOrderSubmitResult:
    status = getattr(intent, "submit_status", None)
    success = status == IntentSubmitStatus.ACCEPTED or str(status) == IntentSubmitStatus.ACCEPTED.value
    qmt_order_id = None
    broker_message = "event_loop batch restored from qmt_strategy order intent"
    preflight = OrderPreflightResult(
        allowed=success,
        errors=()
        if success
        else (
            OrderPreflightError(
                "MINIQMT_EVENT_LOOP_STORED_INTENT_NOT_ACCEPTED",
                "stored event_loop order intent is not accepted",
                {"intent_id": getattr(intent, "intent_id", None)},
            ),
        ),
        strategy_id=getattr(intent, "strategy_id", None),
        estimated_notional=getattr(intent, "estimated_notional", None) or Decimal("0"),
        estimated_fee=getattr(intent, "estimated_fee", None) or Decimal("0"),
        freeze_amount=Decimal("0"),
        available_cash=None,
    )
    return ManagedOrderSubmitResult(
        success,
        getattr(intent, "intent_id", None),
        qmt_order_id,
        broker_message,
        preflight,
        success,
    )


def _event_loop_owned_parent_intent_mismatches(
    *,
    batch_id: str,
    request: ManagedOrderRequest,
    intent: Any,
) -> dict[str, dict[str, Any]]:
    parent_id = _parent_id_from_request(request)
    intent_metadata = dict(getattr(intent, "metadata", None) or {})
    request_metadata = dict(request.metadata or {})
    expected = {
        "intent_id": parent_id,
        "batch_id": batch_id,
        "account_id": request.account_id,
        "strategy_name": request.strategy_name,
        "symbol": request.symbol,
        "side": request.side,
        "order_type": request.order_type,
        "quantity": request.quantity,
        "price_type": request.price_type,
        "order_remark": request.order_remark,
        "trade_date": request.trade_date,
        "package_id": request.package_id,
        "selection_run_id": request.selection_run_id,
        "limit_price": request.price,
        "target_weight": request.target_weight,
        "preflight_status": IntentPreflightStatus.PASSED,
        "submit_status": IntentSubmitStatus.SUBMITTED,
    }
    actual = {
        key: getattr(intent, key, None)
        for key in expected
    }
    mismatches = {
        key: {"expected": _diagnostic_value(value), "actual": _diagnostic_value(actual[key])}
        for key, value in expected.items()
        if actual[key] != value
    }
    expected_metadata = {
        "runtime_id": request_metadata.get("runtime_id"),
        "runtime_parent_intent_id": parent_id,
        "event_loop_submit": True,
        "broker_called": False,
        "broker_call_pending": True,
        "qmt_batch_id": batch_id,
    }
    for key, value in expected_metadata.items():
        if intent_metadata.get(key) != value:
            mismatches[f"metadata.{key}"] = {
                "expected": _diagnostic_value(value),
                "actual": _diagnostic_value(intent_metadata.get(key)),
            }
    return mismatches


def _event_loop_owned_retry_request_mismatches(
    *,
    requests: list[ManagedOrderRequest],
    stored_requests: list[ManagedOrderRequest],
) -> dict[str, Any]:
    incoming_by_parent = {_parent_id_from_request(request): request for request in requests}
    stored_by_parent = {_parent_id_from_request(request): request for request in stored_requests}
    if (
        not incoming_by_parent
        or len(incoming_by_parent) != len(requests)
        or len(stored_by_parent) != len(stored_requests)
        or set(incoming_by_parent) != set(stored_by_parent)
    ):
        return {
            "parent_intent_ids": {
                "incoming": sorted(incoming_by_parent),
                "stored": sorted(stored_by_parent),
            }
        }
    mismatches: dict[str, Any] = {}
    for parent_id, stored in stored_by_parent.items():
        incoming = incoming_by_parent[parent_id]
        fields = {
            "account_id": (incoming.account_id, stored.account_id),
            "strategy_name": (incoming.strategy_name, stored.strategy_name),
            "symbol": (incoming.symbol, stored.symbol),
            "side": (incoming.side, stored.side),
            "order_type": (incoming.order_type, stored.order_type),
            "quantity": (incoming.quantity, stored.quantity),
            "price_type": (incoming.price_type, stored.price_type),
            "order_remark": (incoming.order_remark, stored.order_remark),
            "trade_date": (incoming.trade_date, stored.trade_date),
            "mode": (incoming.mode, stored.mode),
            "package_id": (incoming.package_id, stored.package_id),
            "selection_run_id": (incoming.selection_run_id, stored.selection_run_id),
            "target_weight": (incoming.target_weight, stored.target_weight),
        }
        incoming_metadata = dict(incoming.metadata or {})
        stored_metadata = dict(stored.metadata or {})
        for key in (
            "runtime_id",
            "runtime_parent_intent_id",
            "event_loop_preflight",
            "event_loop_submit",
            "runtime_owner",
            "source",
            "runtime_child_order_id",
            "runtime_algo_instance_id",
            "execution_policy_id",
            "execution_policy_sha256",
            "compiler_route_retired",
        ):
            fields[f"metadata.{key}"] = (incoming_metadata.get(key), stored_metadata.get(key))
        parent_mismatches = {
            key: {
                "incoming": _diagnostic_value(incoming_value),
                "stored": _diagnostic_value(stored_value),
            }
            for key, (incoming_value, stored_value) in fields.items()
            if incoming_value != stored_value
        }
        if parent_mismatches:
            mismatches[parent_id] = parent_mismatches
    return mismatches


def _diagnostic_value(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if hasattr(value, "value"):
        return value.value
    return value


def _event_loop_probe_intent_for_algo(
    instance: MiniQMTExecutionAlgoInstance,
    *,
    trade_date: date,
) -> OrderIntent:
    metadata = dict(instance.metadata or {})
    child_context = (
        metadata.get("runtime_child_context") if isinstance(metadata.get("runtime_child_context"), dict) else {}
    )
    return OrderIntent(
        intent_id=instance.parent_intent_id,
        package_id=str(child_context.get("package_id") or metadata.get("package_id") or instance.strategy_slot_id),
        portfolio_id=str(
            child_context.get("portfolio_id") or metadata.get("portfolio_id") or instance.strategy_slot_id
        ),
        symbol=instance.symbol,
        side=instance.side,
        quantity=max(int(instance.target_quantity), 1),
        order_type=OrderType.LIMIT,
        limit_price=metadata.get("marketable_limit_reference_price") or metadata.get("limit_price"),
        target_trade_date=trade_date,
        metadata={
            **dict(child_context),
            "reference_price": metadata.get("marketable_limit_reference_price") or metadata.get("limit_price"),
            "source": "event_loop_tick_driver_probe_intent",
            "runtime_algo_instance_id": instance.algo_instance_id,
            "runtime_parent_intent_id": instance.parent_intent_id,
        },
    )


def _event_loop_tail_sweep_enabled(config: dict[str, Any], *, as_of_time: datetime | None) -> bool:
    if as_of_time is None:
        return False
    raw_enabled = config.get("tail_sweep_enabled", True)
    if not isinstance(raw_enabled, bool):
        raise BrokerSubmitError("MiniQMT execution policy tail_sweep_enabled must be boolean")
    if not raw_enabled:
        return False
    cutoff = str(config.get("tail_sweep_time", "14:55")).strip()
    try:
        hour_text, minute_text = cutoff.split(":", 1)
        cutoff_minutes = int(hour_text) * 60 + int(minute_text)
    except (TypeError, ValueError) as exc:
        raise BrokerSubmitError("MiniQMT execution policy tail_sweep_time must use HH:MM") from exc
    if not 0 <= cutoff_minutes < 24 * 60:
        raise BrokerSubmitError("MiniQMT execution policy tail_sweep_time must use HH:MM")
    return as_of_time.hour * 60 + as_of_time.minute >= cutoff_minutes


def _event_loop_child_request_index(child: MiniQMTChildOrder, *, fallback: int) -> int:
    for value in (
        child.metadata.get("event_loop_request_index") if isinstance(child.metadata, dict) else None,
        child.metadata.get("request_index") if isinstance(child.metadata, dict) else None,
    ):
        try:
            index = int(value)
        except (TypeError, ValueError):
            continue
        if index > 0:
            return index
    return fallback


def _is_event_loop_pending_algo(instance: MiniQMTExecutionAlgoInstance | None) -> bool:
    if instance is None or instance.status != MiniQMTAlgoInstanceStatus.ACTIVE:
        return False
    state = instance.metadata.get("vnpy_algo_state") if isinstance(instance.metadata, dict) else None
    snapshot = state.get("snapshot") if isinstance(state, dict) else None
    status = str(snapshot.get("status") if isinstance(snapshot, dict) else "").strip().lower()
    return status in {"", "running", "active"}


def _pending_algo_count(
    algo_instances: tuple[MiniQMTExecutionAlgoInstance, ...],
    child_orders: tuple[MiniQMTChildOrder, ...],
) -> int:
    child_parent_ids = {child.parent_intent_id for child in child_orders}
    return sum(
        1
        for instance in algo_instances
        if _is_event_loop_pending_algo(instance) and instance.parent_intent_id not in child_parent_ids
    )


def _is_event_loop_pending_result(result: ManagedOrderSubmitResult) -> bool:
    if result.success or result.broker_called or result.qmt_order_id:
        return False
    if not result.preflight.allowed:
        return False
    message = str(result.broker_message or "").lower()
    return "pending tick trigger" in message or "algo dispatched" in message


def _event_loop_pending_algo_result(
    *,
    intent: OrderIntent,
    request: ManagedOrderRequest,
    preflight: OrderPreflightResult,
    algo_instance: MiniQMTExecutionAlgoInstance,
    source: str,
) -> ManagedOrderSubmitResult:
    return ManagedOrderSubmitResult(
        success=False,
        intent_id=request.metadata.get("runtime_parent_intent_id") or intent.intent_id,
        qmt_order_id=None,
        broker_message=(
            "event_loop algo dispatched and running; child order pending tick trigger "
            f"algo_instance_id={algo_instance.algo_instance_id}"
        ),
        preflight=replace(
            preflight,
            allowed=True,
            errors=tuple(
                error for error in preflight.errors if str(error.code).upper() != "MINIQMT_EVENT_LOOP_NO_CHILD_ORDER"
            ),
        ),
        broker_called=False,
    )


def _accepted_event_loop_child(children: list[MiniQMTChildOrder]) -> MiniQMTChildOrder:
    for child in children:
        if child.status != MiniQMTChildOrderStatus.REJECTED and child.broker_order_id:
            return child
    return children[-1]


def _event_loop_batch_status(
    requests: tuple[ManagedOrderRequest, ...] | list[ManagedOrderRequest],
    results: tuple[ManagedOrderSubmitResult, ...],
) -> OrderBatchStatus:
    succeeded = sum(1 for result in results if result.success)
    pending = sum(1 for result in results if _is_event_loop_pending_result(result))
    failed = len(results) - succeeded - pending
    if failed == 0 and pending > 0:
        return OrderBatchStatus.SUBMITTING
    if failed == 0 and succeeded > 0:
        return OrderBatchStatus.SUCCEEDED
    if succeeded > 0:
        return OrderBatchStatus.PARTIAL
    if any(result.broker_called for result in results):
        return OrderBatchStatus.FAILED
    if results and any(
        _is_non_compensating_batch_residual(request, result.preflight)
        for request, result in zip(list(requests), results, strict=False)
    ):
        return OrderBatchStatus.FAILED
    return OrderBatchStatus.PREFLIGHT_FAILED


def _b0_quote_v2_recovering_active(
    *,
    algo_instances: tuple[MiniQMTExecutionAlgoInstance, ...],
    active_child_orders: tuple[MiniQMTChildOrder, ...],
) -> bool:
    """Allow drain recovery only when durable active execution facts exist."""

    return bool(
        active_child_orders
        or any(
            instance.status in {MiniQMTAlgoInstanceStatus.ACTIVE, MiniQMTAlgoInstanceStatus.PAUSED}
            for instance in algo_instances
        )
    )


def _event_loop_preflight_passed(
    requests: tuple[ManagedOrderRequest, ...],
    results: tuple[ManagedOrderSubmitResult, ...],
) -> bool:
    residual_failed = sum(
        1
        for request, result in zip(requests, results, strict=False)
        if not result.success
        and not _is_event_loop_pending_result(result)
        and _is_non_compensating_batch_residual(request, result.preflight)
    )
    return residual_failed == 0 and all(result.preflight.allowed for result in results if result.broker_called)


def _event_loop_compensation_required(
    status: OrderBatchStatus | str,
    requests: tuple[ManagedOrderRequest, ...] | list[ManagedOrderRequest],
    results: tuple[ManagedOrderSubmitResult, ...],
) -> bool:
    normalized = status.value if isinstance(status, OrderBatchStatus) else str(status)
    if normalized != OrderBatchStatus.PARTIAL.value:
        return False
    failed = sum(1 for result in results if not result.success)
    pending = sum(1 for result in results if _is_event_loop_pending_result(result))
    residual_failed = sum(
        1
        for request, result in zip(list(requests), results, strict=False)
        if not result.success
        and not _is_event_loop_pending_result(result)
        and _is_non_compensating_batch_residual(request, result.preflight)
    )
    return failed - pending != residual_failed


def _persist_event_loop_tca_batch_observations(
    *,
    repository: Any,
    batch_id: str,
    requests: tuple[ManagedOrderRequest, ...],
    results: tuple[ManagedOrderSubmitResult, ...],
    arrival_capture_context_by_parent: dict[str, dict[str, Any]],
    request_quantity_before_cash_by_parent: dict[str, int],
    policy_context: dict[str, Any],
) -> None:
    """Persist observation-only arrival/preflight evidence after the existing batch write."""

    merger = getattr(repository, "merge_order_batch_tca_capture_sidecar", None)
    if not callable(merger):
        LOGGER.error(
            "TCA batch capture unavailable reason_code=ADAPTIVE_IS_TCA_CAPTURE_REPOSITORY_MISSING stage=CAPTURE batch_id=%s",
            batch_id,
        )
        return
    try:
        from backend.services.simulation_runtime.tca_capture import (
            CaptureMergeOutcome,
            TcaCaptureConfigurationError,
            build_arrival_benchmark_capture,
            build_capture_error,
            build_preflight_eligibility_capture,
            resolve_execution_deadline,
            resolve_tca_benchmark_policy,
        )
        from backend.services.trading_core.tca_sidecar import canonical_json_sha256

        scope_rows = []
        for request in requests:
            parent_id = _parent_id_from_request(request)
            if parent_id:
                scope_rows.append(
                    {
                        "parent_intent_id": parent_id,
                        "execution_plan_id": request.metadata.get("execution_plan_id"),
                        "execution_plan_hash": request.metadata.get("execution_plan_hash"),
                    }
                )
        logical_tca_scope_hash = canonical_json_sha256(
            {"batch_id": batch_id, "parents": sorted(scope_rows, key=lambda item: item["parent_intent_id"])}
        )
        policy = resolve_tca_benchmark_policy(policy_context)
    except TcaCaptureConfigurationError as exc:
        for request in requests:
            parent_id = _parent_id_from_request(request)
            if not parent_id:
                continue
            outcome = merger(
                batch_id=batch_id,
                logical_tca_scope_hash=logical_tca_scope_hash,
                parent_intent_id=parent_id,
                capture_error=build_capture_error(
                    parent_intent_id=parent_id,
                    stage="CAPTURE",
                    reason_code=exc.reason_code,
                    message=str(exc),
                    context={"batch_id": batch_id},
                ),
            )
            LOGGER.error(
                "TCA batch capture policy missing reason_code=%s stage=CAPTURE batch_id=%s parent_intent_id=%s outcome=%s",
                exc.reason_code,
                batch_id,
                parent_id,
                getattr(outcome, "value", outcome),
            )
        return
    except Exception as exc:  # Observation evidence must never alter broker execution.
        LOGGER.exception(
            "TCA batch capture setup failed reason_code=ADAPTIVE_IS_TCA_BATCH_CAPTURE_SETUP_FAILED stage=CAPTURE batch_id=%s error_type=%s",
            batch_id,
            type(exc).__name__,
        )
        return

    for request, result in zip(requests, results, strict=False):
        parent_id = _parent_id_from_request(request)
        if not parent_id:
            LOGGER.error(
                "TCA batch capture skipped reason_code=ADAPTIVE_IS_TCA_PARENT_IDENTITY_MISSING stage=CAPTURE batch_id=%s",
                batch_id,
            )
            continue
        try:
            plan_id = str(request.metadata.get("execution_plan_id") or "").strip()
            plan_hash = str(request.metadata.get("execution_plan_hash") or "").strip()
            if not plan_id or not plan_hash:
                raise ValueError(
                    "execution_plan_id/execution_plan_hash missing from canonical managed request metadata"
                )
            arrival_context = arrival_capture_context_by_parent.get(parent_id)
            if not isinstance(arrival_context, dict):
                raise ValueError("first event-loop quote capture context is missing")
            arrival = build_arrival_benchmark_capture(
                execution_plan_id=plan_id,
                execution_plan_hash=plan_hash,
                parent_intent_id=parent_id,
                symbol=request.symbol,
                side=request.side,
                arrival_time=arrival_context["arrival_time"],
                arrival_quote_received_at=arrival_context["arrival_quote_received_at"],
                tick_payload=arrival_context["tick_payload"],
                policy=policy,
            )
            outcome = merger(
                batch_id=batch_id,
                logical_tca_scope_hash=logical_tca_scope_hash,
                parent_intent_id=parent_id,
                arrival_capture=arrival.model_dump(mode="json"),
            )
            if getattr(outcome, "value", outcome) in {
                CaptureMergeOutcome.CONFLICT.value,
                CaptureMergeOutcome.IDENTITY_DRIFT.value,
                CaptureMergeOutcome.NOT_FOUND.value,
            }:
                LOGGER.error(
                    "TCA arrival merge failed reason_code=ADAPTIVE_IS_TCA_CAPTURE_MERGE_%s stage=CAPTURE batch_id=%s parent_intent_id=%s",
                    getattr(outcome, "value", outcome),
                    batch_id,
                    parent_id,
                )
            eligibility = build_preflight_eligibility_capture(
                parent_intent_id=parent_id,
                batch_id=batch_id,
                eligibility_as_of=datetime.now(UTC),
                request_quantity_before_cash=request_quantity_before_cash_by_parent.get(
                    parent_id, int(request.quantity)
                ),
                request_quantity_after_cash=int(request.quantity),
                preflight_result=result.preflight.to_dict(),
                is_dependent_buy=_is_dependent_buy_proceeds_deferred(request, result.preflight),
                is_capacity_residual=_is_capacity_residual_skipped(request, result.preflight),
                dependency_parent_ids=tuple(
                    str(item) for item in request.metadata.get("dependent_parent_intent_ids", ()) if str(item).strip()
                ),
                deadline_context=resolve_execution_deadline(
                    schedule_window=(
                        policy_context.get("policy_json", {}).get("schedule_window", {})
                        if isinstance(policy_context.get("policy_json"), dict)
                        else {}
                    ),
                    trade_date=request.trade_date,
                ),
            )
            outcome = merger(
                batch_id=batch_id,
                logical_tca_scope_hash=logical_tca_scope_hash,
                parent_intent_id=parent_id,
                eligibility_capture=eligibility.model_dump(mode="json"),
            )
            if eligibility.eligibility_class == "UNKNOWN_UNMAPPED":
                LOGGER.error(
                    "TCA eligibility unmapped reason_code=ADAPTIVE_IS_TCA_ELIGIBILITY_REASON_UNMAPPED stage=CAPTURE batch_id=%s parent_intent_id=%s primary_reason_code=%s",
                    batch_id,
                    parent_id,
                    eligibility.primary_reason_code,
                )
        except Exception as exc:  # evidence failure is loud and isolated from B0.
            outcome = merger(
                batch_id=batch_id,
                logical_tca_scope_hash=logical_tca_scope_hash,
                parent_intent_id=parent_id,
                capture_error=build_capture_error(
                    parent_intent_id=parent_id,
                    stage="CAPTURE",
                    reason_code="ADAPTIVE_IS_TCA_EVENT_LOOP_CAPTURE_FAILED",
                    message=f"{type(exc).__name__}: {exc}",
                    context={"batch_id": batch_id, "symbol": request.symbol},
                ),
            )
            LOGGER.exception(
                "TCA event-loop capture failed reason_code=ADAPTIVE_IS_TCA_EVENT_LOOP_CAPTURE_FAILED stage=CAPTURE batch_id=%s parent_intent_id=%s outcome=%s",
                batch_id,
                parent_id,
                getattr(outcome, "value", outcome),
            )


def _event_loop_batch_metadata(
    *,
    requests: tuple[ManagedOrderRequest, ...],
    results: tuple[ManagedOrderSubmitResult, ...],
    runtime_evidence: MiniQMTRuntimeEvidence,
    source: str,
) -> dict[str, Any]:
    dependent_buy_count = sum(
        1
        for request, result in zip(requests, results, strict=False)
        if not result.success and _is_dependent_buy_proceeds_deferred(request, result.preflight)
    )
    capacity_residual_count = sum(
        1
        for request, result in zip(requests, results, strict=False)
        if not result.success and _is_capacity_residual_skipped(request, result.preflight)
    )
    status = _event_loop_batch_status(requests, results)
    pending_count = sum(1 for result in results if _is_event_loop_pending_result(result))
    return {
        "source": source,
        "runtime_route": "A_EVENT_LOOP",
        "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
        "compiler_route_retired": True,
        "preflight_passed": _event_loop_preflight_passed(requests, results),
        "dependent_buy_deferred": dependent_buy_count > 0,
        "dependent_buy_count": dependent_buy_count,
        "capacity_residual_skipped": capacity_residual_count > 0,
        "capacity_residual_count": capacity_residual_count,
        "event_loop_pending": pending_count > 0,
        "event_loop_pending_count": pending_count,
        "triggered_child_order_count": sum(1 for result in results if result.success),
        "compensation_required": _event_loop_compensation_required(status, requests, results),
        "compensation_actions": [],
        "broker_called": any(result.broker_called for result in results),
        "runtime_evidence": runtime_evidence.to_dict(),
    }


def _single_strategy_id(results: tuple[ManagedOrderSubmitResult, ...]) -> str | None:
    strategy_ids = sorted({result.preflight.strategy_id for result in results if result.preflight.strategy_id})
    return strategy_ids[0] if len(strategy_ids) == 1 else None


def _optional_str(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _event_loop_no_child_result(*, intent: OrderIntent, source: str) -> ManagedOrderSubmitResult:
    preflight = OrderPreflightResult(
        allowed=False,
        errors=(
            OrderPreflightError(
                code="MINIQMT_EVENT_LOOP_NO_CHILD_ORDER",
                message="event_loop route produced no child order for parent intent",
                context={"intent_id": intent.intent_id, "symbol": intent.symbol, "source": source},
            ),
        ),
        strategy_id=str(intent.metadata.get("strategy_id") or intent.portfolio_id),
        estimated_notional=Decimal("0"),
        estimated_fee=Decimal("0"),
        freeze_amount=Decimal("0"),
        available_cash=None,
    )
    return ManagedOrderSubmitResult(
        success=False,
        intent_id=intent.intent_id,
        qmt_order_id=None,
        broker_message="event_loop route produced no child order",
        preflight=preflight,
        broker_called=False,
    )


def _raise_event_loop_no_child_order(
    *,
    missing_child_intents: list[OrderIntent],
    parent_intents: list[OrderIntent],
    new_children: tuple[MiniQMTChildOrder, ...],
    runtime_evidence: MiniQMTRuntimeEvidence,
    runtime_id: str,
    strategy_slot_id: str,
    source: str,
) -> None:
    submitted_children = [
        child
        for child in new_children
        if child.status != MiniQMTChildOrderStatus.REJECTED and bool(child.broker_order_id)
    ]
    first_missing = missing_child_intents[0]
    raise BrokerSubmitError(
        "MiniQMT event_loop route produced no child order for parent intent",
        context={
            "reason_code": "MINIQMT_EVENT_LOOP_NO_CHILD_ORDER",
            "stage": "MINIQMT_EVENT_LOOP_SUBMIT_NO_CHILD_ORDER",
            "runtime_id": runtime_id,
            "runtime_evidence": runtime_evidence.to_dict(),
            "strategy_slot_id": strategy_slot_id,
            "source": source,
            "intent_id": first_missing.intent_id,
            "symbol": first_missing.symbol,
            "side": first_missing.side.value,
            "quantity": int(first_missing.quantity),
            "missing_parent_intent_ids": [intent.intent_id for intent in missing_child_intents],
            "parent_intent_count": len(parent_intents),
            "broker_called": bool(new_children),
            "submitted_intents": len(submitted_children),
            "failed_intents": len(missing_child_intents),
            "child_order_count": len(new_children),
            "submitted_child_order_ids": [child.child_order_id for child in submitted_children],
        },
    )


def _event_loop_prepare_order_intent(
    *,
    repository: Any,
    request: ManagedOrderRequest,
    preflight: OrderPreflightResult,
    source: str,
) -> Any | None:
    parent_id = _parent_id_from_request(request)
    if not parent_id:
        raise BrokerSubmitError(
            "MiniQMT event_loop submit requires runtime parent intent id before broker call",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_PARENT_INTENT_ID_MISSING",
                "stage": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PERSIST",
                "order_remark": request.order_remark,
                "broker_called": False,
            },
        )
    getter = getattr(repository, "get_order_intent", None)
    creator = getattr(repository, "create_order_intent", None)
    if not callable(getter) or not callable(creator):
        raise BrokerSubmitError(
            "MiniQMT event_loop submit requires qmt_strategy order intent persistence",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_ORDER_INTENT_REPOSITORY_MISSING",
                "stage": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PERSIST",
                "parent_intent_id": parent_id,
                "broker_called": False,
            },
        )
    try:
        return getter(parent_id)
    except DataUnavailableError:
        pass
    strategy_id = preflight.strategy_id or str(request.metadata.get("strategy_id") or "").strip()
    if not strategy_id:
        raise BrokerSubmitError(
            "MiniQMT event_loop submit requires qmt_strategy strategy_id for order intent persistence",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_STRATEGY_ID_MISSING",
                "stage": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PERSIST",
                "parent_intent_id": parent_id,
                "broker_called": False,
            },
        )
    return creator(
        OrderIntentRecord(
            intent_id=parent_id,
            batch_id=request.metadata.get("qmt_batch_id"),
            strategy_id=strategy_id,
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
            preflight_status=IntentPreflightStatus.PASSED if preflight.allowed else IntentPreflightStatus.FAILED,
            submit_status=IntentSubmitStatus.SUBMITTED,
            metadata={
                **dict(request.metadata or {}),
                "source": source,
                "event_loop_submit": True,
                "broker_called": False,
                "broker_call_pending": True,
            },
        )
    )


def _event_loop_upsert_order_intent(
    *,
    repository: Any,
    request: ManagedOrderRequest,
    child: MiniQMTChildOrder,
    preflight: OrderPreflightResult,
    accepted: bool,
    source: str,
) -> Any | None:
    getter = getattr(repository, "get_order_intent", None)
    creator = getattr(repository, "create_order_intent", None)
    setter = getattr(repository, "set_order_intent_submit_status", None)
    if not callable(getter) or not callable(creator):
        raise BrokerSubmitError(
            "MiniQMT event_loop submit requires qmt_strategy order intent persistence",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_ORDER_INTENT_REPOSITORY_MISSING",
                "stage": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PERSIST",
                "parent_intent_id": child.parent_intent_id,
                "qmt_order_id": child.broker_order_id,
            },
        )
    existing = None
    try:
        existing = getter(child.parent_intent_id)
    except Exception:  # noqa: BLE001 - repository miss types differ across test/prod implementations.
        existing = None
    status = IntentSubmitStatus.ACCEPTED if accepted else IntentSubmitStatus.REJECTED
    if existing is not None:
        expected_batch_id = request.metadata.get("qmt_batch_id")
        if expected_batch_id and getattr(existing, "batch_id", None) not in {expected_batch_id, None}:
            raise BrokerSubmitError(
                "MiniQMT event_loop order intent is already linked to a different batch",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_ORDER_INTENT_BATCH_MISMATCH",
                    "stage": "MINIQMT_EVENT_LOOP_ORDER_INTENT_PERSIST",
                    "parent_intent_id": child.parent_intent_id,
                    "existing_batch_id": getattr(existing, "batch_id", None),
                    "expected_batch_id": expected_batch_id,
                    "broker_called": True,
                    "qmt_order_id": child.broker_order_id,
                },
            )
        if callable(setter):
            return setter(
                child.parent_intent_id,
                status,
                submitted_at=child.submitted_at or datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        return existing
    return creator(
        OrderIntentRecord(
            intent_id=child.parent_intent_id,
            batch_id=request.metadata.get("qmt_batch_id"),
            strategy_id=preflight.strategy_id or str(request.metadata.get("strategy_id") or child.strategy_slot_id),
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
            preflight_status=IntentPreflightStatus.PASSED if preflight.allowed else IntentPreflightStatus.FAILED,
            submit_status=status,
            metadata={
                **dict(request.metadata or {}),
                "source": source,
                "runtime_child_order_id": child.child_order_id,
                "runtime_algo_instance_id": child.algo_instance_id,
                "runtime_parent_intent_id": child.parent_intent_id,
                "event_loop_submit": True,
                "broker_called": True,
                "qmt_order_id": child.broker_order_id,
            },
            submitted_at=child.submitted_at or datetime.now(UTC),
        )
    )


def _event_loop_child_submit_result(
    *,
    child: MiniQMTChildOrder,
    request: ManagedOrderRequest,
    preflight: OrderPreflightResult,
    repository: Any,
    source: str,
) -> ManagedOrderSubmitResult:
    accepted = child.status != MiniQMTChildOrderStatus.REJECTED and bool(child.broker_order_id)
    if not accepted:
        error = OrderPreflightError(
            code="MINIQMT_EVENT_LOOP_CHILD_REJECTED",
            message=str(child.metadata.get("gateway_message") or "event_loop child order rejected"),
            context={
                "runtime_id": child.runtime_id,
                "child_order_id": child.child_order_id,
                "parent_intent_id": child.parent_intent_id,
                "symbol": child.symbol,
                "source": source,
            },
        )
        preflight = replace(preflight, allowed=False, errors=preflight.errors + (error,))
    intent = _event_loop_upsert_order_intent(
        repository=repository,
        request=request,
        child=child,
        preflight=preflight,
        accepted=accepted,
        source=source,
    )
    return ManagedOrderSubmitResult(
        success=accepted,
        intent_id=intent.intent_id if intent is not None else child.parent_intent_id,
        qmt_order_id=child.broker_order_id,
        broker_message=str(child.metadata.get("gateway_message") or ("accepted" if accepted else "rejected")),
        preflight=preflight,
        broker_called=True,
    )


def _timer_iterations(algo_code: str, config: dict[str, Any]) -> int:
    if algo_code != "TWAP_LITE_MINIQMT":
        return int(config.get("timer_iterations", 1) or 1)
    interval = int(config.get("interval", config.get("interval_seconds", 60)) or 60)
    return int(config.get("timer_iterations", max(1, interval)) or max(1, interval))


def _board_lot_for_runtime(symbol: str) -> tuple[int, int]:
    try:
        return board_lot_rule(symbol)
    except ValueError as exc:
        raise BrokerSubmitError(
            "MiniQMTExecutionRuntime vn.py path requires a recognized A-share symbol",
            context={"symbol": symbol, "reason": str(exc)},
        ) from exc


def _runtime_status_from_managed_result(
    result: ManagedOrderSubmitResult,
    *,
    ledger_order: OrderLedgerRecord | None,
) -> MiniQMTChildOrderStatus:
    if not result.success:
        return MiniQMTChildOrderStatus.REJECTED
    if ledger_order is None:
        return MiniQMTChildOrderStatus.SUBMITTED
    traded_volume = int(ledger_order.traded_volume or 0)
    order_volume = max(int(ledger_order.order_volume or 0), 1)
    if is_terminal_order_status(ledger_order.order_status):
        return _runtime_terminal_status_from_order_status(ledger_order.order_status)
    if is_partial_order_status(ledger_order.order_status) or traded_volume > 0:
        if traded_volume >= order_volume:
            return MiniQMTChildOrderStatus.FILLED
        return MiniQMTChildOrderStatus.PARTIALLY_FILLED
    return MiniQMTChildOrderStatus.SUBMITTED


def _runtime_terminal_status_from_order_status(order_status: Any) -> MiniQMTChildOrderStatus:
    try:
        status = int(order_status)
    except (TypeError, ValueError):
        return MiniQMTChildOrderStatus.SUBMITTED
    if status == STATUS_CANCELLED:
        return MiniQMTChildOrderStatus.CANCELLED
    if status == STATUS_FILLED:
        return MiniQMTChildOrderStatus.FILLED
    if status == STATUS_REJECTED:
        return MiniQMTChildOrderStatus.REJECTED
    return MiniQMTChildOrderStatus.SUBMITTED


def _ledger_order_for_managed_result(
    managed_order_service: QmtManagedOrderService,
    request: ManagedOrderRequest,
    result: ManagedOrderSubmitResult,
) -> OrderLedgerRecord | None:
    qmt_order_id = str(result.qmt_order_id or "").strip()
    if not qmt_order_id:
        return None
    repository = getattr(managed_order_service, "_repository", None)
    if repository is None:
        raise BrokerSubmitError(
            "MiniQMT managed child status sync requires strategy ledger repository",
            context={"reason_code": "MINIQMT_RUNTIME_LEDGER_REPOSITORY_MISSING", "qmt_order_id": qmt_order_id},
        )
    getter = getattr(repository, "get_order_ledger", None)
    if not callable(getter):
        raise BrokerSubmitError(
            "MiniQMT managed child status sync requires get_order_ledger(account_id, qmt_order_id)",
            context={
                "reason_code": "MINIQMT_RUNTIME_LEDGER_GETTER_MISSING",
                "qmt_order_id": qmt_order_id,
                "account_id": request.account_id,
            },
        )
    order = getter(request.account_id, qmt_order_id)
    if order is not None:
        return order
    raise BrokerSubmitError(
        "MiniQMT managed child status sync could not find broker order in strategy ledger",
        context={
            "reason_code": "MINIQMT_RUNTIME_LEDGER_ORDER_MISSING",
            "qmt_order_id": qmt_order_id,
            "account_id": request.account_id,
            "order_remark": request.order_remark,
            "intent_id": result.intent_id,
        },
    )


def _ledger_order_payload(order: OrderLedgerRecord) -> dict[str, Any]:
    return {
        "qmt_order_id": order.qmt_order_id,
        "symbol": order.symbol,
        "order_type": order.order_type,
        "order_volume": order.order_volume,
        "traded_volume": order.traded_volume,
        "order_status": order.order_status,
        "status_msg": order.status_msg,
        "order_remark": order.order_remark,
    }


def _submitted_child_count(child_orders: tuple[MiniQMTChildOrder, ...]) -> int:
    accepted_statuses = {
        MiniQMTChildOrderStatus.SUBMITTED,
        MiniQMTChildOrderStatus.PARTIALLY_FILLED,
        MiniQMTChildOrderStatus.FILLED,
        MiniQMTChildOrderStatus.CANCELLED,
    }
    return sum(1 for item in child_orders if item.status in accepted_statuses)


def _all_results_preview_only(results: tuple[ManagedOrderSubmitResult, ...]) -> bool:
    if not results:
        return False
    for result in results:
        if not _result_preview_only(result):
            return False
    return True


def _result_preview_only(result: ManagedOrderSubmitResult) -> bool:
    try:
        payload = result.to_dict()
    except Exception:  # noqa: BLE001
        payload = {}
    if payload.get("preview_only") is True:
        return True
    broker_message = str(getattr(result, "broker_message", "") or "").lower()
    return broker_message.startswith("preview-only")


def _find_child_order(
    repository: MiniQMTExecutionRuntimeRepository,
    *,
    runtime_id: str,
    child_order_id: str,
) -> MiniQMTChildOrder | None:
    for child in repository.list_child_orders(runtime_id, active_only=False):
        if child.child_order_id == child_order_id:
            return child
    return None


def _short_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=True, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def _json_safe(value: Any) -> Any:
    return json.loads(json.dumps(value, ensure_ascii=True, sort_keys=True, default=str))


__all__ = [
    "MiniQMTExecutionRuntimeClient",
    "MiniQMTPlanPreviewResult",
    "MiniQMTOperatorCommandResult",
    "MiniQMTRuntimeEvidence",
    "MiniQMTRuntimeManagedBatchSubmitResult",
    "MiniQMTRuntimeManagedVnpyBuildResult",
]
