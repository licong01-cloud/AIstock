"""Runtime client facades for MiniQMT product entry points."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Callable

from backend.execution_algos.board_lot import board_lot_rule
from backend.execution_algos.vnpy_style import get_vnpy_style_asset, is_vnpy_style_algo
from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle, OrderHandleStatus
from backend.services.qmt_strategy_ledger.models import (
    OrderLedgerRecord,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_REJECTED,
    is_partial_order_status,
    is_terminal_order_status,
)
from backend.services.qmt_strategy_ledger.order_service import (
    ManagedBatchSubmitResult,
    ManagedOrderRequest,
    ManagedOrderSubmitResult,
    OrderPreflightError,
    OrderPreflightResult,
    QmtManagedOrderService,
)
from backend.services.qmt_strategy_ledger.repository import QmtStrategyLedgerRepository
from backend.services.trading_core.errors import BrokerSubmitError, TradingCoreError
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTAlgoChildOrder,
    MiniQMTAlgoExecutionResult,
    MiniQMTChildOrderHandle,
    MiniQMTOrderStatus,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

from .config import MiniQMTExecutionRuntimeKind, get_miniqmt_execution_runtime_kind
from .gateway import MiniQMTGateway, MiniQMTGatewayCancelAck, MiniQMTGatewayOrderAck, QmtClientMiniQMTEventLoopGateway
from .models import (
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOperatorCommandResult,
)
from .repository import MiniQMTExecutionRuntimeRepository, default_miniqmt_execution_runtime_repository
from .runtime import MiniQMTExecutionRuntime

RUNTIME_OWNER = "MiniQMTExecutionRuntime"
MINIQMT_EVENT_LOOP_BROKER_QUOTE_SOURCE = "MINIQMT_REALTIME.broker_quote"


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
        payload = {
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
class PaperMiniQMTRuntimeChildResult:
    parent_intent: OrderIntent
    child_intent: OrderIntent
    child_order: MiniQMTChildOrder
    handle: OrderHandle | None
    status: OrderHandleStatus | None
    trades: tuple[dict[str, Any], ...]
    native_context: dict[str, Any]
    submit_error: dict[str, Any] | None = None
    submit_exception: BaseException | None = None


@dataclass(frozen=True)
class PaperMiniQMTRuntimeSubmitResult:
    runtime_evidence: MiniQMTRuntimeEvidence
    child_results: tuple[PaperMiniQMTRuntimeChildResult, ...]


class MiniQMTExecutionRuntimeClient:
    """Facade used by product paths to enter the canonical runtime owner."""

    def __init__(
        self,
        *,
        repository: MiniQMTExecutionRuntimeRepository | None = None,
        strategy_ledger_repository: Any | None = None,
        runtime_kind: MiniQMTExecutionRuntimeKind | str | None = None,
    ) -> None:
        self.repository = repository or default_miniqmt_execution_runtime_repository()
        self.runtime_kind = (
            MiniQMTExecutionRuntimeKind(runtime_kind)
            if runtime_kind is not None
            else get_miniqmt_execution_runtime_kind(os.environ)
        )
        self.strategy_ledger_repository = strategy_ledger_repository
        if self.runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP and self.strategy_ledger_repository is None:
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
                limit_price=_limit_price_for_runtime(intent=intent, quote_provider=quote_provider),
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
        source: str = "simulation_runtime_event_loop_submit",
    ) -> MiniQMTRuntimeManagedBatchSubmitResult:
        """Submit a gray-switched SIM scope through the real event-loop gateway.

        This is the D4 Route-A boundary.  It refuses compiler-style managed
        request generation and drives vn.py instances only through the
        event-loop gateway callback surface.
        """

        if self.runtime_kind != MiniQMTExecutionRuntimeKind.EVENT_LOOP:
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires runtime_kind=event_loop",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_RUNTIME_KIND_REQUIRED",
                    "source": source,
                    "runtime_kind": self.runtime_kind.value,
                },
            )
        if self.strategy_ledger_repository is None:
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires qmt_strategy ledger authority",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_LEDGER_AUTHORITY_MISSING",
                    "source": source,
                    "runtime_id": runtime_id,
                    "account_group_id": account_group_id,
                },
            )
        if not parent_intents:
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires at least one parent intent",
                context={"reason_code": "MINIQMT_EVENT_LOOP_PARENT_INTENTS_MISSING", "source": source},
            )
        policy_json = policy_context.get("policy_json") if isinstance(policy_context, dict) else None
        if not isinstance(policy_json, dict):
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires policy_json",
                context={"reason_code": "MINIQMT_EVENT_LOOP_POLICY_JSON_MISSING", "source": source},
            )
        algo_code = str(policy_json.get("algo_code") or "").strip().upper()
        if not is_vnpy_style_algo(algo_code):
            raise BrokerSubmitError(
                "MiniQMT event_loop submit requires approved MiniQMT vn.py algo",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_ALGO_UNSUPPORTED",
                    "source": source,
                    "algo_code": algo_code,
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
                "source": source,
                "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
                "gateway_class": "QmtClientMiniQMTEventLoopGateway",
                "oms_authority": "qmt_strategy_ledger",
                "algo_code": algo_code,
                "account_id": account_id,
            },
        )
        runtime.start()
        runtime.recover()
        existing_child_ids = {
            child.child_order_id
            for child in self.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
        }
        for index, intent in enumerate(parent_intents, start=1):
            tick_payload = _required_event_loop_tick_payload(
                intent=intent,
                quote_provider=quote_provider,
                qmt_client=qmt_client,
                source=source,
            )
            child_context = (
                child_context_factory(intent, index)
                if child_context_factory is not None
                else _event_loop_child_metadata(intent=intent, trade_date=trade_date, source=source, index=index)
            )
            runtime.create_vnpy_algo_instance(
                parent_intent_id=intent.intent_id,
                strategy_slot_id=strategy_slot_id,
                symbol=intent.symbol,
                side=intent.side,
                target_quantity=int(intent.quantity),
                algo_code=algo_code,
                limit_price=_limit_price_for_event_loop(intent=intent, tick_payload=tick_payload),
                algo_config=dict(policy_json.get("algo_config") or {}),
                metadata={
                    "source": source,
                    "runtime_child_context": child_context,
                    "execution_policy_id": policy_context.get("validated_execution_policy_id"),
                    "execution_policy_sha256": policy_context.get("policy_sha256"),
                    "event_loop_submit": True,
                    "quote_source": tick_payload.get("source") or tick_payload.get("quote_source"),
                },
            )
            gateway.on_tick(tick_payload)
        runtime_evidence = self._evidence(runtime, source=source)
        new_children = tuple(
            child
            for child in self.repository.list_child_orders(runtime.config.runtime_id, active_only=False)
            if child.child_order_id not in existing_child_ids
        )
        child_by_parent: dict[str, list[MiniQMTChildOrder]] = {}
        for child in new_children:
            child_by_parent.setdefault(child.parent_intent_id, []).append(child)
        results: list[ManagedOrderSubmitResult] = []
        missing_child_intents: list[OrderIntent] = []
        for intent in parent_intents:
            children = child_by_parent.get(intent.intent_id) or []
            if not children:
                missing_child_intents.append(intent)
                continue
            for child in children:
                results.append(_event_loop_child_submit_result(child=child, source=source))
        if missing_child_intents:
            _raise_event_loop_no_child_order(
                missing_child_intents=missing_child_intents,
                parent_intents=parent_intents,
                new_children=new_children,
                runtime_evidence=runtime_evidence,
                runtime_id=runtime.config.runtime_id,
                strategy_slot_id=strategy_slot_id,
                source=source,
            )
        succeeded = sum(1 for item in results if item.success)
        failed = len(results) - succeeded
        batch_status = "SUCCEEDED" if failed == 0 and succeeded > 0 else ("PARTIAL" if succeeded else "FAILED")
        managed_result = ManagedBatchSubmitResult(
            success=failed == 0 and succeeded > 0,
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=tuple(results),
            compensation_required=False,
            compensation_hint=None if failed == 0 else "inspect event_loop child result reason_code",
            batch_id=f"mqrt_event_loop_{_short_hash([runtime_evidence.runtime_id, len(results), source])}",
            batch_status=batch_status,
            preflight_passed=failed == 0,
            compensation_actions=(),
        )
        return MiniQMTRuntimeManagedBatchSubmitResult.from_managed_result(
            managed_result,
            runtime_evidence=runtime_evidence,
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
        policy_json = execution_policy_context.get("policy_json") if isinstance(execution_policy_context, dict) else None
        if not isinstance(policy_json, dict):
            raise BrokerSubmitError("MiniQMTExecutionRuntime vn.py client requires policy_json")
        algo_code = str(policy_json.get("algo_code") or "").strip().upper()
        if not is_vnpy_style_algo(algo_code):
            raise BrokerSubmitError(
                "MiniQMTExecutionRuntime vn.py client requires approved MiniQMT algo",
                context={"algo_code": algo_code},
            )
        spec = get_vnpy_style_asset(algo_code)
        gateway = PaperV2MiniQMTRuntimeGateway(broker=broker)
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=_paper_runtime_id(run),
            gateway=gateway,
            metadata={"portfolio_id": portfolio.portfolio_id, "run_id": run.run_id, "source": source},
        )
        runtime.start()
        runtime.create_vnpy_algo_instance(
            parent_intent_id=intent.intent_id,
            strategy_slot_id=strategy_slot_id,
            symbol=intent.symbol,
            side=intent.side,
            target_quantity=int(intent.quantity),
            algo_code=algo_code,
            limit_price=_limit_price_for_runtime(intent=intent, quote_provider=quote_provider),
            algo_config=dict(policy_json.get("algo_config") or {}),
            metadata={
                "source": source,
                "runtime_child_context": _paper_child_metadata(intent=intent, trade_date=trade_date, source=source),
                "execution_policy_id": execution_policy_context.get("validated_execution_policy_id"),
                "execution_policy_sha256": execution_policy_context.get("policy_sha256"),
            },
        )
        tick_payload = _tick_payload_for_runtime(intent=intent, quote_provider=quote_provider)
        runtime.on_tick(symbol=intent.symbol, price=float(tick_payload["price"]), payload=tick_payload)
        for index in range(_timer_iterations(algo_code, dict(policy_json.get("algo_config") or {}))):
            runtime.on_timer(timer_name=f"paper_v2_{algo_code.lower()}_{index + 1}")
        latest_algo = self.repository.list_algo_instances(runtime.config.runtime_id, active_only=False)[-1]
        child_results = tuple(gateway.results_for_parent_intent(intent.intent_id))
        child_by_id = {result.child_order.child_order_id: result for result in child_results}
        for child in self.repository.list_child_orders(runtime.config.runtime_id, active_only=False):
            result = child_by_id.get(child.child_order_id)
            if result is None:
                continue
            updated_child = self._sync_paper_child_status(
                runtime,
                child,
                result,
                preserve_gateway_rejection=result.submit_error is not None,
            )
            child_by_id[child.child_order_id] = replace(result, child_order=updated_child)
        child_results = tuple(child_by_id[child_id] for child_id in child_by_id)
        children = [
            MiniQMTAlgoChildOrder(
                vt_orderid=str(result.child_order.metadata.get("vnpy_vt_orderid") or result.child_order.child_order_id),
                handle=_shared_handle(result),
                intent=result.child_intent,
                submitted_at=result.child_order.submitted_at or datetime.now(UTC),
                native_context=dict(result.native_context),
                status=_shared_status(result.status),
                trades=[dict(row) for row in result.trades],
                submit_error=result.submit_error,
            )
            for result in child_results
        ]
        diagnostic = _vnpy_runtime_diagnostic(
            algo_code=algo_code,
            spec_metadata=spec.metadata(),
            intent=intent,
            execution_policy_context=execution_policy_context,
            tick_payload=tick_payload,
            child_results=child_results,
            runtime_evidence=self._evidence(runtime, source=source),
            algo_state=dict(latest_algo.metadata.get("vnpy_algo_state") or {}),
        )
        return MiniQMTAlgoExecutionResult(
            parent_intent=intent,
            algo_code=algo_code,
            policy_context=dict(execution_policy_context),
            policy_sha256=str(execution_policy_context.get("policy_sha256") or "") or None,
            asset_metadata=spec.metadata(),
            actions=[],
            child_orders=children,
            algo_state=dict(latest_algo.metadata.get("vnpy_algo_state") or {}),
            terminal_state=_terminal_state(child_results),
            diagnostic=diagnostic,
        )

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
                runtime_id=runtime_id or f"mqrt_{_short_hash([account_group_id, trade_date.isoformat(), runtime_config_hash])}",
                account_group_id=account_group_id,
                trade_date=trade_date,
                runtime_config_hash=runtime_config_hash,
                metadata=dict(metadata or {}),
            ),
            repository=self.repository,
            gateway=gateway,
            strategy_ledger_repository=(
                self.strategy_ledger_repository
                if self.runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP
                else None
            ),
            account_id=account_id or account_group_id,
        )

    def _reject_event_loop_compiler_lifecycle(self, *, source: str, operation: str, **context: Any) -> None:
        if self.runtime_kind != MiniQMTExecutionRuntimeKind.EVENT_LOOP:
            return
        raise BrokerSubmitError(
            "MiniQMT event_loop runtime requires real gateway callbacks and refuses compiler-style "
            "managed runtime lifecycle; reason_code=MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS",
                "source": source,
                "operation": operation,
                **context,
            },
        )

    def evidence_for_runtime(self, runtime_id: str, *, source: str) -> MiniQMTRuntimeEvidence:
        runtime_record = self.repository.get_runtime(runtime_id)
        if runtime_record is None:
            raise BrokerSubmitError("MiniQMT runtime evidence is missing", context={"runtime_id": runtime_id})
        child_orders = tuple(self.repository.list_child_orders(runtime_id, active_only=False))
        return MiniQMTRuntimeEvidence(
            runtime_id=runtime_id,
            runtime_owner=RUNTIME_OWNER,
            account_group_id=runtime_record.account_group_id,
            trade_date=runtime_record.trade_date,
            event_count=len(self.repository.list_events(runtime_id)),
            algo_instance_ids=tuple(
                item.algo_instance_id for item in self.repository.list_algo_instances(runtime_id, active_only=False)
            ),
            child_order_ids=tuple(item.child_order_id for item in child_orders),
            submitted_child_count=_submitted_child_count(child_orders),
            rejected_child_count=sum(1 for item in child_orders if item.status == MiniQMTChildOrderStatus.REJECTED),
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
        return MiniQMTRuntimeEvidence(
            runtime_id=runtime_id,
            runtime_owner=RUNTIME_OWNER,
            account_group_id=runtime.config.account_group_id,
            trade_date=runtime.config.trade_date,
            event_count=len(self.repository.list_events(runtime_id)),
            algo_instance_ids=tuple(
                item.algo_instance_id for item in self.repository.list_algo_instances(runtime_id, active_only=False)
            ),
            child_order_ids=tuple(item.child_order_id for item in child_orders),
            submitted_child_count=_submitted_child_count(child_orders),
            rejected_child_count=sum(1 for item in child_orders if item.status == MiniQMTChildOrderStatus.REJECTED),
            source=source,
        )

    def _sync_paper_child_status(
        self,
        runtime: MiniQMTExecutionRuntime,
        child: MiniQMTChildOrder,
        result: PaperMiniQMTRuntimeChildResult,
        *,
        preserve_gateway_rejection: bool = False,
    ) -> MiniQMTChildOrder:
        status = _runtime_status_from_paper_result(result)
        metadata = dict(child.metadata)
        if preserve_gateway_rejection and result.submit_error:
            metadata.update(
                {
                    "gateway_rejection_raw": result.submit_error,
                    "gateway_message": _submit_error_message(result.submit_error),
                }
            )
        updated_child = child.model_copy(
            update={
                "status": status,
                "broker_order_id": result.native_context.get("miniqmt_order_id")
                or result.native_context.get("qmt_order_id")
                or child.broker_order_id,
                "metadata": metadata,
            }
        )
        return self.repository.upsert_child_order(updated_child)

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
                "submitted_at": datetime.now(UTC) if managed_result.success and child.submitted_at is None else child.submitted_at,
                "metadata": {
                    **dict(child.metadata),
                    "source": source,
                    "managed_order_result": managed_result.to_dict(),
                    "broker_called": managed_result.broker_called,
                    "broker_synced_child_status": status.value,
                    **({"broker_order_ledger": _ledger_order_payload(ledger_order)} if ledger_order is not None else {}),
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


class PaperV2MiniQMTRuntimeGateway:
    """Gateway boundary that adapts runtime child orders to Paper v2 brokers."""

    def __init__(self, *, broker: BrokerBackend) -> None:
        self.broker = broker
        self.connected_runtime_ids: list[str] = []
        self.submitted_orders: list[MiniQMTChildOrder] = []
        self.cancelled_orders: list[MiniQMTChildOrder] = []
        self._results_by_child_id: dict[str, PaperMiniQMTRuntimeChildResult] = {}

    def connect(self, *, runtime_id: str) -> None:
        self.connected_runtime_ids.append(runtime_id)

    def sync_orders(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def sync_trades(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def sync_positions(self, *, runtime_id: str) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def submit_child_order(self, order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck:
        self.submitted_orders.append(order)
        child_intent = _paper_intent_from_runtime_child(order)
        try:
            handle = self.broker.submit_order_intent(child_intent)
            native = _safe_order_context(self.broker, handle)
            status = self.broker.query_status(handle)
            trades = tuple(dict(row) for row in self.broker.query_trades(handle))
        except TradingCoreError as exc:
            native = _native_from_exception(exc, child_intent=child_intent)
            status = _rejected_status_from_exception(exc, child_order_id=order.child_order_id)
            self._results_by_child_id[order.child_order_id] = PaperMiniQMTRuntimeChildResult(
                parent_intent=child_intent,
                child_intent=child_intent,
                child_order=order,
                handle=None,
                status=status,
                trades=(),
                native_context=native,
                submit_error=exc.to_dict(),
                submit_exception=exc,
            )
            return MiniQMTGatewayOrderAck(
                accepted=False,
                broker_order_id=str(native.get("miniqmt_order_id") or "") or None,
                message=exc.message,
                raw=exc.to_dict(),
            )
        except Exception as exc:  # noqa: BLE001
            submit_error = {
                "error_code": "PAPER_MINIQMT_RUNTIME_GATEWAY_SUBMIT_FAILED",
                "message": "Paper v2 MiniQMT runtime gateway submit failed",
                "context": {"reason": f"{type(exc).__name__}: {exc}"},
            }
            native = {"handle_id": order.child_order_id, "intent_id": child_intent.intent_id}
            status = _rejected_status_from_error(submit_error, child_order_id=order.child_order_id)
            wrapped = BrokerSubmitError(submit_error["message"], context=submit_error["context"])
            self._results_by_child_id[order.child_order_id] = PaperMiniQMTRuntimeChildResult(
                parent_intent=child_intent,
                child_intent=child_intent,
                child_order=order,
                handle=None,
                status=status,
                trades=(),
                native_context=native,
                submit_error=submit_error,
                submit_exception=wrapped,
            )
            return MiniQMTGatewayOrderAck(accepted=False, broker_order_id=None, message=str(exc), raw=submit_error)

        self._results_by_child_id[order.child_order_id] = PaperMiniQMTRuntimeChildResult(
            parent_intent=child_intent,
            child_intent=child_intent,
            child_order=order,
            handle=handle,
            status=status,
            trades=trades,
            native_context=native,
        )
        return MiniQMTGatewayOrderAck(
            accepted=True,
            broker_order_id=str(native.get("miniqmt_order_id") or native.get("qmt_order_id") or handle.handle_id),
            message=str(status.status_msg or "paper v2 MiniQMT runtime gateway accepted child order"),
            raw={"native_context": dict(native), "status": status.model_dump(mode="json")},
        )

    def cancel_child_order(self, order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck:
        self.cancelled_orders.append(order)
        result = self._results_by_child_id.get(order.child_order_id)
        if result is None or result.handle is None:
            return MiniQMTGatewayCancelAck(False, order.broker_order_id, "child order has no paper handle")
        ack = self.broker.cancel(result.handle)
        return MiniQMTGatewayCancelAck(bool(ack.accepted), order.broker_order_id, ack.reason, ack.model_dump(mode="json"))

    def require_result(self, child_order_id: str) -> PaperMiniQMTRuntimeChildResult:
        return self._results_by_child_id[child_order_id]

    def results_for_parent_intent(self, intent_id: str) -> list[PaperMiniQMTRuntimeChildResult]:
        return [
            result
            for result in self._results_by_child_id.values()
            if result.child_intent.metadata.get("parent_intent_id") == intent_id or result.child_intent.intent_id == intent_id
        ]


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


def _paper_runtime_id(run: Any) -> str:
    return f"mqrt_paper_{_short_hash([getattr(run, 'run_id', ''), getattr(run, 'trade_date', '')])}"


def _paper_child_metadata(*, intent: OrderIntent, trade_date: date, source: str) -> dict[str, Any]:
    return {
        "source": source,
        "paper_parent_intent_id": intent.intent_id,
        "paper_intent_id": intent.intent_id,
        "package_id": intent.package_id,
        "portfolio_id": intent.portfolio_id,
        "order_type": intent.order_type.value,
        "limit_price": intent.limit_price,
        "target_trade_date": trade_date.isoformat(),
        "parent_intent_metadata": dict(intent.metadata or {}),
    }


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
    strategy_name = str(intent.metadata.get("strategy_name") or intent.metadata.get("strategy_id") or intent.portfolio_id)
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


def _paper_intent_from_runtime_child(order: MiniQMTChildOrder) -> OrderIntent:
    metadata = dict(order.metadata or {})
    parent_metadata = dict(metadata.get("parent_intent_metadata") or {})
    order_type = OrderType(str(metadata.get("order_type") or (OrderType.LIMIT.value if order.price > 0 else OrderType.MARKET.value)))
    limit_price = float(order.price) if order_type == OrderType.LIMIT else None
    trade_date_raw = metadata.get("target_trade_date")
    trade_date = date.fromisoformat(str(trade_date_raw)) if trade_date_raw else datetime.now(UTC).date()
    return OrderIntent(
        intent_id=str(metadata.get("paper_child_intent_id") or metadata.get("paper_intent_id") or order.child_order_id),
        package_id=str(metadata.get("package_id") or parent_metadata.get("package_id") or "miniqmt_runtime"),
        portfolio_id=str(metadata.get("portfolio_id") or parent_metadata.get("portfolio_id") or order.strategy_slot_id),
        symbol=order.symbol,
        side=order.side,
        quantity=int(order.quantity),
        order_type=order_type,
        limit_price=limit_price,
        target_trade_date=trade_date,
        metadata={
            **parent_metadata,
            "parent_intent_id": metadata.get("paper_parent_intent_id") or order.parent_intent_id,
            "runtime_owner": RUNTIME_OWNER,
            "runtime_id": order.runtime_id,
            "algo_instance_id": order.algo_instance_id,
            "child_order_id": order.child_order_id,
            **{key: value for key, value in metadata.items() if key not in {"parent_intent_metadata"}},
        },
    )


def _safe_order_context(broker: BrokerBackend, handle: OrderHandle) -> dict[str, Any]:
    if hasattr(broker, "order_context"):
        try:
            return dict(broker.order_context(handle))  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            return {"handle_id": handle.handle_id, "intent_id": handle.intent_id, "order_context_error": str(exc)}
    return {"handle_id": handle.handle_id, "intent_id": handle.intent_id}


def _native_from_exception(exc: TradingCoreError, *, child_intent: OrderIntent) -> dict[str, Any]:
    context = exc.context if isinstance(exc.context, dict) else {}
    native = {
        key: context.get(key)
        for key in ("handle_id", "miniqmt_order_id", "strategy_name", "order_remark")
        if context.get(key) is not None
    }
    native.setdefault("handle_id", context.get("handle_id") or f"rejected_{child_intent.intent_id}")
    native.setdefault("intent_id", child_intent.intent_id)
    if context.get("message") is not None:
        native["broker_submit_message"] = context.get("message")
    return native


def _submit_error_message(error: dict[str, Any]) -> str:
    context = error.get("context") if isinstance(error, dict) else None
    if isinstance(context, dict):
        for key in ("message", "reason"):
            value = context.get(key)
            if value:
                return str(value)
    return str(error.get("message") or "MiniQMT child order submit failed")


def _rejected_status_from_exception(exc: TradingCoreError, *, child_order_id: str) -> OrderHandleStatus:
    return OrderHandleStatus(
        handle_id=str(exc.context.get("handle_id") if isinstance(exc.context, dict) else None) or child_order_id,
        state="rejected",
        filled_quantity=0,
        avg_fill_price=None,
        last_event_at=datetime.now(UTC),
        rejection_reason=exc.message,
        raw_status="submit_error",
        status_msg=exc.message,
        raw=exc.to_dict(),
    )


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


def _shared_handle(result: PaperMiniQMTRuntimeChildResult) -> MiniQMTChildOrderHandle | None:
    if result.handle is None:
        return None
    return MiniQMTChildOrderHandle(
        handle_id=result.handle.handle_id,
        intent_id=result.handle.intent_id,
        native_order_id=str(result.native_context.get("miniqmt_order_id") or result.native_context.get("qmt_order_id") or "")
        or None,
        native_context=dict(result.native_context),
    )


def _shared_status(status: OrderHandleStatus | None) -> MiniQMTOrderStatus | None:
    if status is None:
        return None
    return MiniQMTOrderStatus(
        handle_id=status.handle_id,
        state=status.state,
        filled_quantity=int(status.filled_quantity),
        avg_fill_price=status.avg_fill_price,
        last_event_at=status.last_event_at,
        rejection_reason=status.rejection_reason,
        raw_status=status.raw_status,
        status_msg=status.status_msg,
        raw=dict(status.raw),
    )


def _limit_price_for_runtime(
    *,
    intent: OrderIntent,
    quote_provider: Callable[[str], dict[str, Any] | None] | None,
) -> float:
    if intent.limit_price is not None:
        return float(intent.limit_price)
    quote = quote_provider(intent.symbol) if quote_provider is not None else None
    if quote:
        return float(quote.get("ask_price_1") if intent.side == OrderSide.BUY else quote.get("bid_price_1"))
    raise BrokerSubmitError("MiniQMTExecutionRuntime vn.py client requires quote or limit_price")


def _limit_price_for_event_loop(*, intent: OrderIntent, tick_payload: dict[str, Any]) -> float:
    if intent.limit_price is not None:
        price = float(intent.limit_price)
    elif intent.side == OrderSide.BUY:
        price = float(tick_payload.get("ask_price_1") or tick_payload.get("price") or 0)
    else:
        price = float(tick_payload.get("bid_price_1") or tick_payload.get("price") or 0)
    if price <= 0:
        raise BrokerSubmitError(
            "MiniQMT event_loop submit requires positive broker quote price",
            context={
                "reason_code": "MINIQMT_EVENT_LOOP_QUOTE_PRICE_INVALID",
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
            },
        )
    return price


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
        payload.setdefault("price", payload.get("last_price") or payload.get("ask_price_1") or payload.get("bid_price_1"))
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


def _event_loop_child_submit_result(*, child: MiniQMTChildOrder, source: str) -> ManagedOrderSubmitResult:
    accepted = child.status != MiniQMTChildOrderStatus.REJECTED and bool(child.broker_order_id)
    error_code = None if accepted else "MINIQMT_EVENT_LOOP_CHILD_REJECTED"
    preflight = OrderPreflightResult(
        allowed=accepted,
        errors=()
        if accepted
        else (
            OrderPreflightError(
                code=error_code or "MINIQMT_EVENT_LOOP_CHILD_REJECTED",
                message=str(child.metadata.get("gateway_message") or "event_loop child order rejected"),
                context={
                    "runtime_id": child.runtime_id,
                    "child_order_id": child.child_order_id,
                    "parent_intent_id": child.parent_intent_id,
                    "symbol": child.symbol,
                    "source": source,
                },
            ),
        ),
        strategy_id=str(child.metadata.get("strategy_id") or child.strategy_slot_id),
        estimated_notional=Decimal(str(child.price or 0)) * Decimal(int(child.quantity or 0)),
        estimated_fee=Decimal("0"),
        freeze_amount=Decimal("0"),
        available_cash=None,
    )
    return ManagedOrderSubmitResult(
        success=accepted,
        intent_id=child.parent_intent_id,
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


def _terminal_state(results: tuple[PaperMiniQMTRuntimeChildResult, ...]) -> str:
    if not results:
        return "NO_ACTION"
    if any(result.submit_error for result in results):
        return "SUBMIT_REJECTED"
    states = {result.status.state for result in results if result.status is not None}
    if "rejected" in states:
        return "REJECTED"
    if "cancelled" in states:
        return "CANCELLED"
    if "filled" in states:
        return "FILLED"
    if "partial_filled" in states:
        return "PARTIAL"
    return "PENDING"


def _runtime_status_from_paper_result(result: PaperMiniQMTRuntimeChildResult) -> MiniQMTChildOrderStatus:
    if result.submit_error:
        return MiniQMTChildOrderStatus.REJECTED
    if result.status is None:
        return MiniQMTChildOrderStatus.SUBMITTED if result.handle is not None else MiniQMTChildOrderStatus.REJECTED
    if result.status.state == "rejected":
        return MiniQMTChildOrderStatus.REJECTED
    if result.status.state == "cancelled":
        return MiniQMTChildOrderStatus.CANCELLED
    if result.status.state == "filled":
        return MiniQMTChildOrderStatus.FILLED
    if result.status.state == "partial_filled":
        return MiniQMTChildOrderStatus.PARTIALLY_FILLED
    return MiniQMTChildOrderStatus.SUBMITTED if result.handle is not None else MiniQMTChildOrderStatus.REJECTED


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


def _vnpy_runtime_diagnostic(
    *,
    algo_code: str,
    spec_metadata: dict[str, Any],
    intent: OrderIntent,
    execution_policy_context: dict[str, Any],
    tick_payload: dict[str, Any],
    child_results: tuple[PaperMiniQMTRuntimeChildResult, ...],
    runtime_evidence: MiniQMTRuntimeEvidence,
    algo_state: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "miniqmt_vnpy_execution_diagnostic_v1",
        "adapter": "MiniQMTExecutionRuntimeClient",
        "runtime_owner": RUNTIME_OWNER,
        "runtime_evidence": runtime_evidence.to_dict(),
        "execution_algo_code": algo_code,
        "execution_asset_version": spec_metadata.get("asset_version"),
        "execution_policy_id": execution_policy_context.get("validated_execution_policy_id"),
        "execution_policy_sha256": execution_policy_context.get("policy_sha256"),
        "source_attribution": spec_metadata.get("source_attribution"),
        "parent_intent_id": intent.intent_id,
        "symbol": intent.symbol,
        "side": intent.side.value,
        "quantity": intent.quantity,
        "terminal_state": _terminal_state(child_results),
        "quote": dict(tick_payload),
        "actions": [],
        "child_orders": [_runtime_child_payload(result) for result in child_results],
        "algo_state": algo_state,
    }


def _runtime_child_payload(result: PaperMiniQMTRuntimeChildResult) -> dict[str, Any]:
    return {
        "vt_orderid": result.child_order.metadata.get("vnpy_vt_orderid"),
        "handle_id": result.handle.handle_id if result.handle else None,
        "intent_id": result.child_intent.intent_id,
        "symbol": result.child_intent.symbol,
        "side": result.child_intent.side.value,
        "quantity": result.child_intent.quantity,
        "limit_price": result.child_intent.limit_price,
        "submitted_at": result.child_order.submitted_at.isoformat() if result.child_order.submitted_at else None,
        "native_context": dict(result.native_context),
        "status": result.status.model_dump(mode="json") if result.status else None,
        "trades": [dict(row) for row in result.trades],
        "submit_error": result.submit_error,
        "runtime_child_order_id": result.child_order.child_order_id,
        "runtime_algo_instance_id": result.child_order.algo_instance_id,
    }


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
    "PaperMiniQMTRuntimeChildResult",
    "PaperMiniQMTRuntimeSubmitResult",
    "PaperV2MiniQMTRuntimeGateway",
]
