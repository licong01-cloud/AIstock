"""Runtime client facades for MiniQMT product entry points."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from typing import Any, Callable

from backend.execution_algos.board_lot import board_lot_rule
from backend.execution_algos.vnpy_style import get_vnpy_style_asset, is_vnpy_style_algo
from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle, OrderHandleStatus
from backend.services.qmt_strategy_ledger.order_service import (
    ManagedBatchSubmitResult,
    ManagedOrderRequest,
    ManagedOrderSubmitResult,
    OrderPreflightResult,
    QmtManagedOrderService,
)
from backend.services.trading_core.errors import BrokerSubmitError, TradingCoreError
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTAlgoChildOrder,
    MiniQMTAlgoExecutionResult,
    MiniQMTChildOrderHandle,
    MiniQMTOrderStatus,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

from .gateway import MiniQMTGateway, MiniQMTGatewayCancelAck, MiniQMTGatewayOrderAck
from .models import (
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOperatorCommandResult,
)
from .repository import MiniQMTExecutionRuntimeRepository, default_miniqmt_execution_runtime_repository
from .runtime import MiniQMTExecutionRuntime

RUNTIME_OWNER = "MiniQMTExecutionRuntime"


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

    def __init__(self, *, repository: MiniQMTExecutionRuntimeRepository | None = None) -> None:
        self.repository = repository or default_miniqmt_execution_runtime_repository()

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
        if not requests:
            raise BrokerSubmitError("MiniQMTExecutionRuntime requires at least one managed order request")
        runtime = self._runtime(
            account_group_id=account_group_id,
            trade_date=trade_date,
            runtime_config_hash=runtime_config_hash,
            runtime_id=runtime_id,
            gateway=_PreviewOnlyRuntimeGateway(),
        )
        runtime.start()
        algo_by_remark: dict[str, str] = {}
        for request in requests:
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
            algo_by_remark[request.order_remark] = instance.algo_instance_id

        managed_result = managed_order_service.submit_batch(requests)
        for request, item in zip(requests, managed_result.results, strict=True):
            status = MiniQMTChildOrderStatus.SUBMITTED if item.success else MiniQMTChildOrderStatus.REJECTED
            runtime.record_external_child_order(
                algo_instance_id=algo_by_remark[request.order_remark],
                quantity=int(request.quantity),
                price=float(request.price),
                price_type=int(request.price_type),
                status=status,
                broker_order_id=item.qmt_order_id,
                metadata={
                    "source": source,
                    "managed_order_result": item.to_dict(),
                    "managed_batch_id": managed_result.batch_id,
                    "managed_batch_status": managed_result.batch_status,
                    "order_remark": request.order_remark,
                },
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
        managed_result = managed_order_service.submit_batch(list(build.requests))
        for request, item in zip(build.requests, managed_result.results, strict=True):
            child_order_id = build.child_order_id_by_order_remark.get(request.order_remark)
            if child_order_id:
                self._sync_managed_child_result(
                    runtime_id=build.runtime_evidence.runtime_id,
                    child_order_id=child_order_id,
                    managed_result=item,
                    source=source,
                )
        return MiniQMTRuntimeManagedBatchSubmitResult.from_managed_result(
            managed_result,
            runtime_evidence=self.evidence_for_runtime(build.runtime_evidence.runtime_id, source=source),
        )

    def submit_paper_order_intents(
        self,
        *,
        portfolio: Any,
        run: Any,
        trade_date: date,
        intents: list[OrderIntent],
        broker: BrokerBackend,
        runtime_config_hash: str,
        account_group_id: str,
        strategy_slot_id: str,
        source: str = "paper_v2_direct_miniqmt",
    ) -> PaperMiniQMTRuntimeSubmitResult:
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
        child_results: list[PaperMiniQMTRuntimeChildResult] = []
        for intent in intents:
            instance = runtime.create_algo_instance(
                parent_intent_id=intent.intent_id,
                strategy_slot_id=strategy_slot_id,
                symbol=intent.symbol,
                side=intent.side,
                target_quantity=int(intent.quantity),
                algo_code="PAPER_V2_DIRECT_MINIQMT",
                metadata={"source": source, "paper_parent_intent_id": intent.intent_id},
            )
            child = runtime.submit_child_order(
                algo_instance_id=instance.algo_instance_id,
                quantity=int(intent.quantity),
                price=float(intent.limit_price or 0.0),
                price_type=11 if intent.order_type == OrderType.LIMIT else 5,
                metadata=_paper_child_metadata(intent=intent, trade_date=trade_date, source=source),
            )
            child_result = gateway.require_result(child.child_order_id)
            updated_child = self._sync_paper_child_status(runtime, child, child_result)
            child_results.append(replace(child_result, child_order=updated_child))
        return PaperMiniQMTRuntimeSubmitResult(
            runtime_evidence=self._evidence(runtime, source=source),
            child_results=tuple(child_results),
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
            if result is None or result.status is None:
                continue
            updated_child = self._sync_paper_child_status(runtime, child, result)
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
    ) -> MiniQMTChildOrder:
        status = _runtime_status_from_paper_result(result)
        updated_child = child.model_copy(
            update={
                "status": status,
                "broker_order_id": result.native_context.get("miniqmt_order_id")
                or result.native_context.get("qmt_order_id")
                or child.broker_order_id,
            }
        )
        return self.repository.upsert_child_order(updated_child)

    def _sync_managed_child_result(
        self,
        *,
        runtime_id: str,
        child_order_id: str,
        managed_result: ManagedOrderSubmitResult,
        source: str,
    ) -> MiniQMTChildOrder | None:
        child = _find_child_order(self.repository, runtime_id=runtime_id, child_order_id=child_order_id)
        if child is None:
            return None
        status = MiniQMTChildOrderStatus.SUBMITTED if managed_result.success else MiniQMTChildOrderStatus.REJECTED
        updated_child = child.model_copy(
            update={
                "status": status,
                "broker_order_id": managed_result.qmt_order_id or child.broker_order_id,
                "submitted_at": datetime.now(UTC) if managed_result.success else child.submitted_at,
                "metadata": {
                    **dict(child.metadata),
                    "source": source,
                    "managed_order_result": managed_result.to_dict(),
                    "broker_called": managed_result.broker_called,
                },
            }
        )
        return self.repository.upsert_child_order(updated_child)


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
    """Runtime gateway that converts child orders into qmt_strategy requests.

    The broker call still happens later through QmtManagedOrderService, but the
    vn.py algo instance, child-order identity, and event order are owned by the
    canonical MiniQMTExecutionRuntime before that boundary is reached.
    """

    def __init__(self, *, managed_request_factory: Callable[[MiniQMTChildOrder, int], ManagedOrderRequest]) -> None:
        self.managed_request_factory = managed_request_factory
        self.connected_runtime_ids: list[str] = []
        self.requests: list[ManagedOrderRequest] = []
        self.child_order_id_by_order_remark: dict[str, str] = {}

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
                "managed_order_request": _managed_request_signature(request),
            },
        )

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


def _managed_request_signature(request: ManagedOrderRequest) -> dict[str, Any]:
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
        "metadata": dict(request.metadata),
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
