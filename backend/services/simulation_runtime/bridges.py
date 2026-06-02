"""Broker execution bridges for shared simulation runtime plans."""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any

from backend.execution_algos.vnpy_style import VnpyAction, is_vnpy_style_algo
from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle
from backend.services.qmt_strategy_ledger.order_service import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    ManagedBatchSubmitResult,
    ManagedOrderRequest,
    OrderPreflightResult,
    QmtManagedOrderService,
)
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTCancelResult,
    MiniQMTChildOrderHandle,
    MiniQMTChildOrderRequest,
    MiniQMTChildOrderSubmitResult,
    MiniQMTOrderStatus,
    UnifiedMiniQMTVnpyExecutionAdapter,
)
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    BrokerUnavailableError,
    InvalidStateTransitionError,
    LiveApprovalRequiredError,
    MarketDataUnavailableError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

from .models import ExecutionPlan, ExecutionPlanIntent, SimulationReleaseBinding


@dataclass(frozen=True)
class LocalSimPlanSubmitResult:
    order_intents: tuple[OrderIntent, ...]
    handles: tuple[OrderHandle, ...]


@dataclass(frozen=True)
class MiniQMTPlanPreviewResult:
    requests: tuple[ManagedOrderRequest, ...]
    preflights: tuple[OrderPreflightResult, ...]


class LocalSimExecutionBridge:
    """Submit a shared ``ExecutionPlan`` to a LocalSim-compatible broker."""

    def build_order_intents(self, plan: ExecutionPlan) -> list[OrderIntent]:
        return [self._to_order_intent(intent, plan=plan) for intent in plan.intents]

    def submit_plan(self, *, plan: ExecutionPlan, broker: BrokerBackend) -> LocalSimPlanSubmitResult:
        order_intents = self.build_order_intents(plan)
        handles = tuple(broker.submit_order_intent(intent) for intent in order_intents)
        return LocalSimPlanSubmitResult(order_intents=tuple(order_intents), handles=handles)

    @staticmethod
    def _to_order_intent(plan_intent: ExecutionPlanIntent, *, plan: ExecutionPlan) -> OrderIntent:
        order_type = OrderType(str(plan_intent.price_policy.get("order_type") or OrderType.MARKET.value))
        limit_price = plan_intent.price_policy.get("limit_price")
        return OrderIntent(
            intent_id=plan_intent.intent_id,
            package_id=plan_intent.package_id,
            portfolio_id=plan_intent.portfolio_id,
            symbol=plan_intent.symbol,
            side=plan_intent.side,
            quantity=plan_intent.order_quantity,
            order_type=order_type,
            limit_price=float(limit_price) if limit_price is not None else None,
            target_trade_date=plan.target_trade_date,
            metadata={
                **plan_intent.metadata,
                "source_execution_plan_id": plan.plan_id,
                "source_execution_plan_hash": plan.plan_hash,
                "release_id": plan.release_id,
                "release_hash": plan.release_hash,
                "binding_id": plan.binding_id,
                "binding_hash": plan.binding_hash,
                "selection_evidence_id": plan.selection_evidence_id,
                "trading_rule_decision_id": plan_intent.trading_rule_decision_id,
                "rebalance_reason": plan_intent.rebalance_reason,
                "target_quantity": plan_intent.target_quantity,
                "delta_quantity": plan_intent.delta_quantity,
                "current_quantity": plan_intent.current_quantity,
                "current_available_quantity": plan_intent.current_available_quantity,
                "target_weight": plan_intent.target_weight,
            },
        )


class MiniQMTExecutionBridge:
    """Translate shared plans into managed MiniQMT virtual-ledger orders."""

    def __init__(self, *, managed_order_service: QmtManagedOrderService) -> None:
        self._managed_order_service = managed_order_service

    def build_managed_order_requests(
        self,
        *,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        account_id: str | None = None,
        strategy_name: str | None = None,
        order_remark_prefix: str | None = None,
        price_type: int = 5,
        mode: str = "SIM",
        price_by_symbol: dict[str, Decimal | float | int | str] | None = None,
        quote_by_symbol: dict[str, dict[str, Any]] | None = None,
    ) -> list[ManagedOrderRequest]:
        self._validate_plan_binding(plan=plan, binding=binding, mode=mode)
        effective_account = str(account_id or binding.broker_account_id or "").strip()
        effective_strategy_name = str(strategy_name or binding.strategy_name or binding.strategy_id).strip()
        effective_prefix = str(order_remark_prefix or binding.order_remark_prefix or "aistock").strip()
        if not effective_account:
            raise BrokerUnavailableError(
                "MiniQMTExecutionBridge requires broker account_id",
                context={"plan_id": plan.plan_id, "binding_id": binding.binding_id},
            )
        if not effective_strategy_name:
            raise RuntimeConfigInvalidError(
                "MiniQMTExecutionBridge requires strategy_name",
                context={"plan_id": plan.plan_id, "binding_id": binding.binding_id},
            )
        vnpy_policy_context = _vnpy_policy_context_from_plan(plan)
        if vnpy_policy_context is not None:
            return self._build_vnpy_style_managed_order_requests(
                plan=plan,
                binding=binding,
                account_id=effective_account,
                strategy_name=effective_strategy_name,
                order_remark_prefix=effective_prefix,
                price_type=price_type,
                mode=mode,
                price_by_symbol=price_by_symbol or {},
                quote_by_symbol=quote_by_symbol or {},
                policy_context=vnpy_policy_context,
            )
        requests: list[ManagedOrderRequest] = []
        for index, intent in enumerate(plan.intents, start=1):
            side = intent.side.value
            order_type = BUY_ORDER_TYPE if intent.side == OrderSide.BUY else SELL_ORDER_TYPE
            price = self._request_price(intent, price_by_symbol or {})
            requests.append(
                ManagedOrderRequest(
                    account_id=effective_account,
                    strategy_name=effective_strategy_name,
                    symbol=intent.symbol,
                    side=side,
                    order_type=order_type,
                    quantity=intent.order_quantity,
                    price_type=int(price_type),
                    price=price,
                    order_remark=self._order_remark(effective_prefix, plan=plan, intent=intent, index=index),
                    trade_date=plan.target_trade_date,
                    mode=str(mode or "SIM").strip().upper(),
                    package_id=plan.package_id,
                    target_weight=Decimal(str(intent.target_weight)) if intent.target_weight is not None else None,
                    metadata={
                        "source": "shared_execution_plan",
                        "execution_plan_id": plan.plan_id,
                        "execution_plan_hash": plan.plan_hash,
                        "execution_plan_intent_id": intent.intent_id,
                        "release_id": plan.release_id,
                        "release_hash": plan.release_hash,
                        "binding_id": plan.binding_id,
                        "binding_hash": plan.binding_hash,
                        "selection_evidence_id": plan.selection_evidence_id,
                        "selection_evidence_hash": plan.selection_evidence_hash,
                        "trading_rule_decision_id": intent.trading_rule_decision_id,
                        "rebalance_reason": intent.rebalance_reason,
                    },
                )
            )
        return requests

    def preview_plan(self, **kwargs: Any) -> MiniQMTPlanPreviewResult:
        requests = self.build_managed_order_requests(**kwargs)
        preflights = tuple(self._managed_order_service.preview_order(request) for request in requests)
        return MiniQMTPlanPreviewResult(requests=tuple(requests), preflights=preflights)

    def submit_plan(self, **kwargs: Any) -> ManagedBatchSubmitResult:
        requests = self.build_managed_order_requests(**kwargs)
        if not requests:
            raise ArtifactGenerationFailedError("MiniQMTExecutionBridge requires at least one plan intent")
        mode = str(kwargs.get("mode") or "SIM").strip().upper()
        if mode != "SIM":
            raise LiveApprovalRequiredError(
                "MiniQMTExecutionBridge only submits SIM orders; LIVE requires separate approval path",
                context={"mode": mode},
            )
        return self._managed_order_service.submit_batch(requests)

    def _build_vnpy_style_managed_order_requests(
        self,
        *,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        account_id: str,
        strategy_name: str,
        order_remark_prefix: str,
        price_type: int,
        mode: str,
        price_by_symbol: dict[str, Decimal | float | int | str],
        quote_by_symbol: dict[str, dict[str, Any]],
        policy_context: dict[str, Any],
    ) -> list[ManagedOrderRequest]:
        submitter = QmtManagedOrderSubmitter(
            account_id=account_id,
            strategy_name=strategy_name,
            order_remark_prefix=order_remark_prefix,
            price_type=price_type,
            mode=str(mode or "SIM").strip().upper(),
            plan=plan,
            binding=binding,
        )
        adapter = UnifiedMiniQMTVnpyExecutionAdapter(
            submitter=submitter,
            policy_context=policy_context,
            quote_provider=_quote_provider(quote_by_symbol=quote_by_symbol, price_by_symbol=price_by_symbol),
        )
        for intent in plan.intents:
            parent_intent = self._vnpy_parent_order_intent(intent, plan=plan)
            result = adapter.execute_intent(parent_intent, trade_date=plan.target_trade_date)
            submitter.attach_execution_result(result.diagnostic)
        return list(submitter.requests)

    @staticmethod
    def _validate_plan_binding(*, plan: ExecutionPlan, binding: SimulationReleaseBinding, mode: str) -> None:
        if plan.binding_id != binding.binding_id or plan.binding_hash != binding.binding_hash:
            raise InvalidStateTransitionError(
                "execution plan binding does not match MiniQMT simulation binding",
                context={"plan_id": plan.plan_id, "binding_id": binding.binding_id},
            )
        if binding.broker_backend.value != "minqmt_sim":
            raise RuntimeConfigInvalidError(
                "MiniQMTExecutionBridge requires a minqmt_sim binding",
                context={"binding_id": binding.binding_id, "broker_backend": binding.broker_backend.value},
            )
        if str(mode or "SIM").strip().upper() != "SIM":
            raise LiveApprovalRequiredError(
                "MiniQMTExecutionBridge build path currently accepts SIM mode only",
                context={"mode": mode},
            )

    @staticmethod
    def _request_price(intent: ExecutionPlanIntent, price_by_symbol: dict[str, Decimal | float | int | str]) -> Decimal:
        if intent.symbol in price_by_symbol:
            return Decimal(str(price_by_symbol[intent.symbol]))
        if intent.price_policy.get("limit_price") is not None:
            return Decimal(str(intent.price_policy["limit_price"]))
        if intent.price_policy.get("reference_price") is not None:
            return Decimal(str(intent.price_policy["reference_price"]))
        if intent.side == OrderSide.SELL:
            return Decimal("0")
        raise MarketDataUnavailableError(
            "MiniQMT BUY managed order requires reference price or explicit price_by_symbol",
            context={"intent_id": intent.intent_id, "symbol": intent.symbol},
        )

    @staticmethod
    def _order_remark(prefix: str, *, plan: ExecutionPlan, intent: ExecutionPlanIntent, index: int) -> str:
        safe_prefix = prefix[:20] or "aistock"
        return f"{safe_prefix}-{plan.plan_hash[:8]}-{index:02d}-{intent.symbol[:6]}-{intent.side.value[0]}"

    @staticmethod
    def _vnpy_parent_order_intent(plan_intent: ExecutionPlanIntent, *, plan: ExecutionPlan) -> OrderIntent:
        order_type = OrderType(str(plan_intent.price_policy.get("order_type") or OrderType.LIMIT.value))
        limit_price = plan_intent.price_policy.get("limit_price")
        if limit_price is None:
            limit_price = plan_intent.price_policy.get("reference_price")
        return OrderIntent(
            intent_id=plan_intent.intent_id,
            package_id=plan_intent.package_id,
            portfolio_id=plan_intent.portfolio_id,
            symbol=plan_intent.symbol,
            side=plan_intent.side,
            quantity=plan_intent.order_quantity,
            order_type=order_type,
            limit_price=float(limit_price) if limit_price is not None else None,
            target_trade_date=plan.target_trade_date,
            metadata={
                **plan_intent.metadata,
                "source_execution_plan_id": plan.plan_id,
                "source_execution_plan_hash": plan.plan_hash,
                "release_id": plan.release_id,
                "release_hash": plan.release_hash,
                "binding_id": plan.binding_id,
                "binding_hash": plan.binding_hash,
                "selection_evidence_id": plan.selection_evidence_id,
                "selection_evidence_hash": plan.selection_evidence_hash,
                "execution_plan_intent_id": plan_intent.intent_id,
                "trading_rule_decision_id": plan_intent.trading_rule_decision_id,
                "rebalance_reason": plan_intent.rebalance_reason,
                "target_quantity": plan_intent.target_quantity,
                "delta_quantity": plan_intent.delta_quantity,
                "current_quantity": plan_intent.current_quantity,
                "current_available_quantity": plan_intent.current_available_quantity,
                "target_weight": plan_intent.target_weight,
            },
        )


class QmtManagedOrderSubmitter:
    """Collect shared vn.py child actions as virtual-ledger managed requests."""

    def __init__(
        self,
        *,
        account_id: str,
        strategy_name: str,
        order_remark_prefix: str,
        price_type: int,
        mode: str,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
    ) -> None:
        self.account_id = account_id
        self.strategy_name = strategy_name
        self.order_remark_prefix = order_remark_prefix
        self.price_type = price_type
        self.mode = mode
        self.plan = plan
        self.binding = binding
        self.requests: list[ManagedOrderRequest] = []
        self._last_result_start = 0

    def submit_child(self, request: MiniQMTChildOrderRequest) -> MiniQMTChildOrderSubmitResult:
        index = len(self.requests) + 1
        child = request.child_intent
        side = child.side.value
        order_type = BUY_ORDER_TYPE if child.side == OrderSide.BUY else SELL_ORDER_TYPE
        managed_request = ManagedOrderRequest(
            account_id=self.account_id,
            strategy_name=self.strategy_name,
            symbol=child.symbol,
            side=side,
            order_type=order_type,
            quantity=int(child.quantity),
            price_type=int(self.price_type),
            price=Decimal(str(child.limit_price or 0)),
            order_remark=self._child_order_remark(request=request, index=index),
            trade_date=request.trade_date,
            mode=self.mode,
            package_id=child.package_id,
            target_weight=Decimal(str(child.metadata["target_weight"])) if child.metadata.get("target_weight") is not None else None,
            metadata=self._metadata_for_child_request(request=request, index=index),
        )
        self.requests.append(managed_request)
        handle = MiniQMTChildOrderHandle(
            handle_id=f"managed_preview_{managed_request.order_remark}",
            intent_id=child.intent_id,
            native_order_id=None,
            native_context={
                "managed_order_request_index": index,
                "order_remark": managed_request.order_remark,
                "strategy_name": managed_request.strategy_name,
                "request_source": "qmt_managed_order_submitter",
            },
        )
        return MiniQMTChildOrderSubmitResult(
            handle=handle,
            status=MiniQMTOrderStatus(
                handle_id=handle.handle_id,
                state="pending",
                filled_quantity=0,
                raw_status="generated",
                status_msg="generated by UnifiedMiniQMTVnpyExecutionAdapter",
                raw={"managed_order_request": _managed_order_request_payload(managed_request)},
            ),
            trades=[],
            native_context=handle.native_context,
        )

    def cancel_child(self, handle: MiniQMTChildOrderHandle, *, action: VnpyAction, reason: str) -> MiniQMTCancelResult:  # noqa: ARG002
        return MiniQMTCancelResult(
            accepted=False,
            reason="managed request was generated for batch preflight; no broker order exists before submit_batch",
            raw={"handle_id": handle.handle_id, "reason": reason},
        )

    def query_order(self, handle: MiniQMTChildOrderHandle) -> MiniQMTOrderStatus | None:
        return MiniQMTOrderStatus(
            handle_id=handle.handle_id,
            state="pending",
            raw_status="generated",
            status_msg="managed request pending batch submission",
            raw=dict(handle.native_context),
        )

    def query_trades(self, handle: MiniQMTChildOrderHandle) -> list[dict[str, Any]]:  # noqa: ARG002
        return []

    def attach_execution_result(self, diagnostic: dict[str, Any]) -> None:
        for idx in range(self._last_result_start, len(self.requests)):
            request = self.requests[idx]
            self.requests[idx] = replace(
                request,
                metadata={
                    **dict(request.metadata),
                    "vnpy_execution_terminal_state": diagnostic.get("terminal_state"),
                    "vnpy_execution_diagnostic": diagnostic,
                },
            )
        self._last_result_start = len(self.requests)

    def _child_order_remark(self, *, request: MiniQMTChildOrderRequest, index: int) -> str:
        safe_prefix = self.order_remark_prefix[:20] or "aistock"
        symbol = request.child_intent.symbol[:6]
        side = request.child_intent.side.value[0]
        return f"{safe_prefix}-{self.plan.plan_hash[:8]}-vn{index:02d}-{symbol}-{side}"

    def _metadata_for_child_request(self, *, request: MiniQMTChildOrderRequest, index: int) -> dict[str, Any]:
        child = request.child_intent
        return {
            **dict(child.metadata or {}),
            "source": "shared_vnpy_execution_adapter",
            "execution_plan_id": self.plan.plan_id,
            "execution_plan_hash": self.plan.plan_hash,
            "execution_plan_intent_id": request.parent_intent.metadata.get("execution_plan_intent_id") or request.parent_intent.intent_id,
            "release_id": self.plan.release_id,
            "release_hash": self.plan.release_hash,
            "binding_id": self.plan.binding_id,
            "binding_hash": self.plan.binding_hash,
            "selection_evidence_id": self.plan.selection_evidence_id,
            "selection_evidence_hash": self.plan.selection_evidence_hash,
            "strategy_id": self.binding.strategy_id,
            "strategy_name": self.strategy_name,
            "child_order_index": index,
            "vnpy_action": {
                "action_id": request.action.action_id,
                "action_type": request.action.action_type.value,
                "vt_orderid": request.action.vt_orderid,
                "price": request.action.price,
                "volume": request.action.volume,
                "reason": request.action.reason,
            },
        }


def _vnpy_policy_context_from_plan(plan: ExecutionPlan) -> dict[str, Any] | None:
    policy_container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(policy_container, dict):
        return None
    payload = policy_container.get("payload")
    if not isinstance(payload, dict):
        return None
    policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), dict) else payload
    algo_code = str(policy_json.get("algo_code") or payload.get("algo_code") or "").strip().upper()
    if not is_vnpy_style_algo(algo_code):
        return None
    policy_json = {**dict(policy_json), "algo_code": algo_code}
    return {
        "validated_execution_policy_id": str(
            payload.get("validated_execution_policy_id")
            or payload.get("policy_id")
            or policy_container.get("version_id")
            or plan.execution_policy_version_id
        ),
        "policy_sha256": str(
            payload.get("policy_sha256")
            or policy_container.get("sha256")
            or plan.execution_policy_sha256
        ),
        "algo_code": algo_code,
        "policy_json": policy_json,
        "source": "simulation_runtime_execution_plan",
    }


def _quote_provider(
    *,
    quote_by_symbol: dict[str, dict[str, Any]],
    price_by_symbol: dict[str, Decimal | float | int | str],
) -> Any:
    def load(symbol: str) -> dict[str, Any] | None:
        if symbol in quote_by_symbol:
            return dict(quote_by_symbol[symbol])
        if symbol in price_by_symbol:
            price = float(Decimal(str(price_by_symbol[symbol])))
            return {
                "symbol": symbol,
                "bid_price_1": price,
                "bid_volume_1": 10_000_000,
                "ask_price_1": price,
                "ask_volume_1": 10_000_000,
                "source": "price_by_symbol_synthetic_quote",
            }
        return None

    return load


def _managed_order_request_payload(request: ManagedOrderRequest) -> dict[str, Any]:
    return {
        "account_id": request.account_id,
        "strategy_name": request.strategy_name,
        "symbol": request.symbol,
        "side": request.side,
        "order_type": request.order_type,
        "quantity": request.quantity,
        "price_type": request.price_type,
        "price": float(request.price),
        "order_remark": request.order_remark,
        "trade_date": request.trade_date.isoformat(),
        "mode": request.mode,
        "package_id": request.package_id,
        "metadata": dict(request.metadata),
    }
