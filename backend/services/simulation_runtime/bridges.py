"""Broker execution bridges for shared simulation runtime plans."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from typing import Any

from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle
from backend.services.qmt_strategy_ledger.order_service import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    ManagedBatchSubmitResult,
    ManagedOrderRequest,
    OrderPreflightResult,
    QmtManagedOrderService,
)
from backend.services.trading_core.errors import StrategyPackageValidationError
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
    ) -> list[ManagedOrderRequest]:
        self._validate_plan_binding(plan=plan, binding=binding, mode=mode)
        effective_account = str(account_id or binding.broker_account_id or "").strip()
        effective_strategy_name = str(strategy_name or binding.strategy_name or binding.strategy_id).strip()
        effective_prefix = str(order_remark_prefix or binding.order_remark_prefix or "aistock").strip()
        if not effective_account:
            raise StrategyPackageValidationError(
                "MiniQMTExecutionBridge requires broker account_id",
                context={"plan_id": plan.plan_id, "binding_id": binding.binding_id},
            )
        if not effective_strategy_name:
            raise StrategyPackageValidationError(
                "MiniQMTExecutionBridge requires strategy_name",
                context={"plan_id": plan.plan_id, "binding_id": binding.binding_id},
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
            raise StrategyPackageValidationError("MiniQMTExecutionBridge requires at least one plan intent")
        mode = str(kwargs.get("mode") or "SIM").strip().upper()
        if mode != "SIM":
            raise StrategyPackageValidationError(
                "MiniQMTExecutionBridge only submits SIM orders; LIVE requires separate approval path",
                context={"mode": mode},
            )
        return self._managed_order_service.submit_batch(requests)

    @staticmethod
    def _validate_plan_binding(*, plan: ExecutionPlan, binding: SimulationReleaseBinding, mode: str) -> None:
        if plan.binding_id != binding.binding_id or plan.binding_hash != binding.binding_hash:
            raise StrategyPackageValidationError(
                "execution plan binding does not match MiniQMT simulation binding",
                context={"plan_id": plan.plan_id, "binding_id": binding.binding_id},
            )
        if binding.broker_backend.value != "minqmt_sim":
            raise StrategyPackageValidationError(
                "MiniQMTExecutionBridge requires a minqmt_sim binding",
                context={"binding_id": binding.binding_id, "broker_backend": binding.broker_backend.value},
            )
        if str(mode or "SIM").strip().upper() != "SIM":
            raise StrategyPackageValidationError(
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
        raise StrategyPackageValidationError(
            "MiniQMT BUY managed order requires reference price or explicit price_by_symbol",
            context={"intent_id": intent.intent_id, "symbol": intent.symbol},
        )

    @staticmethod
    def _order_remark(prefix: str, *, plan: ExecutionPlan, intent: ExecutionPlanIntent, index: int) -> str:
        safe_prefix = prefix[:20] or "aistock"
        return f"{safe_prefix}-{plan.plan_hash[:8]}-{index:02d}-{intent.symbol[:6]}-{intent.side.value[0]}"
