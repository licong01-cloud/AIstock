"""Versioned tail/unfilled handling for simulation execution plans."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle, OrderHandleStatus
from backend.services.trading_core.errors import StrategyPackageValidationError

from .models import ExecutionPlan


SUPPORTED_TAIL_POLICIES = frozenset({"cancel_unfilled_at_close"})


@dataclass(frozen=True)
class TailHandlingOrderResult:
    intent_id: str
    handle_id: str
    symbol: str
    side: str
    state_before_tail: str
    filled_quantity: int
    remaining_quantity: int
    action: str
    success: bool
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent_id": self.intent_id,
            "handle_id": self.handle_id,
            "symbol": self.symbol,
            "side": self.side,
            "state_before_tail": self.state_before_tail,
            "filled_quantity": self.filled_quantity,
            "remaining_quantity": self.remaining_quantity,
            "action": self.action,
            "success": self.success,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class TailHandlingResult:
    policy: str
    policy_version_id: str
    plan_id: str
    intent_count: int
    filled_count: int
    partial_cancelled_count: int
    no_fill_cancelled_count: int
    rejected_count: int
    cancelled_count: int
    failed_count: int
    order_results: tuple[TailHandlingOrderResult, ...]

    @property
    def success(self) -> bool:
        return self.failed_count == 0 and self.rejected_count == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "policy": self.policy,
            "policy_version_id": self.policy_version_id,
            "plan_id": self.plan_id,
            "intent_count": self.intent_count,
            "filled_count": self.filled_count,
            "partial_cancelled_count": self.partial_cancelled_count,
            "no_fill_cancelled_count": self.no_fill_cancelled_count,
            "rejected_count": self.rejected_count,
            "cancelled_count": self.cancelled_count,
            "failed_count": self.failed_count,
            "success": self.success,
            "order_results": [item.to_dict() for item in self.order_results],
        }


class TailHandlingPolicyService:
    """Apply an explicit TailHandlingPolicy to submitted broker order handles."""

    def handle_local_sim_tail(
        self,
        *,
        plan: ExecutionPlan,
        broker: BrokerBackend,
        handles: tuple[OrderHandle, ...] | list[OrderHandle],
    ) -> TailHandlingResult:
        policy = self._tail_policy_name(plan)
        handles_by_intent = {handle.intent_id: handle for handle in handles}
        missing = [intent.intent_id for intent in plan.intents if intent.intent_id not in handles_by_intent]
        if missing:
            raise StrategyPackageValidationError(
                "TailHandlingPolicy requires one broker handle for every execution plan intent",
                context={"plan_id": plan.plan_id, "missing_intent_ids": missing},
            )

        order_results: list[TailHandlingOrderResult] = []
        for intent in plan.intents:
            handle = handles_by_intent[intent.intent_id]
            status = broker.query_status(handle)
            order_results.append(
                self._handle_cancel_unfilled_at_close(
                    plan=plan,
                    intent_symbol=intent.symbol,
                    intent_side=intent.side.value,
                    handle=handle,
                    status=status,
                    broker=broker,
                )
            )
        return self._summarize(policy=policy, plan=plan, order_results=order_results)

    @staticmethod
    def _tail_policy_name(plan: ExecutionPlan) -> str:
        payload = plan.plan_payload_json.get("tail_policy") if isinstance(plan.plan_payload_json, dict) else None
        policy_payload = payload.get("payload") if isinstance(payload, dict) else None
        policy = None
        if isinstance(policy_payload, dict):
            policy = policy_payload.get("policy") or policy_payload.get("unfilled_policy")
        policy_text = str(policy or "").strip()
        if not policy_text:
            raise StrategyPackageValidationError(
                "TailHandlingPolicy execution requires explicit policy payload",
                context={"plan_id": plan.plan_id, "tail_policy_version_id": plan.tail_policy_version_id},
            )
        if policy_text not in SUPPORTED_TAIL_POLICIES:
            raise StrategyPackageValidationError(
                "unsupported TailHandlingPolicy",
                context={
                    "plan_id": plan.plan_id,
                    "tail_policy_version_id": plan.tail_policy_version_id,
                    "policy": policy_text,
                    "supported_policies": sorted(SUPPORTED_TAIL_POLICIES),
                },
            )
        return policy_text

    @staticmethod
    def _handle_cancel_unfilled_at_close(
        *,
        plan: ExecutionPlan,
        intent_symbol: str,
        intent_side: str,
        handle: OrderHandle,
        status: OrderHandleStatus,
        broker: BrokerBackend,
    ) -> TailHandlingOrderResult:
        order_quantity = next(
            intent.order_quantity
            for intent in plan.intents
            if intent.intent_id == handle.intent_id
        )
        remaining = max(0, order_quantity - status.filled_quantity)
        if status.state == "filled":
            return TailHandlingOrderResult(
                intent_id=handle.intent_id,
                handle_id=handle.handle_id,
                symbol=intent_symbol,
                side=intent_side,
                state_before_tail=status.state,
                filled_quantity=status.filled_quantity,
                remaining_quantity=remaining,
                action="NO_ACTION_FILLED",
                success=True,
            )
        if status.state in {"pending", "partial_filled"}:
            ack = broker.cancel(handle)
            action = "CANCEL_UNFILLED" if status.state == "pending" else "CANCEL_REMAINING_AFTER_PARTIAL_FILL"
            return TailHandlingOrderResult(
                intent_id=handle.intent_id,
                handle_id=handle.handle_id,
                symbol=intent_symbol,
                side=intent_side,
                state_before_tail=status.state,
                filled_quantity=status.filled_quantity,
                remaining_quantity=remaining,
                action=action,
                success=bool(ack.accepted),
                reason=ack.reason,
            )
        if status.state == "cancelled":
            return TailHandlingOrderResult(
                intent_id=handle.intent_id,
                handle_id=handle.handle_id,
                symbol=intent_symbol,
                side=intent_side,
                state_before_tail=status.state,
                filled_quantity=status.filled_quantity,
                remaining_quantity=remaining,
                action="ALREADY_CANCELLED",
                success=True,
                reason=status.rejection_reason,
            )
        return TailHandlingOrderResult(
            intent_id=handle.intent_id,
            handle_id=handle.handle_id,
            symbol=intent_symbol,
            side=intent_side,
            state_before_tail=status.state,
            filled_quantity=status.filled_quantity,
            remaining_quantity=remaining,
            action="BROKER_REJECTED",
            success=False,
            reason=status.rejection_reason,
        )

    @staticmethod
    def _summarize(
        *,
        policy: str,
        plan: ExecutionPlan,
        order_results: list[TailHandlingOrderResult],
    ) -> TailHandlingResult:
        return TailHandlingResult(
            policy=policy,
            policy_version_id=plan.tail_policy_version_id,
            plan_id=plan.plan_id,
            intent_count=len(order_results),
            filled_count=sum(1 for item in order_results if item.action == "NO_ACTION_FILLED"),
            partial_cancelled_count=sum(1 for item in order_results if item.action == "CANCEL_REMAINING_AFTER_PARTIAL_FILL"),
            no_fill_cancelled_count=sum(1 for item in order_results if item.action == "CANCEL_UNFILLED"),
            rejected_count=sum(1 for item in order_results if item.action == "BROKER_REJECTED"),
            cancelled_count=sum(1 for item in order_results if item.action == "ALREADY_CANCELLED"),
            failed_count=sum(1 for item in order_results if not item.success),
            order_results=tuple(order_results),
        )
