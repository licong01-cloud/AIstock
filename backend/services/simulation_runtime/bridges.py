"""Broker execution bridges for shared simulation runtime plans."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from backend.execution_algos.vnpy_style import VNPY_STYLE_ASSETS, is_vnpy_style_algo
from backend.services.paper_trading_v2.broker.base import BrokerBackend, OrderHandle
from backend.services.qmt_strategy_ledger.order_service import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    ManagedOrderRequest,
    QmtManagedOrderService,
)
from backend.services.miniqmt_execution_runtime import (
    MiniQMTChildOrder,
    MiniQMTExecutionRuntimeClient,
    MiniQMTPlanPreviewResult,
    MiniQMTRuntimeManagedBatchSubmitResult,
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

from .models import ExecutionPlan, ExecutionPlanIntent, MiniQMTUnsupportedExecutionAlgoError, SimulationReleaseBinding, canonical_json_sha256


MINIQMT_UNSUPPORTED_V25_ALGOS = frozenset({"V25_TWO_STAGE", "V25_1_SMALL_CAP"})
MINIQMT_CASH_SHRINK_MAX_OVERSHOOT_RATIO = Decimal("0.02")
MINIQMT_CASH_SHRINK_MAX_OVERSHOOT_AMOUNT = Decimal("10000")


@dataclass(frozen=True)
class LocalSimExecutionSnapshot:
    orders: tuple[Any, ...]
    fills: tuple[Any, ...]
    events: tuple[Any, ...]
    cash_entries: tuple[Any, ...]
    positions: dict[str, Any]
    account: Any | None = None
    handle_statuses: tuple[Any, ...] = ()


@dataclass(frozen=True)
class LocalSimPlanSubmitResult:
    order_intents: tuple[OrderIntent, ...]
    handles: tuple[OrderHandle, ...]
    execution_snapshot: LocalSimExecutionSnapshot | None = None


class LocalSimExecutionBridge:
    """Submit a shared ``ExecutionPlan`` to a LocalSim-compatible broker."""

    def build_order_intents(self, plan: ExecutionPlan) -> list[OrderIntent]:
        _reject_vnpy_style_for_localsim(plan)
        return [self._to_order_intent(intent, plan=plan) for intent in plan.intents]

    def submit_plan(self, *, plan: ExecutionPlan, broker: BrokerBackend) -> LocalSimPlanSubmitResult:
        order_intents = self.build_order_intents(plan)
        handles = tuple(broker.submit_order_intent(intent) for intent in order_intents)
        return LocalSimPlanSubmitResult(
            order_intents=tuple(order_intents),
            handles=handles,
            execution_snapshot=self._export_execution_snapshot(broker=broker, handles=handles),
        )

    @staticmethod
    def _export_execution_snapshot(*, broker: BrokerBackend, handles: tuple[OrderHandle, ...]) -> LocalSimExecutionSnapshot | None:
        exporter = getattr(broker, "export_execution_snapshot", None)
        if callable(exporter):
            raw = exporter(handles=handles)
            if isinstance(raw, LocalSimExecutionSnapshot):
                return raw
            if isinstance(raw, dict):
                return LocalSimExecutionSnapshot(
                    orders=tuple(raw.get("orders") or ()),
                    fills=tuple(raw.get("fills") or ()),
                    events=tuple(raw.get("events") or ()),
                    cash_entries=tuple(raw.get("cash_entries") or ()),
                    positions=dict(raw.get("positions") or {}),
                    account=raw.get("account"),
                    handle_statuses=tuple(raw.get("handle_statuses") or ()),
                )
        return None

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
    """Runtime-client facade for shared MiniQMT execution plans."""

    def __init__(
        self,
        *,
        managed_order_service: QmtManagedOrderService,
        runtime_client: MiniQMTExecutionRuntimeClient | None = None,
    ) -> None:
        self._managed_order_service = managed_order_service
        self._runtime_client = runtime_client or MiniQMTExecutionRuntimeClient()

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
            build = self._runtime_client.build_managed_vnpy_order_requests(
                parent_intents=[self._vnpy_parent_order_intent(intent, plan=plan) for intent in plan.intents],
                policy_context=vnpy_policy_context,
                account_group_id=self._account_group_id(plan=plan, binding=binding),
                trade_date=plan.target_trade_date,
                runtime_config_hash=self._runtime_config_hash(plan),
                runtime_id=self._runtime_id(plan=plan, binding=binding),
                strategy_slot_id=binding.strategy_slot_id or binding.strategy_id,
                managed_request_factory=self._managed_vnpy_request_factory(
                    plan=plan,
                    binding=binding,
                    account_id=effective_account,
                    strategy_name=effective_strategy_name,
                    order_remark_prefix=effective_prefix,
                    price_type=price_type,
                    mode=mode,
                ),
                quote_provider=_quote_provider(quote_by_symbol=quote_by_symbol or {}, price_by_symbol=price_by_symbol or {}),
                source="simulation_runtime_vnpy_request_build",
            )
            return list(build.requests)
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

    def _build_vnpy_runtime_submission_kwargs(
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
    ) -> dict[str, Any] | None:
        self._validate_plan_binding(plan=plan, binding=binding, mode=mode)
        policy_context = _vnpy_policy_context_from_plan(plan)
        if policy_context is None:
            return None
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
        return {
            "parent_intents": [self._vnpy_parent_order_intent(intent, plan=plan) for intent in plan.intents],
            "policy_context": policy_context,
            "account_group_id": self._account_group_id(plan=plan, binding=binding),
            "trade_date": plan.target_trade_date,
            "runtime_config_hash": self._runtime_config_hash(plan),
            "runtime_id": self._runtime_id(plan=plan, binding=binding),
            "strategy_slot_id": binding.strategy_slot_id or binding.strategy_id,
            "managed_request_factory": self._managed_vnpy_request_factory(
                plan=plan,
                binding=binding,
                account_id=effective_account,
                strategy_name=effective_strategy_name,
                order_remark_prefix=effective_prefix,
                price_type=price_type,
                mode=mode,
            ),
            "quote_provider": _quote_provider(quote_by_symbol=quote_by_symbol or {}, price_by_symbol=price_by_symbol or {}),
        }

    def preview_plan(self, **kwargs: Any) -> MiniQMTPlanPreviewResult:
        plan = kwargs["plan"]
        binding = kwargs["binding"]
        vnpy_kwargs = self._build_vnpy_runtime_submission_kwargs(**kwargs)
        if vnpy_kwargs is not None:
            return self._runtime_client.preview_managed_vnpy_order_requests(
                managed_order_service=self._managed_order_service,
                source="simulation_runtime_vnpy_preview",
                **vnpy_kwargs,
            )
        requests = self.build_managed_order_requests(**kwargs)
        return self._runtime_client.preview_managed_order_requests(
            managed_order_service=self._managed_order_service,
            requests=requests,
            account_group_id=self._account_group_id(plan=plan, binding=binding),
            trade_date=plan.target_trade_date,
            runtime_config_hash=self._runtime_config_hash(plan),
            runtime_id=self._runtime_id(plan=plan, binding=binding),
            source="simulation_runtime_preview",
        )

    def submit_plan(self, **kwargs: Any) -> MiniQMTRuntimeManagedBatchSubmitResult:
        plan = kwargs["plan"]
        binding = kwargs["binding"]
        vnpy_kwargs = self._build_vnpy_runtime_submission_kwargs(**kwargs)
        if vnpy_kwargs is not None:
            mode = str(kwargs.get("mode") or "SIM").strip().upper()
            if mode != "SIM":
                raise LiveApprovalRequiredError(
                    "MiniQMTExecutionBridge only submits SIM orders; LIVE requires separate approval path",
                    context={"mode": mode},
                )
            return self._runtime_client.submit_managed_vnpy_order_requests(
                managed_order_service=self._managed_order_service,
                source="simulation_runtime_vnpy_submit",
                **vnpy_kwargs,
            )
        requests = self.build_managed_order_requests(**kwargs)
        if not requests:
            raise ArtifactGenerationFailedError("MiniQMTExecutionBridge requires at least one plan intent")
        mode = str(kwargs.get("mode") or "SIM").strip().upper()
        if mode != "SIM":
            raise LiveApprovalRequiredError(
                "MiniQMTExecutionBridge only submits SIM orders; LIVE requires separate approval path",
                context={"mode": mode},
            )
        return self._runtime_client.submit_managed_order_requests(
            managed_order_service=self._managed_order_service,
            requests=requests,
            account_group_id=self._account_group_id(plan=plan, binding=binding),
            trade_date=plan.target_trade_date,
            runtime_config_hash=self._runtime_config_hash(plan),
            runtime_id=self._runtime_id(plan=plan, binding=binding),
            source="simulation_runtime_submit",
        )

    def _managed_vnpy_request_factory(
        self,
        *,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        account_id: str,
        strategy_name: str,
        order_remark_prefix: str,
        price_type: int,
        mode: str,
    ) -> Callable[[MiniQMTChildOrder, int], ManagedOrderRequest]:
        def build(child: MiniQMTChildOrder, index: int) -> ManagedOrderRequest:
            child_metadata = dict(child.metadata or {})
            parent_metadata = dict(child_metadata.get("parent_intent_metadata") or {})
            target_weight_raw = parent_metadata.get("target_weight")
            order_type = BUY_ORDER_TYPE if child.side == OrderSide.BUY else SELL_ORDER_TYPE
            metadata = {
                **parent_metadata,
                "source": "runtime_owned_vnpy_algo",
                "runtime_owner": "MiniQMTExecutionRuntime",
                "runtime_id": child.runtime_id,
                "runtime_child_order_id": child.child_order_id,
                "runtime_algo_instance_id": child.algo_instance_id,
                "runtime_parent_intent_id": child.parent_intent_id,
                "execution_plan_id": plan.plan_id,
                "execution_plan_hash": plan.plan_hash,
                "execution_plan_intent_id": parent_metadata.get("execution_plan_intent_id") or child.parent_intent_id,
                "release_id": plan.release_id,
                "release_hash": plan.release_hash,
                "binding_id": plan.binding_id,
                "binding_hash": plan.binding_hash,
                "selection_evidence_id": plan.selection_evidence_id,
                "selection_evidence_hash": plan.selection_evidence_hash,
                "strategy_id": binding.strategy_id,
                "strategy_name": strategy_name,
                "execution_algo_code": child_metadata.get("execution_algo_code"),
                "execution_policy_id": child_metadata.get("execution_policy_id"),
                "execution_policy_sha256": child_metadata.get("execution_policy_sha256"),
                "source_attribution": child_metadata.get("source_attribution"),
                "vnpy_action": {
                    "action_id": child_metadata.get("vnpy_action_id"),
                    "action_type": child_metadata.get("vnpy_action_type"),
                    "vt_orderid": child_metadata.get("vnpy_vt_orderid"),
                    "price": child.price,
                    "volume": child.quantity,
                    "reason": child_metadata.get("vnpy_reason"),
                },
            }
            if child.side == OrderSide.BUY:
                metadata.update(
                    {
                        "miniqmt_cash_preflight_shrink_enabled": True,
                        "miniqmt_cash_shrink_max_overshoot_ratio": str(MINIQMT_CASH_SHRINK_MAX_OVERSHOOT_RATIO),
                        "miniqmt_cash_shrink_max_overshoot": str(MINIQMT_CASH_SHRINK_MAX_OVERSHOOT_AMOUNT),
                    }
                )
            return ManagedOrderRequest(
                account_id=account_id,
                strategy_name=strategy_name,
                symbol=child.symbol,
                side=child.side.value,
                order_type=order_type,
                quantity=int(child.quantity),
                price_type=int(price_type),
                price=Decimal(str(child.price or 0)),
                order_remark=self._vnpy_order_remark(order_remark_prefix, plan=plan, child=child, index=index),
                trade_date=plan.target_trade_date,
                mode=str(mode or "SIM").strip().upper(),
                package_id=child_metadata.get("package_id") or plan.package_id,
                target_weight=Decimal(str(target_weight_raw)) if target_weight_raw is not None else None,
                metadata=metadata,
            )

        return build

    @staticmethod
    def _account_group_id(*, plan: ExecutionPlan, binding: SimulationReleaseBinding) -> str:
        return str(plan.account_group_id or binding.account_group_id or binding.broker_account_id or binding.strategy_id)

    @staticmethod
    def _runtime_config_hash(plan: ExecutionPlan) -> str:
        return canonical_json_sha256(
            {
                "plan_id": plan.plan_id,
                "plan_hash": plan.plan_hash,
                "execution_policy_sha256": plan.execution_policy_sha256,
                "tail_policy_sha256": plan.tail_policy_sha256,
            }
        )

    @staticmethod
    def _runtime_id(*, plan: ExecutionPlan, binding: SimulationReleaseBinding) -> str:
        digest = canonical_json_sha256(
            {
                "binding_id": binding.binding_id,
                "plan_id": plan.plan_id,
                "trade_date": plan.target_trade_date.isoformat(),
            }
        )
        return f"mqrt_sim_{digest[:24]}"

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
        _reject_v25_for_miniqmt_broker_execution(plan)
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
    def _vnpy_order_remark(prefix: str, *, plan: ExecutionPlan, child: MiniQMTChildOrder, index: int) -> str:
        safe_prefix = prefix[:20] or "aistock"
        symbol = child.symbol[:6]
        side = child.side.value[0]
        return f"{safe_prefix}-{plan.plan_hash[:8]}-vn{index:02d}-{symbol}-{side}"

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


def _vnpy_policy_context_from_plan(plan: ExecutionPlan) -> dict[str, Any] | None:
    policy_container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(policy_container, dict):
        return None
    payload = policy_container.get("payload")
    if not isinstance(payload, dict):
        return None
    policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), dict) else payload
    algo_code = str(policy_json.get("algo_code") or payload.get("algo_code") or "").strip().upper()
    inferred_algo_code = _infer_vnpy_algo_code_from_policy_ids(plan=plan, policy_container=policy_container, payload=payload)
    if not algo_code and inferred_algo_code:
        raise RuntimeConfigInvalidError(
            "MiniQMT vn.py-style execution plan requires a full policy_json snapshot",
            context=_policy_error_context(
                plan=plan,
                policy_container=policy_container,
                payload=payload,
                inferred_algo_code=inferred_algo_code,
                broker_backend="minqmt_sim",
            ),
        )
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


def _reject_v25_for_miniqmt_broker_execution(plan: ExecutionPlan) -> None:
    policy_container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(policy_container, dict):
        return
    payload = policy_container.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), dict) else payload
    explicit_algo_code = str(policy_json.get("algo_code") or payload.get("algo_code") or "").strip()
    candidates = (
        (explicit_algo_code,)
        if explicit_algo_code
        else (
            payload.get("validated_execution_policy_id"),
            payload.get("policy_id"),
            payload.get("policy_version_id"),
            policy_container.get("version_id"),
            plan.execution_policy_version_id,
        )
    )
    matched = _infer_unsupported_v25_algo_from_values(candidates)
    if matched is None:
        return
    raise MiniQMTUnsupportedExecutionAlgoError(
        "MiniQMT broker execution does not support V25_* execution algorithms",
        context=_policy_error_context(
            plan=plan,
            policy_container=policy_container,
            payload=payload,
            inferred_algo_code=matched,
            broker_backend="minqmt_sim",
            required_action=(
                "activate SNIPER_MINIQMT, BEST_LIMIT_MINIQMT, TWAP_LITE_MINIQMT, "
                "or another approved MiniQMT vn.py-style execution asset"
            ),
        ),
    )


def _reject_vnpy_style_for_localsim(plan: ExecutionPlan) -> None:
    policy_container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(policy_container, dict):
        return
    payload = policy_container.get("payload")
    if not isinstance(payload, dict):
        return
    policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), dict) else payload
    algo_code = str(policy_json.get("algo_code") or payload.get("algo_code") or "").strip().upper()
    inferred_algo_code = _infer_vnpy_algo_code_from_policy_ids(plan=plan, policy_container=policy_container, payload=payload)
    effective_algo_code = algo_code or inferred_algo_code
    if not is_vnpy_style_algo(effective_algo_code):
        return
    raise RuntimeConfigInvalidError(
        "LocalSim cannot execute MiniQMT vn.py-style execution policy",
        context=_policy_error_context(
            plan=plan,
            policy_container=policy_container,
            payload=payload,
            inferred_algo_code=effective_algo_code,
            broker_backend="local_sim",
            required_action="activate a LocalSim-compatible minute execution policy snapshot or bind this release only to MiniQMT",
        ),
    )


def _infer_vnpy_algo_code_from_policy_ids(
    *,
    plan: ExecutionPlan,
    policy_container: dict[str, Any],
    payload: dict[str, Any],
) -> str | None:
    candidates = (
        payload.get("validated_execution_policy_id"),
        payload.get("policy_id"),
        payload.get("policy_version_id"),
        policy_container.get("version_id"),
        plan.execution_policy_version_id,
    )
    for value in candidates:
        text = str(value or "").strip()
        if not text:
            continue
        inferred = _infer_vnpy_algo_code_from_text(text)
        if inferred:
            return inferred
    return None


def _infer_vnpy_algo_code_from_text(value: str) -> str | None:
    normalized = str(value or "").strip().upper()
    if is_vnpy_style_algo(normalized):
        return normalized
    for segment in normalized.split(":"):
        if is_vnpy_style_algo(segment):
            return segment
    for algo_code in VNPY_STYLE_ASSETS:
        if algo_code in normalized:
            return algo_code
    return None


def _infer_unsupported_v25_algo_from_values(values: tuple[Any, ...]) -> str | None:
    for value in values:
        normalized = str(value or "").strip().upper()
        if not normalized:
            continue
        if normalized in MINIQMT_UNSUPPORTED_V25_ALGOS:
            return normalized
        for segment in normalized.split(":"):
            if segment in MINIQMT_UNSUPPORTED_V25_ALGOS:
                return segment
        for algo_code in MINIQMT_UNSUPPORTED_V25_ALGOS:
            if algo_code in normalized:
                return algo_code
    return None


def _policy_error_context(
    *,
    plan: ExecutionPlan,
    policy_container: dict[str, Any],
    payload: dict[str, Any],
    inferred_algo_code: str | None,
    broker_backend: str,
    required_action: str | None = None,
) -> dict[str, Any]:
    context: dict[str, Any] = {
        "plan_id": plan.plan_id,
        "plan_hash": plan.plan_hash,
        "release_id": plan.release_id,
        "binding_id": plan.binding_id,
        "strategy_id": plan.strategy_id,
        "package_id": plan.package_id,
        "broker_backend": broker_backend,
        "execution_policy_version_id": plan.execution_policy_version_id,
        "execution_policy_sha256": plan.execution_policy_sha256,
        "policy_container_version_id": policy_container.get("version_id"),
        "policy_container_sha256": policy_container.get("sha256"),
        "payload_policy_id": payload.get("validated_execution_policy_id") or payload.get("policy_id") or payload.get("policy_version_id"),
        "payload_policy_sha256": payload.get("policy_sha256"),
        "payload_has_policy_json": isinstance(payload.get("policy_json"), dict),
        "inferred_algo_code": inferred_algo_code,
    }
    if required_action:
        context["required_action"] = required_action
    return context


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

