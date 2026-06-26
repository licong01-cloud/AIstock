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
from backend.services.miniqmt_execution_runtime.shadow import (
    MINIQMT_SHADOW_CANARY_REQUIRED_SCENARIOS,
    MiniQMTShadowCompilerAdapter,
    MiniQMTShadowEventLoopAdapter,
    MiniQMTShadowInputEvent,
    MiniQMTShadowParallelRunner,
    MiniQMTShadowReconciliationReport,
    MiniQMTShadowReconciler,
    MiniQMTShadowScenario,
    build_miniqmt_shadow_scenario_replay_events,
)
from backend.services.trading_core.errors import (
    BrokerUnavailableError,
    InvalidStateTransitionError,
    LiveApprovalRequiredError,
    MarketDataUnavailableError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType

from .models import (
    ExecutionPathNotCanonicalError,
    ExecutionPlan,
    ExecutionPlanIntent,
    MiniQMTUnsupportedExecutionAlgoError,
    SimulationReleaseBinding,
    canonical_json_sha256,
)


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
        build = self._runtime_client.build_managed_vnpy_order_requests(
            parent_intents=[self._vnpy_parent_order_intent(intent, plan=plan) for intent in plan.intents],
            policy_context=self._require_vnpy_policy_context(plan),
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

    @staticmethod
    def _require_vnpy_policy_context(plan: ExecutionPlan) -> dict[str, Any]:
        policy_context = _vnpy_policy_context_from_plan(plan)
        if policy_context is not None:
            return policy_context
        raise ExecutionPathNotCanonicalError(
            "MiniQMT broker execution requires a full approved vn.py-style execution policy snapshot",
            context=_missing_vnpy_policy_context(plan),
        )

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
    ) -> dict[str, Any]:
        self._validate_plan_binding(plan=plan, binding=binding, mode=mode)
        policy_context = self._require_vnpy_policy_context(plan)
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
        vnpy_kwargs = self._build_vnpy_runtime_submission_kwargs(**kwargs)
        return self._runtime_client.preview_managed_vnpy_order_requests(
            managed_order_service=self._managed_order_service,
            source="simulation_runtime_vnpy_preview",
            **vnpy_kwargs,
        )

    def submit_plan(self, **kwargs: Any) -> MiniQMTRuntimeManagedBatchSubmitResult:
        vnpy_kwargs = self._build_vnpy_runtime_submission_kwargs(**kwargs)
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

    def run_shadow_reconciliation(self, **kwargs: Any) -> MiniQMTShadowReconciliationReport:
        reports = self.run_shadow_reconciliations(**kwargs)
        if len(reports) != 1:
            raise RuntimeConfigInvalidError(
                "MiniQMT shadow reconciliation expected exactly one report for single-scenario call",
                context={
                    "reason_code": "MINIQMT_SHADOW_SCENARIO_CARDINALITY_INVALID",
                    "report_count": len(reports),
                    "scenarios": [report.scenario.value for report in reports],
                },
            )
        return reports[0]

    def run_shadow_reconciliations(self, **kwargs: Any) -> list[MiniQMTShadowReconciliationReport]:
        mode = str(kwargs.get("mode") or "SIM").strip().upper()
        if mode != "SIM":
            raise LiveApprovalRequiredError(
                "MiniQMT shadow reconciliation only runs for SIM mode",
                context={"mode": mode, "reason_code": "MINIQMT_SHADOW_SIM_MODE_REQUIRED"},
            )
        scenarios = self._shadow_scenarios(kwargs.get("scenario"), kwargs.get("scenarios"))
        plan = kwargs["plan"]
        binding = kwargs["binding"]
        run = kwargs.get("run")
        vnpy_kwargs = self._build_vnpy_runtime_submission_kwargs(
            plan=plan,
            binding=binding,
            account_id=kwargs.get("account_id"),
            strategy_name=kwargs.get("strategy_name"),
            order_remark_prefix=kwargs.get("order_remark_prefix"),
            price_type=int(kwargs.get("price_type") or 5),
            mode=mode,
            price_by_symbol=kwargs.get("price_by_symbol"),
            quote_by_symbol=kwargs.get("quote_by_symbol"),
        )
        runtime_id = str(vnpy_kwargs["runtime_id"])
        metadata = self._shadow_metadata(
            plan=plan,
            binding=binding,
            vnpy_kwargs=vnpy_kwargs,
            run_id=str(kwargs.get("run_id") or getattr(run, "run_id", "") or ""),
        )
        events = self._shadow_input_events(
            parent_intents=vnpy_kwargs["parent_intents"],
            policy_context=vnpy_kwargs["policy_context"],
            quote_provider=vnpy_kwargs["quote_provider"],
            plan=plan,
            binding=binding,
        )
        runner = MiniQMTShadowParallelRunner(
            reconciler=MiniQMTShadowReconciler(repository=self._runtime_client.repository)
        )
        reports: list[MiniQMTShadowReconciliationReport] = []
        for scenario in scenarios:
            scenario_events = build_miniqmt_shadow_scenario_replay_events(events, scenario=scenario)
            scenario_metadata = {
                **metadata,
                "scenario": scenario.value,
                "scenario_source": "same_intent_shadow_scenario_replay",
            }
            reports.append(
                runner.run(
                    runtime_id=runtime_id,
                    scenario=scenario,
                    input_events=scenario_events,
                    event_loop_adapter=MiniQMTShadowEventLoopAdapter(
                        repository=self._runtime_client.repository,
                        runtime_config_hash=str(vnpy_kwargs["runtime_config_hash"]),
                        account_group_id=str(vnpy_kwargs["account_group_id"]),
                        trade_date=plan.target_trade_date,
                    ),
                    compiler_adapter=MiniQMTShadowCompilerAdapter(
                        repository=self._runtime_client.repository,
                        runtime_config_hash=str(vnpy_kwargs["runtime_config_hash"]),
                        account_group_id=str(vnpy_kwargs["account_group_id"]),
                        trade_date=plan.target_trade_date,
                    ),
                    metadata=scenario_metadata,
                )
            )
        return reports

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
    def _shadow_scenarios(raw_scenario: Any, raw_scenarios: Any) -> tuple[MiniQMTShadowScenario, ...]:
        if raw_scenario is not None and raw_scenarios is not None:
            raise RuntimeConfigInvalidError(
                "MiniQMT shadow reconciliation accepts either scenario or scenarios, not both",
                context={"reason_code": "MINIQMT_SHADOW_SCENARIO_ARGUMENT_CONFLICT"},
            )
        if raw_scenario is None and raw_scenarios is None:
            raise RuntimeConfigInvalidError(
                "MiniQMT shadow reconciliation requires an explicit scenario or scenario set",
                context={"reason_code": "MINIQMT_SHADOW_SCENARIO_REQUIRED"},
            )
        values = raw_scenarios if raw_scenarios is not None else (raw_scenario,)
        if isinstance(values, str | MiniQMTShadowScenario):
            values = (values,)
        try:
            scenarios = tuple(
                value if isinstance(value, MiniQMTShadowScenario) else MiniQMTShadowScenario(str(value))
                for value in values
            )
        except TypeError as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT shadow scenarios must be an iterable of scenario values",
                context={"reason_code": "MINIQMT_SHADOW_SCENARIO_ARGUMENT_INVALID", "raw_scenarios": repr(raw_scenarios)},
            ) from exc
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT shadow scenario is unsupported",
                context={
                    "reason_code": "MINIQMT_SHADOW_SCENARIO_UNSUPPORTED",
                    "raw_scenario": repr(raw_scenario if raw_scenario is not None else raw_scenarios),
                },
            ) from exc
        if not scenarios:
            raise RuntimeConfigInvalidError(
                "MiniQMT shadow scenarios must not be empty",
                context={"reason_code": "MINIQMT_SHADOW_SCENARIO_EMPTY"},
            )
        return tuple(dict.fromkeys(scenarios))

    @staticmethod
    def required_canary_shadow_scenarios() -> tuple[MiniQMTShadowScenario, ...]:
        return MINIQMT_SHADOW_CANARY_REQUIRED_SCENARIOS

    @staticmethod
    def _shadow_metadata(
        *,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        vnpy_kwargs: dict[str, Any],
        run_id: str,
    ) -> dict[str, Any]:
        policy_context = dict(vnpy_kwargs["policy_context"])
        policy_json = policy_context.get("policy_json")
        return {
            "schema_version": "miniqmt_shadow_reconciliation_metadata_v1",
            "source": "simulation_runtime_miniqmt_shadow_runner",
            "shadow_mode": "dry_run_no_broker_mutation",
            "portfolio_id": plan.portfolio_id,
            "strategy_slot_id": str(vnpy_kwargs["strategy_slot_id"]),
            "binding_id": binding.binding_id,
            "run_id": run_id,
            "trade_date": plan.target_trade_date.isoformat(),
            "execution_plan_id": plan.plan_id,
            "execution_plan_hash": plan.plan_hash,
            "account_group_id": str(vnpy_kwargs["account_group_id"]),
            "runtime_config_hash": str(vnpy_kwargs["runtime_config_hash"]),
            "strategy_id": binding.strategy_id,
            "package_id": plan.package_id,
            "broker_backend": binding.broker_backend.value,
            "validated_execution_policy_id": policy_context.get("validated_execution_policy_id"),
            "policy_sha256": policy_context.get("policy_sha256"),
            "policy_json": dict(policy_json) if isinstance(policy_json, dict) else {},
        }

    @staticmethod
    def _shadow_input_events(
        *,
        parent_intents: list[OrderIntent],
        policy_context: dict[str, Any],
        quote_provider: Callable[[str], dict[str, Any] | None],
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
    ) -> list[MiniQMTShadowInputEvent]:
        policy_json = policy_context.get("policy_json")
        events: list[MiniQMTShadowInputEvent] = [
            MiniQMTShadowInputEvent(
                event_type="policy",
                payload={
                    "policy_json": dict(policy_json) if isinstance(policy_json, dict) else {},
                    "validated_execution_policy_id": policy_context.get("validated_execution_policy_id"),
                    "policy_sha256": policy_context.get("policy_sha256"),
                    "source": "simulation_runtime_execution_plan",
                },
            )
        ]
        intent_by_id = {intent.intent_id: intent for intent in plan.intents}
        decision_by_id = {decision.decision_id: decision for decision in plan.trading_rule_decisions}
        for parent in parent_intents:
            plan_intent = intent_by_id.get(parent.intent_id)
            decision = decision_by_id.get(plan_intent.trading_rule_decision_id) if plan_intent is not None else None
            events.append(
                MiniQMTShadowInputEvent(
                    event_type="parent_intent",
                    payload={
                        "intent_id": parent.intent_id,
                        "symbol": parent.symbol,
                        "side": parent.side.value,
                        "quantity": int(parent.quantity),
                        "order_type": parent.order_type.value,
                        "limit_price": parent.limit_price,
                        "package_id": parent.package_id,
                        "portfolio_id": parent.portfolio_id,
                        "strategy_id": binding.strategy_id,
                        "strategy_slot_id": binding.strategy_slot_id or binding.strategy_id,
                        "metadata": dict(parent.metadata),
                    },
                )
            )
            events.append(
                MiniQMTShadowInputEvent(
                    event_type="tick",
                    payload=MiniQMTExecutionBridge._shadow_tick_payload(
                        parent,
                        quote_provider=quote_provider,
                        plan_intent=plan_intent,
                        decision=decision,
                    ),
                )
            )
        return events

    @staticmethod
    def _shadow_tick_payload(
        intent: OrderIntent,
        *,
        quote_provider: Callable[[str], dict[str, Any] | None],
        plan_intent: ExecutionPlanIntent | None,
        decision: Any | None,
    ) -> dict[str, Any]:
        quote = quote_provider(intent.symbol)
        if quote:
            payload = dict(quote)
            payload.setdefault("symbol", intent.symbol)
            payload.setdefault("price", payload.get("last_price") or payload.get("ask_price_1") or payload.get("bid_price_1"))
        else:
            price = float(intent.limit_price or 0.0)
            if price <= 0:
                raise MarketDataUnavailableError(
                    "MiniQMT shadow reconciliation requires quote or positive limit_price",
                    context={
                        "reason_code": "MINIQMT_SHADOW_QUOTE_MISSING",
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                    },
                )
            payload = {
                "symbol": intent.symbol,
                "price": price,
                "last_price": price,
                "bid_price_1": price,
                "bid_volume_1": int(intent.quantity),
                "ask_price_1": price,
                "ask_volume_1": int(intent.quantity),
                "source": "runtime_synthetic_limit_quote",
            }
        payload["source_execution_plan_intent_id"] = intent.intent_id
        if plan_intent is not None:
            payload["trading_rule_decision_id"] = plan_intent.trading_rule_decision_id
        if decision is not None:
            tradability = decision.price_limit_rule.get("pre_trade_tradability")
            if isinstance(tradability, dict):
                payload["pre_trade_tradability"] = dict(tradability)
        return payload

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


def _missing_vnpy_policy_context(plan: ExecutionPlan) -> dict[str, Any]:
    policy_container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(policy_container, dict):
        policy_container = {}
    payload = policy_container.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    policy_json = payload.get("policy_json") if isinstance(payload.get("policy_json"), dict) else payload
    explicit_algo_code = str(policy_json.get("algo_code") or payload.get("algo_code") or "").strip().upper() or None
    inferred_algo_code = explicit_algo_code or _infer_vnpy_algo_code_from_policy_ids(
        plan=plan,
        policy_container=policy_container,
        payload=payload,
    )
    return _policy_error_context(
        plan=plan,
        policy_container=policy_container,
        payload=payload,
        inferred_algo_code=inferred_algo_code,
        broker_backend="minqmt_sim",
        required_action=(
            "activate SNIPER_MINIQMT, BEST_LIMIT_MINIQMT, TWAP_LITE_MINIQMT, "
            "or another approved MiniQMT vn.py-style execution asset"
        ),
    )


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

