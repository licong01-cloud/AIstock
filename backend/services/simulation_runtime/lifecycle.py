"""Unified simulation lifecycle orchestration for LocalSim and MiniQMT.

This service owns the broker-neutral chain from daily selection evidence to a
persisted execution plan. Broker execution bridges are invoked only after the
shared plan exists, so LocalSim and MiniQMT cannot diverge in signal/target
generation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from backend.services.paper_trading_v2.broker.base import BrokerBackend
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.selection_center.models import SignalSnapshot, TargetPosition
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import (
    BrokerUnavailableError,
    InvalidStateTransitionError,
    LiveApprovalRequiredError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import PositionLot
from backend.services.miniqmt_execution_runtime import MiniQMTExecutionRuntimeKind

from .bridges import LocalSimExecutionBridge, MiniQMTExecutionBridge
from .decision import (
    TRADABILITY_BLOCK_REASON_CODES,
    ExecutionPlanCompiler,
    RebalanceIntentResult,
    RebalanceIntentService,
    TargetPositionService,
)
from .models import (
    DailySelectionEvidence,
    ExecutionPlan,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository


SCHEDULER_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULER_TZ_NAME = "Asia/Shanghai"
DEFAULT_SCHEDULER_WINDOWS = (
    {"window_id": "pre_open", "label": "\u76d8\u524d", "start": "08:50", "end": "09:10", "action": "readiness"},
    {"window_id": "selection", "label": "\u9009\u80a1", "start": "09:10", "end": "09:20", "action": "selection_evidence"},
    {"window_id": "planning", "label": "\u8c03\u4ed3", "start": "09:20", "end": "09:25", "action": "execution_plan"},
    {"window_id": "execution", "label": "\u76d8\u4e2d/\u5c3e\u76d8", "start": "09:25", "end": "15:00", "action": "submit"},
    {
        "window_id": "post_close_reconcile",
        "label": "Post-close reconcile",
        "start": "15:00",
        "end": "15:30",
        "action": "eod_reconcile",
    },
)
MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW = "MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW"


def scheduler_now() -> datetime:
    return datetime.now(SCHEDULER_TZ)


def scheduler_time(value: datetime | None) -> datetime:
    if value is None:
        return scheduler_now()
    if value.tzinfo is None:
        return value.replace(tzinfo=SCHEDULER_TZ)
    return value.astimezone(SCHEDULER_TZ)


def compute_schedule_windows(*, trade_date: date, as_of_time: datetime | None) -> tuple[dict[str, Any], ...]:
    local_as_of = scheduler_time(as_of_time) if as_of_time is not None else None
    current_dt = local_as_of.replace(second=0, microsecond=0) if local_as_of is not None else None
    windows: list[dict[str, Any]] = []
    for window in DEFAULT_SCHEDULER_WINDOWS:
        start = datetime.combine(trade_date, time.fromisoformat(window["start"]), tzinfo=SCHEDULER_TZ)
        end = datetime.combine(trade_date, time.fromisoformat(window["end"]), tzinfo=SCHEDULER_TZ)
        state = "PENDING"
        if current_dt is not None:
            if current_dt < start:
                state = "UPCOMING"
            elif start <= current_dt < end:
                state = "ACTIVE"
            else:
                state = "COMPLETED"
        windows.append(
            {
                **window,
                "trade_date": trade_date.isoformat(),
                "state": state,
                "timezone": SCHEDULER_TZ_NAME,
                "start_at": start.isoformat(),
                "end_at": end.isoformat(),
            }
        )
    return tuple(windows)


def active_submit_window(*, trade_date: date, as_of_time: datetime) -> tuple[dict[str, Any] | None, tuple[dict[str, Any], ...]]:
    windows = compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)
    active = next((item for item in windows if item["state"] == "ACTIVE"), None)
    if active is not None and str(active.get("action") or "") == "submit":
        return active, windows
    return None, windows


@dataclass(frozen=True)
class SimulationPlanBuildResult:
    run: SimulationDailyRun
    runtime_release: StrategyRuntimeRelease
    binding: SimulationReleaseBinding
    selection_evidence: DailySelectionEvidence
    signal_snapshot: SignalSnapshot
    target_positions: tuple[TargetPosition, ...]
    rebalance: RebalanceIntentResult
    execution_plan: ExecutionPlan


@dataclass(frozen=True)
class SimulationExecutionResult:
    run: SimulationDailyRun
    execution_plan: ExecutionPlan
    broker_backend: SimulationBrokerBackend
    status: str
    intent_count: int
    broker_result: Any | None = None


def _normalize_miniqmt_runtime_kind(
    raw: MiniQMTExecutionRuntimeKind | str | None,
) -> MiniQMTExecutionRuntimeKind:
    if raw is None:
        return MiniQMTExecutionRuntimeKind.COMPILER
    try:
        return raw if isinstance(raw, MiniQMTExecutionRuntimeKind) else MiniQMTExecutionRuntimeKind(str(raw))
    except ValueError as exc:
        raise LiveApprovalRequiredError(
            "unsupported MiniQMT simulation runtime kind",
            context={"reason_code": "MINIQMT_RUNTIME_KIND_UNSUPPORTED", "runtime_kind": str(raw)},
        ) from exc


def _pre_trade_blocked_symbol_count(pre_trade_tradability: dict[str, dict[str, Any]] | None) -> int:
    if not pre_trade_tradability:
        return 0
    return sum(
        1
        for status in pre_trade_tradability.values()
        if isinstance(status, dict) and not bool(status.get("is_tradable", True))
    )


def _pre_trade_blocked_order_generation_payload(plan: ExecutionPlan) -> dict[str, Any] | None:
    blocked = []
    for decision in plan.trading_rule_decisions:
        if decision.reason_code not in TRADABILITY_BLOCK_REASON_CODES and decision.reason_code != "SUSPENDED_OR_NO_QUOTE_BLOCKED":
            continue
        tradability = decision.price_limit_rule.get("pre_trade_tradability")
        blocked.append(
            {
                "symbol": decision.symbol,
                "side": decision.side.value,
                "requested_quantity": int(decision.requested_quantity),
                "legal_quantity": int(decision.legal_quantity),
                "reason_code": decision.reason_code,
                "pre_trade_tradability": tradability if isinstance(tradability, dict) else None,
            }
        )
    if not blocked:
        return None
    return {
        "schema_version": "pre_trade_blocked_order_generation_v1",
        "reason_code": "SUSPENDED_OR_NO_QUOTE_BLOCKED",
        "blocked_intent_count": len(blocked),
        "blocked_symbols": sorted({item["symbol"] for item in blocked}),
        "blocked_orders": blocked,
    }


def _target_equity_basis_payload(
    *,
    binding: SimulationReleaseBinding,
    target_total_equity: float | None,
    target_equity_context: dict[str, Any] | None,
) -> dict[str, Any]:
    if target_total_equity is None:
        return {
            "schema_version": "simulation_target_equity_basis_v1",
            "source": "binding.capital_allocation",
            "total_equity": float(binding.capital_allocation),
            "capital_allocation": float(binding.capital_allocation),
        }
    payload = dict(target_equity_context or {})
    payload.setdefault("schema_version", "simulation_target_equity_basis_v1")
    payload.setdefault("source", "dynamic_account_equity")
    payload["total_equity"] = float(target_total_equity)
    payload["capital_allocation"] = float(binding.capital_allocation)
    return payload


class SimulationLifecycleOrchestrator:
    """Build and submit one broker-neutral simulation day lifecycle."""

    def __init__(
        self,
        *,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        target_service: TargetPositionService | None = None,
        rebalance_service: RebalanceIntentService | None = None,
        plan_compiler: ExecutionPlanCompiler | None = None,
        local_bridge: LocalSimExecutionBridge | None = None,
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.target_service = target_service or TargetPositionService()
        self.rebalance_service = rebalance_service or RebalanceIntentService()
        self.plan_compiler = plan_compiler or ExecutionPlanCompiler()
        self.local_bridge = local_bridge or LocalSimExecutionBridge()

    def build_execution_plan(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        selection_evidence: DailySelectionEvidence,
        signal_snapshot: SignalSnapshot,
        current_positions: dict[str, PositionLot] | None = None,
        current_prices: dict[str, float] | None = None,
        pre_trade_tradability: dict[str, dict[str, Any]] | None = None,
        manifest: StrategyPackageManifest | None = None,
        portfolio_id: str | None = None,
        top_k: int | None = None,
        execution_policy_payload: dict[str, Any] | None = None,
        tail_policy_payload: dict[str, Any] | None = None,
        target_total_equity: float | None = None,
        target_equity_context: dict[str, Any] | None = None,
        created_by: str | None = None,
    ) -> SimulationPlanBuildResult:
        self._validate_release_binding(runtime_release=runtime_release, binding=binding)
        self._validate_trade_date(selection_evidence=selection_evidence, signal_snapshot=signal_snapshot)
        self.repository.save_daily_selection_evidence(selection_evidence)
        run = self._create_or_load_run(
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=selection_evidence,
            created_by=created_by,
        )
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.TARGET_GENERATING,
            selection_evidence=selection_evidence,
            payload_patch={"last_stage": "TARGET_GENERATING"},
        )
        targets = self.target_service.build_target_positions(
            selection_evidence=selection_evidence,
            signal_snapshot=signal_snapshot,
            runtime_release=runtime_release,
            binding=binding,
            total_equity=target_total_equity if target_total_equity is not None else binding.capital_allocation,
            top_k=top_k,
            manifest=manifest,
            current_positions=current_positions or {},
            current_prices=current_prices or {},
        )
        rebalance = self.rebalance_service.build_order_intents(
            package_id=runtime_release.package_id,
            portfolio_id=str(portfolio_id or binding.strategy_id),
            strategy_id=binding.strategy_id,
            trade_date=selection_evidence.target_trade_date,
            current_positions=current_positions or {},
            target_positions=targets,
            pre_trade_tradability=pre_trade_tradability,
        )
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.PLANNING_EXECUTION,
            payload_patch={
                "target_count": len(targets),
                "order_intent_count": len(rebalance.order_intents),
                "trading_rule_decision_count": len(rebalance.trading_rule_decisions),
                "pre_trade_blocked_symbol_count": _pre_trade_blocked_symbol_count(pre_trade_tradability),
                "target_equity_basis": _target_equity_basis_payload(
                    binding=binding,
                    target_total_equity=target_total_equity,
                    target_equity_context=target_equity_context,
                ),
                "last_stage": "PLANNING_EXECUTION",
            },
        )
        plan = self.plan_compiler.compile_plan(
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=selection_evidence,
            order_intents=rebalance.order_intents,
            trading_rule_decisions=rebalance.trading_rule_decisions,
            portfolio_id=str(portfolio_id or binding.strategy_id),
            execution_policy_payload=execution_policy_payload,
            tail_policy_payload=tail_policy_payload,
        )
        persisted_plan = self.repository.save_execution_plan(plan)
        updated_run = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.PLANNING_EXECUTION,
            execution_plan=persisted_plan,
            payload_patch={"execution_plan_intent_count": len(persisted_plan.intents)},
        )
        return SimulationPlanBuildResult(
            run=updated_run,
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=selection_evidence,
            signal_snapshot=signal_snapshot,
            target_positions=tuple(targets),
            rebalance=rebalance,
            execution_plan=persisted_plan,
        )

    def build_batch_execution_plans(self, requests: Iterable[dict[str, Any]]) -> list[SimulationPlanBuildResult]:
        return [self.build_execution_plan(**request) for request in requests]

    def submit_execution_plan(
        self,
        *,
        build_result: SimulationPlanBuildResult,
        local_broker: BrokerBackend | None = None,
        managed_order_service: QmtManagedOrderService | None = None,
        mode: str = "SIM",
        price_by_symbol: dict[str, Any] | None = None,
        miniqmt_runtime_kind: MiniQMTExecutionRuntimeKind | str | None = None,
        as_of_time: datetime | None = None,
    ) -> SimulationExecutionResult:
        return self.submit_persisted_execution_plan(
            run=build_result.run,
            binding=build_result.binding,
            execution_plan=build_result.execution_plan,
            local_broker=local_broker,
            managed_order_service=managed_order_service,
            mode=mode,
            price_by_symbol=price_by_symbol,
            miniqmt_runtime_kind=miniqmt_runtime_kind,
            as_of_time=as_of_time,
        )

    def submit_persisted_execution_plan(
        self,
        *,
        run: SimulationDailyRun,
        binding: SimulationReleaseBinding,
        execution_plan: ExecutionPlan,
        local_broker: BrokerBackend | None = None,
        managed_order_service: QmtManagedOrderService | None = None,
        mode: str = "SIM",
        price_by_symbol: dict[str, Any] | None = None,
        miniqmt_runtime_kind: MiniQMTExecutionRuntimeKind | str | None = None,
        as_of_time: datetime | None = None,
    ) -> SimulationExecutionResult:
        """Submit an already-persisted plan exactly once after restart recovery."""
        plan = execution_plan
        if not plan.intents:
            blocked_payload = _pre_trade_blocked_order_generation_payload(plan)
            terminal_payload = (
                {
                    "no_rebalance_required": False,
                    "broker_called": False,
                    "pre_trade_blocked_order_generation": blocked_payload,
                    "last_stage": "SUCCEEDED",
                }
                if blocked_payload
                else {"no_rebalance_required": True, "broker_called": False, "last_stage": "SUCCEEDED"}
            )
            succeeded = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.SUCCEEDED,
                payload_patch=terminal_payload,
                payload_unset=("submit_failure",),
            )
            return SimulationExecutionResult(
                run=succeeded,
                execution_plan=plan,
                broker_backend=binding.broker_backend,
                status="PRE_TRADE_BLOCKED" if blocked_payload else "NO_REBALANCE",
                intent_count=0,
                broker_result=None,
            )

        self._assert_within_submit_window(
            run=run,
            binding=binding,
            execution_plan=plan,
            as_of_time=as_of_time,
        )
        run = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.SUBMITTING,
            payload_patch={"last_stage": "SUBMITTING"},
        )

        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            if local_broker is None:
                self.mark_submit_failure(
                    run=run,
                    stage="LOCAL_SIM_BROKER_UNAVAILABLE",
                    exc=BrokerUnavailableError(
                        "LocalSim execution requires an injected LocalSim broker",
                        context={"run_id": run.run_id, "plan_id": plan.plan_id},
                    ),
                )
                raise BrokerUnavailableError(
                    "LocalSim execution requires an injected LocalSim broker",
                    context={"run_id": run.run_id, "plan_id": plan.plan_id},
                )
            try:
                local_result = self.local_bridge.submit_plan(plan=plan, broker=local_broker)
            except Exception as exc:
                self.mark_submit_failure(run=run, stage="LOCAL_SIM_SUBMIT_FAILED", exc=exc)
                raise
            submitted = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.INTRADAY_RUNNING,
                payload_patch={
                    "broker_called": True,
                    "submitted_intents": len(local_result.order_intents),
                    "broker_order_handles": [
                        handle.model_dump(mode="json") for handle in local_result.handles
                    ],
                    "local_sim_synchronous_terminal": True,
                    "last_stage": "LOCAL_SIM_DURABLE_PERSISTENCE_PENDING",
                    "submit_failure": None,
                },
                payload_unset=("submit_failure",),
            )
            return SimulationExecutionResult(
                run=submitted,
                execution_plan=plan,
                broker_backend=binding.broker_backend,
                status="SUBMITTED",
                intent_count=len(plan.intents),
                broker_result=local_result,
            )

        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            if managed_order_service is None:
                self.mark_submit_failure(
                    run=run,
                    stage="MINIQMT_SERVICE_UNAVAILABLE",
                    exc=BrokerUnavailableError(
                        "MiniQMT execution requires QmtManagedOrderService",
                        context={"run_id": run.run_id, "plan_id": plan.plan_id},
                    ),
                )
                raise BrokerUnavailableError(
                    "MiniQMT execution requires QmtManagedOrderService",
                    context={"run_id": run.run_id, "plan_id": plan.plan_id},
                )
            bridge = MiniQMTExecutionBridge(managed_order_service=managed_order_service)
            runtime_kind = _normalize_miniqmt_runtime_kind(miniqmt_runtime_kind)
            submitter = (
                bridge.submit_event_loop_plan
                if runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP
                else bridge.submit_plan
            )
            submit_stage = (
                "MINIQMT_EVENT_LOOP_SUBMIT_FAILED"
                if runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP
                else "MINIQMT_SUBMIT_FAILED"
            )
            try:
                qmt_result = submitter(
                    plan=plan,
                    binding=binding,
                    mode=mode,
                    price_by_symbol=price_by_symbol,
                )
            except Exception as exc:
                if runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP:
                    self._annotate_event_loop_submit_failure(
                        exc=exc,
                        stage=submit_stage,
                        run=run,
                        plan=plan,
                        binding=binding,
                    )
                self.mark_submit_failure(run=run, stage=submit_stage, exc=exc)
                raise
            next_status = SimulationDailyRunStatus.INTRADAY_RUNNING if qmt_result.success else SimulationDailyRunStatus.FAILED_RETRYABLE
            broker_called = any(result.broker_called for result in qmt_result.results)
            payload_patch = {
                "broker_called": broker_called,
                "submitted_intents": qmt_result.succeeded,
                "failed_intents": qmt_result.failed,
                "qmt_batch_id": qmt_result.batch_id,
                "qmt_batch_status": qmt_result.batch_status,
                "qmt_retry_of_batch_id": qmt_result.retry_of_batch_id,
                "qmt_batch_result": qmt_result.to_dict(),
                "last_stage": next_status.value,
            }
            if runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP:
                payload_patch.update(
                    {
                        "miniqmt_runtime_kind": runtime_kind.value,
                        "miniqmt_runtime_route": {
                            "route": "A_EVENT_LOOP",
                            "runtime_kind": runtime_kind.value,
                            "gateway_class": "QmtClientMiniQMTEventLoopGateway",
                            "oms_authority": "qmt_strategy_ledger",
                            "quote_source": "MINIQMT_REALTIME.broker_quote",
                            "reason_code": "MINIQMT_EVENT_LOOP_ROUTE_SELECTED",
                        },
                    }
                )
            updated = self.repository.update_simulation_daily_run(
                run.run_id,
                status=next_status,
                payload_patch=payload_patch,
                payload_unset=("submit_failure",) if qmt_result.success else None,
            )
            return SimulationExecutionResult(
                run=updated,
                execution_plan=plan,
                broker_backend=binding.broker_backend,
                status="SUBMITTED" if qmt_result.success else "BROKER_PRECHECK_FAILED",
                intent_count=len(plan.intents),
                broker_result=qmt_result,
            )

        raise RuntimeConfigInvalidError(
            "unsupported simulation broker backend",
            context={"broker_backend": binding.broker_backend.value},
        )

    def _assert_within_submit_window(
        self,
        *,
        run: SimulationDailyRun,
        binding: SimulationReleaseBinding,
        execution_plan: ExecutionPlan,
        as_of_time: datetime | None,
    ) -> None:
        local_as_of = scheduler_time(as_of_time)
        active_window, windows = active_submit_window(trade_date=run.trade_date, as_of_time=local_as_of)
        if active_window is not None:
            return
        active_non_submit = next((item for item in windows if item["state"] == "ACTIVE"), None)
        intent_count = len(execution_plan.intents)
        previous_broker_called = bool(run.run_payload_json.get("broker_called"))
        try:
            previous_submitted_intents = int(run.run_payload_json.get("submitted_intents") or 0)
        except (TypeError, ValueError):
            previous_submitted_intents = 0
        payload = {
            "schema_version": "simulation_submit_window_gate_v1",
            "reason_code": MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW,
            "reason": "real_broker_submit_outside_execution_window_rejected",
            "run_id": run.run_id,
            "plan_id": execution_plan.plan_id,
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "broker_backend": binding.broker_backend.value,
            "trade_date": run.trade_date.isoformat(),
            "as_of_time": local_as_of.isoformat(),
            "schedule_timezone": SCHEDULER_TZ_NAME,
            "active_window": active_non_submit,
            "schedule_windows": list(windows),
            "blocked_intent_count": intent_count,
            "blocked_buy_intent_count": sum(1 for intent in execution_plan.intents if str(intent.side.value).upper() == "BUY"),
            "blocked_sell_intent_count": sum(1 for intent in execution_plan.intents if str(intent.side.value).upper() == "SELL"),
            "durable_residual": True,
            "broker_called_by_gate": False,
            "broker_called_before_rejection": previous_broker_called,
            "submitted_intents_before_rejection": previous_submitted_intents,
            "next_action": (
                "retry only from the shared scheduler submit window or terminalize via broker-authoritative "
                "post-close/cross-day reconciliation; do not silently submit after close"
            ),
        }
        exc = RuntimeConfigInvalidError(
            "simulation broker submit rejected outside the configured execution window",
            context=payload,
        )
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "broker_called": previous_broker_called,
                "submitted_intents": previous_submitted_intents,
                "failed_intents": intent_count,
                "submit_window_gate": payload,
                "durable_residual": payload,
                "submit_failure": {
                    "stage": MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW,
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": payload,
                },
            },
        )
        raise exc

    @staticmethod
    def _annotate_event_loop_submit_failure(
        *,
        exc: BaseException,
        stage: str,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
    ) -> None:
        context = getattr(exc, "context", None)
        if not isinstance(context, dict):
            return
        context.setdefault("stage", stage)
        context.setdefault("run_id", run.run_id)
        context.setdefault("plan_id", plan.plan_id)
        context.setdefault("binding_id", binding.binding_id)
        context.setdefault("strategy_id", binding.strategy_id)
        context.setdefault("broker_backend", binding.broker_backend.value)
        context.setdefault("broker_called", False)
        context.setdefault("submitted_intents", 0)
        context.setdefault("failed_intents", len(plan.intents))

    def mark_submit_failure(self, *, run: SimulationDailyRun, stage: str, exc: BaseException) -> SimulationDailyRun:
        context = getattr(exc, "context", None)
        payload_patch: dict[str, Any] = {
            "last_stage": "FAILED_RETRYABLE",
            "submit_failure": {
                "stage": (
                    str(context.get("stage"))
                    if isinstance(context, dict) and context.get("stage")
                    else stage
                ),
                "outer_stage": stage,
                "type": type(exc).__name__,
                "message": str(exc),
                "context": context if isinstance(context, dict) else None,
            },
        }
        if isinstance(context, dict):
            for key in ("broker_called", "submitted_intents", "failed_intents"):
                if key in context:
                    payload_patch[key] = context[key]
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch=payload_patch,
        )

    def _create_or_load_run(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        selection_evidence: DailySelectionEvidence,
        created_by: str | None,
    ) -> SimulationDailyRun:
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=selection_evidence.target_trade_date,
        )
        if existing is not None:
            return existing
        identity = {
            "schema_version": "simulation_daily_run_identity_v1",
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "binding_hash": binding.binding_hash,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "broker_backend": binding.broker_backend.value,
            "trade_date": selection_evidence.target_trade_date.isoformat(),
        }
        digest = canonical_json_sha256(identity)
        return self.repository.save_simulation_daily_run(
            SimulationDailyRun(
                run_id=f"simrun_{digest[:16]}",
                trade_date=selection_evidence.target_trade_date,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                package_id=runtime_release.package_id,
                manifest_sha256=runtime_release.manifest_sha256,
                release_id=runtime_release.release_id,
                release_hash=runtime_release.release_hash or "",
                binding_id=binding.binding_id,
                binding_hash=binding.binding_hash or "",
                selection_evidence_id=selection_evidence.evidence_id,
                selection_artifact_hash=selection_evidence.artifact_hash,
                status=SimulationDailyRunStatus.CREATED,
                run_payload_json={**identity, "created_by": created_by},
            )
        )

    @staticmethod
    def _validate_release_binding(*, runtime_release: StrategyRuntimeRelease, binding: SimulationReleaseBinding) -> None:
        if binding.release_id != runtime_release.release_id or binding.release_hash != runtime_release.release_hash:
            raise InvalidStateTransitionError(
                "SimulationReleaseBinding does not match StrategyRuntimeRelease",
                context={
                    "release_id": runtime_release.release_id,
                    "binding_release_id": binding.release_id,
                    "binding_id": binding.binding_id,
                },
            )

    @staticmethod
    def _validate_trade_date(*, selection_evidence: DailySelectionEvidence, signal_snapshot: SignalSnapshot) -> None:
        if selection_evidence.target_trade_date != signal_snapshot.trade_date:
            raise InvalidStateTransitionError(
                "selection evidence target_trade_date does not match signal snapshot trade_date",
                context={
                    "evidence_id": selection_evidence.evidence_id,
                    "target_trade_date": selection_evidence.target_trade_date.isoformat(),
                    "snapshot_trade_date": signal_snapshot.trade_date.isoformat(),
                },
            )
