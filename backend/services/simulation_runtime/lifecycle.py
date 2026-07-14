"""Unified simulation lifecycle orchestration for LocalSim and MiniQMT.

This service owns the broker-neutral chain from daily selection evidence to a
persisted execution plan. Broker execution bridges are invoked only after the
shared plan exists, so LocalSim and MiniQMT cannot diverge in signal/target
generation.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
from datetime import UTC, date, datetime, time
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
from .tca_capture import (
    CaptureMergeOutcome,
    TcaCaptureConfigurationError,
    build_capture_error,
    build_decision_benchmark_capture,
    resolve_tca_benchmark_policy,
)


SCHEDULER_TZ = ZoneInfo("Asia/Shanghai")
SCHEDULER_TZ_NAME = "Asia/Shanghai"
DEFAULT_SCHEDULER_WINDOWS = (
    {"window_id": "pre_open", "label": "\u76d8\u524d", "start": "08:50", "end": "09:10", "action": "readiness"},
    {"window_id": "selection", "label": "\u9009\u80a1", "start": "09:10", "end": "09:20", "action": "selection_evidence"},
    {"window_id": "planning", "label": "\u8c03\u4ed3", "start": "09:20", "end": "09:25", "action": "execution_plan"},
    {
        "window_id": "opening_auction_observe",
        "label": "\u5f00\u76d8\u96c6\u5408\u7ade\u4ef7\u89c2\u5bdf",
        "start": "09:25",
        "end": "09:30",
        "action": "observe_only",
    },
    {
        "window_id": "execution",
        "label": "\u4e0a\u5348\u8fde\u7eed\u7ade\u4ef7",
        "start": "09:30",
        "end": "11:30",
        "action": "submit",
    },
    {
        "window_id": "lunch_recess",
        "label": "\u5348\u95f4\u4f11\u5e02",
        "start": "11:30",
        "end": "13:00",
        "action": "market_wait",
    },
    {
        "window_id": "execution_afternoon",
        "label": "\u4e0b\u5348\u8fde\u7eed\u7ade\u4ef7",
        "start": "13:00",
        "end": "14:57",
        "action": "submit",
    },
    {
        "window_id": "closing_auction_observe",
        "label": "\u6536\u76d8\u96c6\u5408\u7ade\u4ef7\u89c2\u5bdf",
        "start": "14:57",
        "end": "15:00",
        "action": "observe_only",
    },
    {
        "window_id": "post_close_reconcile",
        "label": "Post-close reconcile",
        "start": "15:00",
        "end": "15:30",
        "action": "eod_reconcile",
    },
)
SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW = "SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW"
# Import compatibility only. New durable evidence uses the broker-neutral code.
MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW = SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW
LOGGER = logging.getLogger(__name__)


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
        return MiniQMTExecutionRuntimeKind.EVENT_LOOP
    try:
        kind = raw if isinstance(raw, MiniQMTExecutionRuntimeKind) else MiniQMTExecutionRuntimeKind(str(raw))
    except ValueError as exc:
        raise LiveApprovalRequiredError(
            "unsupported MiniQMT simulation runtime kind",
            context={"reason_code": "MINIQMT_RUNTIME_KIND_UNSUPPORTED", "runtime_kind": str(raw)},
        ) from exc
    if kind == MiniQMTExecutionRuntimeKind.COMPILER:
        raise RuntimeConfigInvalidError(
            "MiniQMT SIM compiler runtime route is retired; SIM submissions must use event_loop",
            context={
                "reason_code": "MINIQMT_SIM_COMPILER_ROUTE_RETIRED",
                "stage": "MINIQMT_RUNTIME_KIND_REJECTED",
                "runtime_kind": kind.value,
                "allowed_runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
            },
        )
    return kind


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
        b0_quote_v2_controller_factory: Any | None = None,
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.target_service = target_service or TargetPositionService()
        self.rebalance_service = rebalance_service or RebalanceIntentService()
        self.plan_compiler = plan_compiler or ExecutionPlanCompiler()
        self.local_bridge = local_bridge or LocalSimExecutionBridge()
        self.b0_quote_v2_controller_factory = b0_quote_v2_controller_factory

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
        self._capture_tca_decision_sidecar(
            run=updated_run,
            binding=binding,
            execution_plan=persisted_plan,
            pre_trade_tradability=pre_trade_tradability,
            execution_policy_payload=execution_policy_payload,
        )
        updated_run = self.repository.get_simulation_daily_run(updated_run.run_id)
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

    def _capture_tca_decision_sidecar(
        self,
        *,
        run: SimulationDailyRun,
        binding: SimulationReleaseBinding,
        execution_plan: ExecutionPlan,
        pre_trade_tradability: dict[str, dict[str, Any]] | None,
        execution_policy_payload: dict[str, Any] | None,
    ) -> None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return
        merger = getattr(self.repository, "merge_run_tca_capture_sidecar", None)
        if not callable(merger):
            LOGGER.error(
                "TCA decision capture unavailable reason_code=ADAPTIVE_IS_TCA_CAPTURE_REPOSITORY_MISSING stage=CAPTURE run_id=%s plan_id=%s",
                run.run_id,
                execution_plan.plan_id,
            )
            return
        decision_event_at = datetime.now(UTC)
        try:
            policy = resolve_tca_benchmark_policy(execution_policy_payload)
        except TcaCaptureConfigurationError as exc:
            for intent in execution_plan.intents:
                outcome = merger(
                    run_id=run.run_id,
                    expected_plan_id=execution_plan.plan_id,
                    expected_plan_hash=execution_plan.plan_hash,
                    parent_intent_id=intent.intent_id,
                    capture_error=build_capture_error(
                        parent_intent_id=intent.intent_id,
                        stage="CAPTURE",
                        reason_code=exc.reason_code,
                        message=str(exc),
                        context={"plan_id": execution_plan.plan_id, "symbol": intent.symbol},
                        occurred_at=decision_event_at,
                    ),
                )
                self._log_tca_capture_outcome(outcome=outcome, parent_intent_id=intent.intent_id, stage="CAPTURE")
            return
        for intent in execution_plan.intents:
            try:
                tradability = (pre_trade_tradability or {}).get(intent.symbol) or {}
                quote_evidence = tradability.get("quote_evidence") if isinstance(tradability, dict) else None
                price_policy = intent.price_policy if isinstance(intent.price_policy, dict) else {}
                capture = build_decision_benchmark_capture(
                    execution_plan_id=execution_plan.plan_id,
                    execution_plan_hash=execution_plan.plan_hash,
                    parent_intent_id=intent.intent_id,
                    symbol=intent.symbol,
                    side=intent.side.value,
                    decision_event_at=decision_event_at,
                    quote_evidence=quote_evidence if isinstance(quote_evidence, dict) else None,
                    policy=policy,
                    strategy_decision_price=price_policy.get("reference_price"),
                    strategy_decision_source="execution_plan.price_policy.reference_price",
                    strategy_decision_time=None,
                    strategy_decision_quality="DIAGNOSTIC_UNTIMED",
                )
                outcome = merger(
                    run_id=run.run_id,
                    expected_plan_id=execution_plan.plan_id,
                    expected_plan_hash=execution_plan.plan_hash,
                    parent_intent_id=intent.intent_id,
                    decision_capture=capture.model_dump(mode="json"),
                )
            except Exception as exc:  # capture is observation-only and must never roll back B0 planning.
                outcome = merger(
                    run_id=run.run_id,
                    expected_plan_id=execution_plan.plan_id,
                    expected_plan_hash=execution_plan.plan_hash,
                    parent_intent_id=intent.intent_id,
                    capture_error=build_capture_error(
                        parent_intent_id=intent.intent_id,
                        stage="CAPTURE",
                        reason_code="ADAPTIVE_IS_TCA_DECISION_CAPTURE_FAILED",
                        message=f"{type(exc).__name__}: {exc}",
                        context={"plan_id": execution_plan.plan_id, "symbol": intent.symbol},
                    ),
                )
            self._log_tca_capture_outcome(outcome=outcome, parent_intent_id=intent.intent_id, stage="CAPTURE")

    @staticmethod
    def _log_tca_capture_outcome(*, outcome: Any, parent_intent_id: str, stage: str) -> None:
        if str(getattr(outcome, "value", outcome)) in {
            CaptureMergeOutcome.CONFLICT.value,
            CaptureMergeOutcome.IDENTITY_DRIFT.value,
            CaptureMergeOutcome.NOT_FOUND.value,
        }:
            LOGGER.error(
                "TCA capture sidecar merge failed reason_code=ADAPTIVE_IS_TCA_CAPTURE_MERGE_%s stage=%s parent_intent_id=%s",
                str(getattr(outcome, "value", outcome)),
                stage,
                parent_intent_id,
            )

    def _capture_tca_first_batch_mapping(
        self,
        *,
        run: SimulationDailyRun,
        execution_plan: ExecutionPlan,
        batch_id: str | None,
    ) -> None:
        if not batch_id:
            return
        merger = getattr(self.repository, "merge_run_tca_capture_sidecar", None)
        if not callable(merger):
            LOGGER.error(
                "TCA batch mapping unavailable reason_code=ADAPTIVE_IS_TCA_CAPTURE_REPOSITORY_MISSING stage=CAPTURE run_id=%s plan_id=%s",
                run.run_id,
                execution_plan.plan_id,
            )
            return
        for intent in execution_plan.intents:
            outcome = merger(
                run_id=run.run_id,
                expected_plan_id=execution_plan.plan_id,
                expected_plan_hash=execution_plan.plan_hash,
                parent_intent_id=intent.intent_id,
                capture_batch_id=batch_id,
            )
            self._log_tca_capture_outcome(outcome=outcome, parent_intent_id=intent.intent_id, stage="CAPTURE")

    def _capture_tca_arrival_attempt_failure(
        self,
        *,
        run: SimulationDailyRun,
        execution_plan: ExecutionPlan,
        exc: BaseException,
    ) -> None:
        context = getattr(exc, "context", None)
        parent_id = str(context.get("intent_id") or "").strip() if isinstance(context, dict) else ""
        if not parent_id:
            LOGGER.error(
                "TCA arrival capture failure has no parent identity reason_code=ADAPTIVE_IS_TCA_PARENT_IDENTITY_MISSING stage=CAPTURE run_id=%s plan_id=%s",
                run.run_id,
                execution_plan.plan_id,
            )
            return
        merger = getattr(self.repository, "merge_run_tca_capture_sidecar", None)
        if not callable(merger):
            LOGGER.error(
                "TCA arrival failure cannot persist reason_code=ADAPTIVE_IS_TCA_CAPTURE_REPOSITORY_MISSING stage=CAPTURE run_id=%s parent_intent_id=%s",
                run.run_id,
                parent_id,
            )
            return
        upstream_reason = str(context.get("reason_code") or "") if isinstance(context, dict) else ""
        outcome = merger(
            run_id=run.run_id,
            expected_plan_id=execution_plan.plan_id,
            expected_plan_hash=execution_plan.plan_hash,
            parent_intent_id=parent_id,
            capture_error=build_capture_error(
                parent_intent_id=parent_id,
                stage="CAPTURE",
                reason_code="ADAPTIVE_IS_TCA_ARRIVAL_CAPTURE_ATTEMPT_FAILED",
                message=f"{type(exc).__name__}: {exc}",
                context={"upstream_reason_code": upstream_reason or None, "plan_id": execution_plan.plan_id},
            ),
        )
        self._log_tca_capture_outcome(outcome=outcome, parent_intent_id=parent_id, stage="CAPTURE")

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
            binder = getattr(local_broker, "bind_execution_plan", None)
            if callable(binder):
                binder(plan=plan, as_of_time=scheduler_time(as_of_time))
            begin_batch = getattr(local_broker, "begin_plan_submission", None)
            commit_batch = getattr(local_broker, "commit_plan_submission", None)
            rollback_batch = getattr(local_broker, "rollback_plan_submission", None)
            batch_started = False
            try:
                if callable(begin_batch):
                    begin_batch(plan_id=plan.plan_id)
                    batch_started = True
                local_result = self.local_bridge.submit_plan(plan=plan, broker=local_broker)
                if batch_started:
                    if not callable(commit_batch):
                        raise RuntimeConfigInvalidError(
                            "LocalSim atomic plan submission is missing commit support",
                            context={"plan_id": plan.plan_id, "run_id": run.run_id},
                        )
                    commit_batch(plan_id=plan.plan_id)
            except Exception as exc:
                if batch_started:
                    if not callable(rollback_batch):
                        raise RuntimeConfigInvalidError(
                            "LocalSim atomic plan submission is missing rollback support",
                            context={"plan_id": plan.plan_id, "run_id": run.run_id},
                        ) from exc
                    rollback_batch(plan_id=plan.plan_id)
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
            bridge = MiniQMTExecutionBridge(
                managed_order_service=managed_order_service,
                b0_quote_v2_controller_factory=self.b0_quote_v2_controller_factory,
            )
            runtime_kind = _normalize_miniqmt_runtime_kind(miniqmt_runtime_kind)
            submit_stage = "MINIQMT_EVENT_LOOP_SUBMIT_FAILED"
            try:
                qmt_result = bridge.submit_event_loop_plan(
                    plan=plan,
                    binding=binding,
                    mode=mode,
                    price_by_symbol=price_by_symbol,
                )
            except Exception as exc:
                self._annotate_event_loop_submit_failure(
                    exc=exc,
                    stage=submit_stage,
                    run=run,
                    plan=plan,
                    binding=binding,
                )
                self._capture_tca_arrival_attempt_failure(
                    run=run,
                    execution_plan=plan,
                    exc=exc,
                )
                self.mark_submit_failure(run=run, stage=submit_stage, exc=exc)
                raise
            broker_called = any(result.broker_called for result in qmt_result.results)
            self._capture_tca_first_batch_mapping(
                run=run,
                execution_plan=plan,
                batch_id=qmt_result.batch_id,
            )
            qmt_batch_payload = qmt_result.to_dict()
            pending_intents = int(qmt_batch_payload.get("pending") or 0)
            failed_intents = int(qmt_batch_payload.get("failed") or qmt_result.failed or 0)
            batch_status = qmt_batch_payload.get("batch_status") or qmt_result.batch_status or ""
            batch_status_value = str(getattr(batch_status, "value", batch_status)).upper().rsplit(".", 1)[-1]
            event_loop_pending = (
                batch_status_value == "SUBMITTING"
                and pending_intents > 0
                and failed_intents == 0
            )
            # BUG-604 semantics: a preflight-approved event-loop parent with no
            # child yet is pending work, not a retryable submit failure.
            next_status = (
                SimulationDailyRunStatus.INTRADAY_RUNNING
                if qmt_result.success or event_loop_pending
                else SimulationDailyRunStatus.FAILED_RETRYABLE
            )
            payload_patch = {
                "broker_called": broker_called,
                "submitted_intents": qmt_result.succeeded,
                "failed_intents": failed_intents,
                "event_loop_pending": event_loop_pending,
                "pending_intents": pending_intents,
                "qmt_batch_id": qmt_result.batch_id,
                "qmt_batch_status": qmt_result.batch_status,
                "qmt_retry_of_batch_id": qmt_result.retry_of_batch_id,
                "qmt_batch_result": qmt_batch_payload,
                "last_stage": next_status.value,
            }
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
                payload_unset=("submit_failure",) if qmt_result.success or event_loop_pending else None,
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
            "reason_code": SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW,
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
                    "stage": SIMULATION_SUBMIT_OUTSIDE_TRADING_WINDOW,
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
