"""Scheduler entry point for unified LocalSim and MiniQMT simulation runs.

The scheduler is intentionally broker-neutral until ``submit=True`` is passed.
It drives StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan for
eligible SimulationReleaseBinding rows and reuses persisted plans on restart so
that a backend tick cannot duplicate orders.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import Any, Protocol

from backend.services.paper_trading_v2.broker.base import BrokerBackend
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.selection_center.models import SelectionMode, SignalSnapshot
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import PositionLot

from .lifecycle import SimulationExecutionResult, SimulationLifecycleOrchestrator, SimulationPlanBuildResult
from .models import (
    ExecutionPlan,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
)
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository
from .selection import StrategyPackageSelectionResult, StrategyPackageSelectionService
from .performance import StrategyPerformanceProjectionService
from .tail import TailHandlingPolicyService


DEFAULT_SCHEDULER_APPROVAL_STATES = (
    SimulationBindingApprovalState.SIM_VALIDATING,
    SimulationBindingApprovalState.SIM_PASSED,
    SimulationBindingApprovalState.LIVE_APPROVAL_PENDING,
    SimulationBindingApprovalState.LIVE_APPROVED,
)

DEFAULT_SCHEDULER_WINDOWS = (
    {"window_id": "pre_open", "label": "盘前", "start": "08:50", "end": "09:10", "action": "readiness"},
    {"window_id": "selection", "label": "选股", "start": "09:10", "end": "09:20", "action": "selection_evidence"},
    {"window_id": "planning", "label": "调仓", "start": "09:20", "end": "09:25", "action": "execution_plan"},
    {"window_id": "execution", "label": "盘中/尾盘", "start": "09:25", "end": "15:00", "action": "submit"},
)

logger = logging.getLogger("aistock.simulation_runtime.scheduler")


@dataclass(frozen=True)
class SimulationRunContext:
    """Authoritative run context supplied by the target broker/account adapter."""

    current_positions: dict[str, PositionLot]
    current_prices: dict[str, float] = field(default_factory=dict)
    portfolio_id: str | None = None
    manifest: StrategyPackageManifest | None = None
    top_k: int | None = None
    execution_policy_payload: dict[str, Any] | None = None
    tail_policy_payload: dict[str, Any] | None = None
    local_broker: BrokerBackend | None = None
    managed_order_service: QmtManagedOrderService | None = None
    qmt_sync_service: QmtStrategyLedgerSyncService | None = None
    qmt_reconciliation_service: QmtStrategyLedgerReconciliationService | None = None
    qmt_ledger_repository: Any | None = None
    broker_positions: list[dict[str, Any]] = field(default_factory=list)
    tail_policy_service: TailHandlingPolicyService | None = None
    price_by_symbol: dict[str, Any] | None = None
    cash: float | None = None
    frozen_cash: float = 0.0
    realized_pnl: float = 0.0


class SimulationRunContextProvider(Protocol):
    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        """Return current positions/prices and optional broker services for one binding."""


class FailFastSimulationRunContextProvider:
    """Default provider that prevents silent empty-position or empty-price success."""

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        raise DataUnavailableError(
            "simulation lifecycle scheduler requires an explicit run context provider",
            context={
                "release_id": runtime_release.release_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )


class StaticSimulationRunContextProvider:
    """Explicit static provider for deterministic tests and controlled validation."""

    def __init__(
        self,
        *,
        by_binding_id: dict[str, SimulationRunContext] | None = None,
        by_strategy_id: dict[str, SimulationRunContext] | None = None,
    ) -> None:
        self._by_binding_id = dict(by_binding_id or {})
        self._by_strategy_id = dict(by_strategy_id or {})

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        if binding.binding_id in self._by_binding_id:
            return self._by_binding_id[binding.binding_id]
        if binding.strategy_id in self._by_strategy_id:
            return self._by_strategy_id[binding.strategy_id]
        raise DataUnavailableError(
            "static simulation run context is missing binding context",
            context={
                "release_id": runtime_release.release_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )


class ProductionSimulationRunContextProvider:
    """Production provider that loads positions, prices, and broker services from
    persisted state and live market data.

    Accepts callable factories so operators can inject the real LocalSim/MiniQMT
    backends without the provider itself importing broker-specific modules.
    """

    def __init__(
        self,
        *,
        position_loader: Callable[[str, date], dict[str, PositionLot]] | None = None,
        price_loader: Callable[[list[str], date], dict[str, float]] | None = None,
        local_broker_factory: Callable[[str], BrokerBackend] | None = None,
        managed_order_service_factory: Callable[[], QmtManagedOrderService] | None = None,
        qmt_sync_service_factory: Callable[[], QmtStrategyLedgerSyncService] | None = None,
        qmt_reconciliation_service_factory: Callable[[], QmtStrategyLedgerReconciliationService] | None = None,
        qmt_ledger_repository: Any | None = None,
    ) -> None:
        self._position_loader = position_loader or _default_position_loader
        self._price_loader = price_loader or _default_price_loader
        self._local_broker_factory = local_broker_factory
        self._managed_order_service_factory = managed_order_service_factory
        self._qmt_sync_service_factory = qmt_sync_service_factory
        self._qmt_reconciliation_service_factory = qmt_reconciliation_service_factory
        self._qmt_ledger_repository = qmt_ledger_repository

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        strategy_id = binding.strategy_id
        try:
            positions = self._position_loader(strategy_id, trade_date)
        except Exception:
            positions = {}
            logger.warning(
                "ProductionSimulationRunContextProvider: position load failed for strategy_id=%s trade_date=%s",
                strategy_id,
                trade_date.isoformat(),
                exc_info=True,
            )
        symbols = list(positions.keys())
        try:
            prices = self._price_loader(symbols, trade_date)
        except Exception:
            prices = {}
            logger.warning(
                "ProductionSimulationRunContextProvider: price load failed for strategy_id=%s",
                strategy_id,
                exc_info=True,
            )

        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            local_broker = self._local_broker_factory(strategy_id) if self._local_broker_factory else None
            return SimulationRunContext(
                current_positions=positions,
                current_prices=prices,
                portfolio_id=binding.strategy_id,
                local_broker=local_broker,
            )

        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            managed_order_service = self._managed_order_service_factory() if self._managed_order_service_factory else None
            qmt_sync_service = self._qmt_sync_service_factory() if self._qmt_sync_service_factory else None
            qmt_reconciliation_service = self._qmt_reconciliation_service_factory() if self._qmt_reconciliation_service_factory else None
            return SimulationRunContext(
                current_positions=positions,
                current_prices=prices,
                portfolio_id=binding.strategy_id,
                managed_order_service=managed_order_service,
                qmt_sync_service=qmt_sync_service,
                qmt_reconciliation_service=qmt_reconciliation_service,
                qmt_ledger_repository=self._qmt_ledger_repository,
            )

        raise DataUnavailableError(
            "ProductionSimulationRunContextProvider: unsupported broker backend",
            context={
                "broker_backend": binding.broker_backend.value,
                "strategy_id": strategy_id,
            },
        )


def _default_position_loader(strategy_id: str, trade_date: date) -> dict[str, PositionLot]:
    return {}


def _default_price_loader(symbols: list[str], trade_date: date) -> dict[str, float]:
    return {}


@dataclass(frozen=True)
class SimulationSchedulerBindingResult:
    binding_id: str
    strategy_id: str
    broker_backend: SimulationBrokerBackend
    status: str
    run: SimulationDailyRun | None = None
    execution_plan: ExecutionPlan | None = None
    execution_result: SimulationExecutionResult | None = None
    sync_result: dict[str, Any] | None = None
    reconciliation_result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None

    @property
    def is_success(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class SimulationSchedulerRunOnceResult:
    trade_date: date
    data_source: str
    submit: bool
    total_bindings: int
    results: tuple[SimulationSchedulerBindingResult, ...]
    as_of_time: datetime | None = None
    schedule_windows: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def planned_count(self) -> int:
        return sum(1 for item in self.results if item.status == "PLANNED")

    @property
    def reused_count(self) -> int:
        return sum(1 for item in self.results if item.status == "REUSED_EXISTING_PLAN")

    @property
    def submitted_count(self) -> int:
        return sum(
            1
            for item in self.results
            if item.status
            in {
                "SUBMITTED",
                "RECONCILED",
                "TAIL_HANDLED",
                "RECONCILIATION_WARNING",
                "NO_REBALANCE",
            }
        )

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.results if item.error is not None)


class SimulationLifecycleScheduler:
    """Run one unattended lifecycle tick for eligible simulation bindings."""

    def __init__(
        self,
        *,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        selection_service: StrategyPackageSelectionService | Any | None = None,
        orchestrator: SimulationLifecycleOrchestrator | None = None,
        context_provider: SimulationRunContextProvider | None = None,
        performance_service: StrategyPerformanceProjectionService | None = None,
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.selection_service = selection_service or StrategyPackageSelectionService(repository=self.repository)
        self.orchestrator = orchestrator or SimulationLifecycleOrchestrator(repository=self.repository)
        self.context_provider = context_provider or FailFastSimulationRunContextProvider()
        self.performance_service = performance_service or StrategyPerformanceProjectionService()

    def status(self) -> dict[str, Any]:
        return {
            "ok": True,
            "scheduler": "simulation_lifecycle_scheduler",
            "autostart": False,
            "default_submit": False,
            "approval_states": [state.value for state in DEFAULT_SCHEDULER_APPROVAL_STATES],
            "manual_tick_endpoint_enabled": False,
            "schedule_windows": list(DEFAULT_SCHEDULER_WINDOWS),
            "restart_recovery_mode": "persisted_state_only",
            "window_orchestration": {
                "pre_open": "readiness",
                "selection": "daily_selection_evidence",
                "planning": "execution_plan",
                "execution": "submit_and_reconcile",
            },
        }

    def run_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        release_id: str | None = None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None = DEFAULT_SCHEDULER_APPROVAL_STATES,
        submit: bool = False,
        mode: str = "SIM",
        as_of_time: datetime | None = None,
        created_by: str = "simulation_lifecycle_scheduler",
        raise_on_error: bool = False,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        bindings = self.repository.list_simulation_release_bindings(
            strategy_id=strategy_id,
            release_id=release_id,
            broker_backend=broker_backend,
            approval_states=approval_states,
            active_on=trade_date,
            limit=limit,
        )
        results: list[SimulationSchedulerBindingResult] = []
        for binding in bindings:
            try:
                results.append(
                    self._run_binding(
                        binding=binding,
                        trade_date=trade_date,
                        data_source=data_source,
                        submit=submit,
                        mode=mode,
                        created_by=created_by,
                    )
                )
            except Exception as exc:
                if raise_on_error:
                    raise
                results.append(
                    SimulationSchedulerBindingResult(
                        binding_id=binding.binding_id,
                        strategy_id=binding.strategy_id,
                        broker_backend=binding.broker_backend,
                        status="FAILED",
                        error={
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "context": getattr(exc, "context", None),
                        },
                    )
                )
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=submit,
            total_bindings=len(bindings),
            results=tuple(results),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    def _run_binding(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
    ) -> SimulationSchedulerBindingResult:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is not None and existing.execution_plan_id:
            return self._existing_plan_result(
                binding=binding,
                run=existing,
                trade_date=trade_date,
                submit=submit,
                mode=mode,
            )

        context = self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        selection = self.selection_service.run_selection(
            package_ids=[binding.package_id],
            mode=SelectionMode.SINGLE_PACKAGE,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config={},
            runtime_release=runtime_release,
            created_by=created_by,
        )
        build_result = self._build_plan_from_selection(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            selection=selection,
            context=context,
            created_by=created_by,
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="PLANNED",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
            )

        sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
        execution = self.orchestrator.submit_execution_plan(
            build_result=build_result,
            local_broker=context.local_broker,
            managed_order_service=context.managed_order_service,
            mode=mode,
            price_by_symbol=context.price_by_symbol,
        )
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=latest_run,
            execution_plan=execution.execution_plan,
            execution_result=execution,
            sync_result=sync_result,
            reconciliation_result=reconciliation,
        )

    def _existing_plan_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        trade_date: date,
        submit: bool,
        mode: str,
    ) -> SimulationSchedulerBindingResult:
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        status = "REUSED_EXISTING_PLAN"
        if run.status == SimulationDailyRunStatus.SUCCEEDED and not plan.intents:
            status = "NO_REBALANCE"
        if self._should_submit_existing_plan(run, plan=plan, submit=submit):
            runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
            context = self.context_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )
            sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
            execution = self.orchestrator.submit_persisted_execution_plan(
                run=run,
                binding=binding,
                execution_plan=plan,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol,
            )
            tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
            reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=status,
                run=latest_run,
                execution_plan=plan,
                execution_result=execution,
                sync_result=sync_result,
                reconciliation_result=reconciliation,
            )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=run,
            execution_plan=plan,
        )

    def _persist_strategy_performance(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        marks = self._performance_marks(context)
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and context.qmt_ledger_repository is not None:
            projection = self.performance_service.project_from_qmt_strategy_ledger(
                strategy_id=binding.strategy_id,
                repository=context.qmt_ledger_repository,
                marks=marks,
            )
        else:
            projection = self.performance_service.project_strategy(
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                initial_capital=float(binding.capital_allocation),
                cash=float(context.cash if context.cash is not None else binding.capital_allocation),
                frozen_cash=float(context.frozen_cash),
                realized_pnl=float(context.realized_pnl),
                positions=context.current_positions,
                marks=marks,
            )
        payload = projection.to_dict()
        self.repository.update_simulation_daily_run(
            run.run_id,
            payload_patch={
                "strategy_performance": payload,
                "performance_projection": payload,
            },
        )
        return payload

    @staticmethod
    def _performance_marks(context: SimulationRunContext) -> dict[str, float]:
        marks = dict(context.current_prices or {})
        if context.price_by_symbol:
            marks.update({symbol: float(price) for symbol, price in context.price_by_symbol.items()})
        return marks

    @staticmethod
    def _should_submit_existing_plan(run: SimulationDailyRun, *, plan: ExecutionPlan, submit: bool) -> bool:
        return (
            submit
            and bool(plan.intents)
            and run.status
            in {
                SimulationDailyRunStatus.CREATED,
                SimulationDailyRunStatus.PRECHECKING,
                SimulationDailyRunStatus.SIGNAL_GENERATING,
                SimulationDailyRunStatus.TARGET_GENERATING,
                SimulationDailyRunStatus.PLANNING_EXECUTION,
                SimulationDailyRunStatus.FAILED_RETRYABLE,
            }
            and not run.run_payload_json.get("broker_called")
        )

    def _sync_before_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        sync_service = getattr(context, "qmt_sync_service", None)
        if sync_service is None:
            raise DataUnavailableError(
                "MiniQMT simulation submit requires sync-before-submit service",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "run_id": run.run_id,
                },
            )
        summary = sync_service.sync_snapshot()
        payload = summary.to_dict() if hasattr(summary, "to_dict") else dict(summary)
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.RECONCILING,
            payload_patch={
                "last_stage": "RECONCILING",
                "sync_before_submit": payload,
            },
        )
        return payload

    def _reconcile_after_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        service = getattr(context, "qmt_reconciliation_service", None)
        if service is None:
            raise DataUnavailableError(
                "MiniQMT simulation submit requires reconcile-after-submit service",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "run_id": run.run_id,
                },
            )
        broker_positions = context.broker_positions or []
        if not broker_positions and context.managed_order_service is not None:
            broker = getattr(context.managed_order_service, "_broker", None)
            get_positions = getattr(broker, "get_positions", None)
            if callable(get_positions):
                broker_positions = list(get_positions())
        report = service.reconcile_snapshot(
            account_id=binding.broker_account_id or "",
            trade_date=run.trade_date,
            broker_positions=broker_positions,
        )
        payload = report.to_dict() if hasattr(report, "to_dict") else dict(report)
        report_status = str(payload.get("run", {}).get("status") or "").upper()
        if report_status == "SUCCEEDED":
            next_status = SimulationDailyRunStatus.SUCCEEDED
        else:
            next_status = SimulationDailyRunStatus.FAILED_RETRYABLE
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "reconcile_after_submit": payload,
            },
        )
        return payload

    def _build_plan_from_selection(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        selection: StrategyPackageSelectionResult,
        context: SimulationRunContext,
        created_by: str,
    ) -> SimulationPlanBuildResult:
        evidence = selection.evidence_by_package[binding.package_id]
        snapshot = SignalSnapshot(
            package_id=binding.package_id,
            manifest_sha256=evidence.manifest_sha256,
            trade_date=trade_date,
            data_source=data_source,
            candidates=selection.package_results.get(binding.package_id, []),
            runtime_config=selection.runtime_config,
            valid_no_candidate=selection.valid_no_candidate,
            no_candidate_reason=selection.no_candidate_reason,
        )
        return self.orchestrator.build_execution_plan(
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=evidence,
            signal_snapshot=snapshot,
            current_positions=context.current_positions,
            current_prices=context.current_prices,
            manifest=context.manifest,
            portfolio_id=context.portfolio_id or binding.strategy_id,
            top_k=context.top_k,
            execution_policy_payload=context.execution_policy_payload,
            tail_policy_payload=context.tail_policy_payload,
            created_by=created_by,
        )

    def _handle_tail_after_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        service = context.tail_policy_service
        if service is None:
            return None
        broker_result = execution.broker_result
        handles = getattr(broker_result, "handles", None)
        if not handles:
            return None
        if context.local_broker is None:
            raise DataUnavailableError(
                "LocalSim tail handling requires the original broker instance",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        result = service.handle_local_sim_tail(
            plan=execution.execution_plan,
            broker=context.local_broker,
            handles=handles,
        )
        payload = result.to_dict()
        next_status = SimulationDailyRunStatus.SUCCEEDED if result.success else SimulationDailyRunStatus.FAILED_RETRYABLE
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "tail_handling": payload,
                "last_stage": next_status.value,
            },
        )
        return payload

    @staticmethod
    def _result_status_after_post_submit(
        execution_status: str,
        *,
        tail_result: dict[str, Any] | None,
        reconciliation: dict[str, Any] | None,
    ) -> str:
        if tail_result is not None:
            return "TAIL_HANDLED" if tail_result.get("success") else "TAIL_HANDLING_FAILED"
        if reconciliation is not None:
            run = reconciliation.get("run") if isinstance(reconciliation.get("run"), dict) else {}
            return "RECONCILED" if run.get("status") == "SUCCEEDED" else "RECONCILIATION_WARNING"
        return execution_status

    @staticmethod
    def _compute_schedule_windows(*, trade_date: date, as_of_time: datetime | None) -> tuple[dict[str, Any], ...]:
        current_time = as_of_time.time().replace(second=0, microsecond=0) if as_of_time is not None else None
        windows: list[dict[str, Any]] = []
        for window in DEFAULT_SCHEDULER_WINDOWS:
            start = datetime.combine(trade_date, time.fromisoformat(window["start"]))
            end = datetime.combine(trade_date, time.fromisoformat(window["end"]))
            state = "PENDING"
            if current_time is not None:
                if current_time < start.time():
                    state = "UPCOMING"
                elif start.time() <= current_time < end.time():
                    state = "ACTIVE"
                else:
                    state = "COMPLETED"
            windows.append(
                {
                    **window,
                    "trade_date": trade_date.isoformat(),
                    "state": state,
                    "start_at": start.isoformat(),
                    "end_at": end.isoformat(),
                }
            )
        return tuple(windows)


simulation_lifecycle_scheduler = SimulationLifecycleScheduler()


class SimulationLifecycleBackgroundScheduler:
    """Opt-in unattended scheduler wrapper with trading-window orchestration."""

    def __init__(
        self,
        *,
        lifecycle_scheduler: SimulationLifecycleScheduler | None = None,
    ) -> None:
        self.lifecycle_scheduler = lifecycle_scheduler or SimulationLifecycleScheduler()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._interval_seconds = self._default_interval()
        self._default_submit = self._env_flag("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", default=False)
        self._data_source = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE") or "DB_HISTORICAL").strip() or "DB_HISTORICAL"
        self._limit = self._default_limit()
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None

    def start(self, *, interval_seconds: int | None = None, default_submit: bool | None = None) -> dict[str, Any]:
        interval = int(interval_seconds or self._interval_seconds)
        if interval <= 0:
            raise ValueError("simulation runtime scheduler interval_seconds must be positive")
        with self._lock:
            self._interval_seconds = interval
            if default_submit is not None:
                self._default_submit = bool(default_submit)
            if self._thread and self._thread.is_alive():
                return self.status()
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="simulation-runtime-lifecycle-scheduler",
                daemon=True,
            )
            self._thread.start()
            logger.info(
                "Simulation runtime scheduler started interval=%ss default_submit=%s",
                self._interval_seconds,
                self._default_submit,
            )
            return self.status()

    def shutdown(self, wait: bool = False) -> dict[str, Any]:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if wait and thread and thread.is_alive():
            thread.join(timeout=5.0)
        logger.info("Simulation runtime scheduler stopped")
        return self.status()

    def status(self) -> dict[str, Any]:
        thread = self._thread
        base = self.lifecycle_scheduler.status()
        running = bool(thread and thread.is_alive() and not self._stop_event.is_set())
        return {
            **base,
            "autostart": running,
            "running": running,
            "thread_alive": bool(thread and thread.is_alive()),
            "interval_seconds": self._interval_seconds,
            "default_submit": self._default_submit,
            "data_source": self._data_source,
            "limit": self._limit,
            "last_run_at": self._last_run_at.isoformat() if self._last_run_at else None,
            "last_result": self._last_result,
        }

    def run_once(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        now = as_of_time or datetime.now()
        trade_date = self._trade_date(now)
        decision = self._window_decision(as_of_time=now, trade_date=trade_date)
        result: dict[str, Any] = {
            "started_at": now.isoformat(),
            "trade_date": trade_date.isoformat(),
            "data_source": self._data_source,
            "window": decision["window"],
            "should_run": decision["should_run"],
            "submit": decision["submit"],
            "processed": [],
            "errors": [],
        }
        if decision["should_run"]:
            try:
                tick = self.lifecycle_scheduler.run_once(
                    trade_date=trade_date,
                    data_source=self._data_source,
                    limit=self._limit,
                    submit=bool(decision["submit"]),
                    as_of_time=now,
                )
                result["processed"] = [
                    {
                        "binding_id": item.binding_id,
                        "strategy_id": item.strategy_id,
                        "broker_backend": item.broker_backend.value,
                        "status": item.status,
                        "run_id": item.run.run_id if item.run else None,
                        "execution_plan_id": item.execution_plan.plan_id if item.execution_plan else None,
                        "error": item.error,
                    }
                    for item in tick.results
                ]
                result["summary"] = {
                    "total_bindings": tick.total_bindings,
                    "planned_count": tick.planned_count,
                    "reused_count": tick.reused_count,
                    "submitted_count": tick.submitted_count,
                    "failed_count": tick.failed_count,
                }
            except Exception as exc:  # scheduler must expose failure, not crash silently
                payload = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": getattr(exc, "context", None),
                }
                result["errors"].append(payload)
                logger.warning("Simulation runtime scheduler tick failed: %s", payload)
        result["completed_at"] = datetime.now().isoformat()
        self._last_run_at = now
        self._last_result = result
        return result

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("Simulation runtime scheduler run_once crashed")
            if self._stop_event.wait(timeout=self._interval_seconds):
                break

    def _window_decision(self, *, as_of_time: datetime, trade_date: date) -> dict[str, Any]:
        windows = SimulationLifecycleScheduler._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)
        active = next((item for item in windows if item["state"] == "ACTIVE"), None)
        if active is None:
            return {"window": None, "should_run": False, "submit": False, "reason": "outside_configured_windows"}
        action = str(active.get("action") or "")
        should_run = action in {"selection_evidence", "execution_plan", "submit"}
        submit = bool(self._default_submit and action == "submit")
        return {"window": active, "should_run": should_run, "submit": submit, "reason": action}

    @staticmethod
    def _trade_date(now: datetime) -> date:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_TRADE_DATE") or "").strip()
        if raw:
            return date.fromisoformat(raw)
        return now.date()

    @staticmethod
    def _default_interval() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC") or "30").strip()
        try:
            value = int(raw)
        except ValueError:
            return 30
        return value if value > 0 else 30

    @staticmethod
    def _default_limit() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_LIMIT") or "100").strip()
        try:
            value = int(raw)
        except ValueError:
            return 100
        return min(max(value, 1), 500)

    @staticmethod
    def _env_flag(name: str, *, default: bool) -> bool:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "y", "on"}


simulation_lifecycle_background_scheduler = SimulationLifecycleBackgroundScheduler(
    lifecycle_scheduler=simulation_lifecycle_scheduler
)
