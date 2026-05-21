"""Scheduler entry point for unified LocalSim and MiniQMT simulation runs.

The scheduler is intentionally broker-neutral until ``submit=True`` is passed.
It drives StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan for
eligible SimulationReleaseBinding rows and reuses persisted plans on restart so
that a backend tick cannot duplicate orders.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Protocol

from backend.services.paper_trading_v2.broker.base import BrokerBackend
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
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


DEFAULT_SCHEDULER_APPROVAL_STATES = (
    SimulationBindingApprovalState.SIM_VALIDATING,
    SimulationBindingApprovalState.SIM_PASSED,
    SimulationBindingApprovalState.LIVE_APPROVAL_PENDING,
    SimulationBindingApprovalState.LIVE_APPROVED,
)


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
    price_by_symbol: dict[str, Any] | None = None


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


@dataclass(frozen=True)
class SimulationSchedulerBindingResult:
    binding_id: str
    strategy_id: str
    broker_backend: SimulationBrokerBackend
    status: str
    run: SimulationDailyRun | None = None
    execution_plan: ExecutionPlan | None = None
    execution_result: SimulationExecutionResult | None = None
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

    @property
    def planned_count(self) -> int:
        return sum(1 for item in self.results if item.status == "PLANNED")

    @property
    def reused_count(self) -> int:
        return sum(1 for item in self.results if item.status == "REUSED_EXISTING_PLAN")

    @property
    def submitted_count(self) -> int:
        return sum(1 for item in self.results if item.status in {"SUBMITTED", "NO_REBALANCE"})

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
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.selection_service = selection_service or StrategyPackageSelectionService(repository=self.repository)
        self.orchestrator = orchestrator or SimulationLifecycleOrchestrator(repository=self.repository)
        self.context_provider = context_provider or FailFastSimulationRunContextProvider()

    def status(self) -> dict[str, Any]:
        return {
            "ok": True,
            "scheduler": "simulation_lifecycle_scheduler",
            "autostart": False,
            "default_submit": False,
            "approval_states": [state.value for state in DEFAULT_SCHEDULER_APPROVAL_STATES],
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
            return self._existing_plan_result(binding=binding, run=existing)

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
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="PLANNED",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
            )

        execution = self.orchestrator.submit_execution_plan(
            build_result=build_result,
            local_broker=context.local_broker,
            managed_order_service=context.managed_order_service,
            mode=mode,
            price_by_symbol=context.price_by_symbol,
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=execution.status,
            run=execution.run,
            execution_plan=execution.execution_plan,
            execution_result=execution,
        )

    def _existing_plan_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
    ) -> SimulationSchedulerBindingResult:
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        status = "REUSED_EXISTING_PLAN"
        if run.status == SimulationDailyRunStatus.SUCCEEDED and not plan.intents:
            status = "NO_REBALANCE"
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=run,
            execution_plan=plan,
        )

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


simulation_lifecycle_scheduler = SimulationLifecycleScheduler()
