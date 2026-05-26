"""Unified simulation lifecycle orchestration for LocalSim and MiniQMT.

This service owns the broker-neutral chain from daily selection evidence to a
persisted execution plan. Broker execution bridges are invoked only after the
shared plan exists, so LocalSim and MiniQMT cannot diverge in signal/target
generation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

from backend.services.paper_trading_v2.broker.base import BrokerBackend
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.selection_center.models import SignalSnapshot, TargetPosition
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import (
    BrokerUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import PositionLot

from .bridges import LocalSimExecutionBridge, MiniQMTExecutionBridge
from .decision import ExecutionPlanCompiler, RebalanceIntentResult, RebalanceIntentService, TargetPositionService
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
        manifest: StrategyPackageManifest | None = None,
        portfolio_id: str | None = None,
        top_k: int | None = None,
        execution_policy_payload: dict[str, Any] | None = None,
        tail_policy_payload: dict[str, Any] | None = None,
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
            total_equity=binding.capital_allocation,
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
        )
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.PLANNING_EXECUTION,
            payload_patch={
                "target_count": len(targets),
                "order_intent_count": len(rebalance.order_intents),
                "trading_rule_decision_count": len(rebalance.trading_rule_decisions),
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
    ) -> SimulationExecutionResult:
        return self.submit_persisted_execution_plan(
            run=build_result.run,
            binding=build_result.binding,
            execution_plan=build_result.execution_plan,
            local_broker=local_broker,
            managed_order_service=managed_order_service,
            mode=mode,
            price_by_symbol=price_by_symbol,
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
    ) -> SimulationExecutionResult:
        """Submit an already-persisted plan exactly once after restart recovery."""
        run = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.SUBMITTING,
            payload_patch={"last_stage": "SUBMITTING"},
        )
        plan = execution_plan
        if not plan.intents:
            succeeded = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.SUCCEEDED,
                payload_patch={"no_rebalance_required": True, "broker_called": False, "last_stage": "SUCCEEDED"},
            )
            return SimulationExecutionResult(
                run=succeeded,
                execution_plan=plan,
                broker_backend=binding.broker_backend,
                status="NO_REBALANCE",
                intent_count=0,
                broker_result=None,
            )

        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            if local_broker is None:
                raise BrokerUnavailableError(
                    "LocalSim execution requires an injected LocalSim broker",
                    context={"run_id": run.run_id, "plan_id": plan.plan_id},
                )
            local_result = self.local_bridge.submit_plan(plan=plan, broker=local_broker)
            succeeded = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.INTRADAY_RUNNING,
                payload_patch={
                    "broker_called": True,
                    "submitted_intents": len(local_result.order_intents),
                    "broker_order_handles": [
                        handle.model_dump(mode="json") for handle in local_result.handles
                    ],
                    "last_stage": "INTRADAY_RUNNING",
                },
            )
            return SimulationExecutionResult(
                run=succeeded,
                execution_plan=plan,
                broker_backend=binding.broker_backend,
                status="SUBMITTED",
                intent_count=len(plan.intents),
                broker_result=local_result,
            )

        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            if managed_order_service is None:
                raise BrokerUnavailableError(
                    "MiniQMT execution requires QmtManagedOrderService",
                    context={"run_id": run.run_id, "plan_id": plan.plan_id},
                )
            bridge = MiniQMTExecutionBridge(managed_order_service=managed_order_service)
            qmt_result = bridge.submit_plan(
                plan=plan,
                binding=binding,
                mode=mode,
                price_by_symbol=price_by_symbol,
            )
            next_status = SimulationDailyRunStatus.INTRADAY_RUNNING if qmt_result.success else SimulationDailyRunStatus.FAILED_RETRYABLE
            broker_called = any(result.broker_called for result in qmt_result.results)
            updated = self.repository.update_simulation_daily_run(
                run.run_id,
                status=next_status,
                payload_patch={
                    "broker_called": broker_called,
                    "submitted_intents": qmt_result.succeeded,
                    "failed_intents": qmt_result.failed,
                    "qmt_batch_id": qmt_result.batch_id,
                    "qmt_batch_status": qmt_result.batch_status,
                    "qmt_retry_of_batch_id": qmt_result.retry_of_batch_id,
                    "qmt_batch_result": qmt_result.to_dict(),
                    "last_stage": next_status.value,
                },
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
