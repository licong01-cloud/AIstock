"""Operator projections and controlled scheduler operations for the unified simulation runtime."""

from __future__ import annotations

import os
from collections import Counter
from datetime import date, datetime
from typing import Any

from backend.services.trading_core.errors import DataUnavailableError

from .models import ExecutionPlan, SimulationBrokerBackend, SimulationDailyRun, SimulationDailyRunStatus
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository
from .scheduler import (
    SimulationLifecycleBackgroundScheduler,
    SimulationLifecycleScheduler,
    simulation_lifecycle_background_scheduler,
)


TERMINAL_RUN_STATUSES = frozenset(
    {
        SimulationDailyRunStatus.SUCCEEDED,
        SimulationDailyRunStatus.FAILED_RETRYABLE,
        SimulationDailyRunStatus.FAILED_TERMINAL,
        SimulationDailyRunStatus.CANCELLED,
    }
)


class SimulationRuntimeOpsService:
    """Expose business-readable runtime state without triggering trading actions."""

    def __init__(
        self,
        *,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        scheduler: SimulationLifecycleScheduler | SimulationLifecycleBackgroundScheduler | None = None,
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.scheduler = scheduler or (
            simulation_lifecycle_background_scheduler
            if repository is None
            else SimulationLifecycleScheduler(repository=self.repository)
        )

    def scheduler_status(self) -> dict[str, Any]:
        status = dict(self.scheduler.status())
        return {
            "ok": True,
            "scheduler": status.get("scheduler") or "simulation_lifecycle_scheduler",
            "autostart": bool(status.get("autostart", False)),
            "default_submit": bool(status.get("default_submit", False)),
            "approval_states": list(status.get("approval_states") or []),
            "schedule_windows": list(status.get("schedule_windows") or []),
            "restart_recovery_mode": status.get("restart_recovery_mode") or "persisted_state_only",
            "window_orchestration": status.get("window_orchestration") or {},
            "read_only_status_api": True,
            "read_only_ops_api": False,
            "controlled_ops_api": True,
            "scheduler_control_api_enabled": bool(status.get("scheduler_control_api_enabled", False)),
            "manual_tick_endpoint_enabled": bool(status.get("manual_tick_endpoint_enabled", False)),
            "context_provider": status.get("context_provider") or {},
            "context_provider_mode": status.get("context_provider_mode"),
            "data_source": status.get("data_source"),
            "data_source_policy": status.get("data_source_policy") or {},
            "summary": {
                "label": "simulation lifecycle scheduler",
                "next_action": "monitor scheduler windows, or use the controlled start/stop/tick APIs",
                "safety_note": (
                    "Status is read-only. start/stop/tick are controlled operations; "
                    "default_submit remains false unless explicitly enabled."
                ),
            },
        }

    def list_runs(
        self,
        *,
        trade_date: date | None = None,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        status: SimulationDailyRunStatus | str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        runs = self.repository.list_simulation_daily_runs(
            trade_date=trade_date,
            broker_backend=broker_backend,
            strategy_id=strategy_id,
            status=status,
            limit=limit,
        )
        return {
            "summary": self._run_list_summary(runs),
            "runs": [self._run_summary(run) for run in runs],
        }

    def get_run_detail(self, run_id: str) -> dict[str, Any]:
        run = self.repository.get_simulation_daily_run(run_id)
        payload: dict[str, Any] = {"run": self._run_summary(run)}
        if run.execution_plan_id:
            payload["execution_plan"] = self._plan_summary(
                self.repository.get_execution_plan(run.execution_plan_id)
            )
        else:
            payload["execution_plan"] = None
        if run.selection_evidence_id:
            evidence = self.repository.get_daily_selection_evidence(run.selection_evidence_id)
            payload["selection_evidence"] = {
                "evidence_id": evidence.evidence_id,
                "artifact_hash": evidence.artifact_hash,
                "target_trade_date": evidence.target_trade_date.isoformat(),
                "package_id": evidence.package_id,
                "manifest_sha256": evidence.manifest_sha256,
                "release_id": evidence.release_id,
                "release_hash": evidence.release_hash,
                "runtime_profile_version_id": evidence.runtime_profile_version_id,
                "runtime_profile_hash": evidence.runtime_profile_hash,
                "candidate_count": evidence.candidate_count,
                "excluded_count": evidence.excluded_count,
                "source_type": evidence.source_type,
                "data_source": evidence.data_source,
            }
        else:
            payload["selection_evidence"] = None
        return payload

    def get_execution_plan_detail(self, plan_id: str) -> dict[str, Any]:
        plan = self.repository.get_execution_plan(plan_id)
        return {"execution_plan": self._plan_summary(plan, include_intents=True)}

    def build_live_admission_evidence(
        self,
        *,
        paper_v2_run_id: str,
        miniqmt_sim_run_id: str,
        target_broker_backend: str,
    ) -> dict[str, Any]:
        """Build the live-approval evidence payload from persisted simulation runs."""

        paper_run = self.repository.get_simulation_daily_run(paper_v2_run_id)
        qmt_run = self.repository.get_simulation_daily_run(miniqmt_sim_run_id)
        self._require_successful_run_for_live_evidence(paper_run, expected_backend=SimulationBrokerBackend.LOCAL_SIM)
        self._require_successful_run_for_live_evidence(qmt_run, expected_backend=SimulationBrokerBackend.MINIQMT_SIM)
        if paper_run.package_id != qmt_run.package_id or paper_run.manifest_sha256 != qmt_run.manifest_sha256:
            raise DataUnavailableError(
                "live admission simulation evidence must reference the same StrategyPackage alpha core",
                context={
                    "paper_v2_run_id": paper_v2_run_id,
                    "miniqmt_sim_run_id": miniqmt_sim_run_id,
                    "paper_package_id": paper_run.package_id,
                    "miniqmt_package_id": qmt_run.package_id,
                    "paper_manifest_sha256": paper_run.manifest_sha256,
                    "miniqmt_manifest_sha256": qmt_run.manifest_sha256,
                },
            )
        if paper_run.release_hash != qmt_run.release_hash:
            raise DataUnavailableError(
                "live admission simulation evidence must reference the same StrategyRuntimeRelease hash",
                context={
                    "paper_v2_run_id": paper_v2_run_id,
                    "miniqmt_sim_run_id": miniqmt_sim_run_id,
                    "paper_release_hash": paper_run.release_hash,
                    "miniqmt_release_hash": qmt_run.release_hash,
                },
            )
        return {
            "sim_validation_evidence": {
                "paper_v2": self._live_evidence_for_run(paper_run, validation_backend="paper_v2"),
                "miniqmt_sim": self._live_evidence_for_run(qmt_run, validation_backend="miniqmt_sim"),
            },
            "broker_compatibility": {
                "status": "VERIFIED",
                "target_broker_backend": target_broker_backend,
                "broker_backend": target_broker_backend,
                "simulation_binding_id": qmt_run.binding_id,
                "simulation_binding_hash": qmt_run.binding_hash,
                "simulation_release_id": qmt_run.release_id,
                "simulation_release_hash": qmt_run.release_hash,
                "miniqmt_sim_run_id": qmt_run.run_id,
            },
        }

    def _run_list_summary(self, runs: list[SimulationDailyRun]) -> dict[str, Any]:
        by_status = Counter(run.status.value for run in runs)
        by_backend = Counter(run.broker_backend.value for run in runs)
        active = sum(1 for run in runs if run.status not in TERMINAL_RUN_STATUSES)
        return {
            "run_count": len(runs),
            "active_run_count": active,
            "terminal_run_count": len(runs) - active,
            "by_status": dict(sorted(by_status.items())),
            "by_broker_backend": dict(sorted(by_backend.items())),
        }

    def _run_summary(self, run: SimulationDailyRun) -> dict[str, Any]:
        stage_counts = self._stage_counts(run.run_payload_json)
        broker_context = self._broker_context(run)
        reconciliation_context = self._reconciliation_context(run)
        return {
            "run_id": run.run_id,
            "trade_date": run.trade_date.isoformat(),
            "strategy_id": run.strategy_id,
            "broker_backend": run.broker_backend.value,
            "package_id": run.package_id,
            "manifest_sha256": run.manifest_sha256,
            "release_id": run.release_id,
            "release_hash": run.release_hash,
            "binding_id": run.binding_id,
            "binding_hash": run.binding_hash,
            "selection_evidence_id": run.selection_evidence_id,
            "selection_artifact_hash": run.selection_artifact_hash,
            "execution_plan_id": run.execution_plan_id,
            "execution_plan_hash": run.execution_plan_hash,
            "status": run.status.value,
            "last_stage": str(run.run_payload_json.get("last_stage") or run.status.value),
            "stage_counts": stage_counts,
            "broker_context": broker_context,
            "strategy_performance": self._strategy_performance(run),
            "reconciliation_context": reconciliation_context,
            "orders": self._orders_projection(run, broker_context),
            "fills": self._fills_projection(run, broker_context),
            "errors": self._errors_projection(run, broker_context, reconciliation_context),
            "audit": {
                "created_at": run.created_at.isoformat(),
                "updated_at": run.updated_at.isoformat(),
                "created_by": run.run_payload_json.get("created_by"),
            },
        }

    @staticmethod
    def _require_successful_run_for_live_evidence(
        run: SimulationDailyRun,
        *,
        expected_backend: SimulationBrokerBackend,
    ) -> None:
        if run.broker_backend != expected_backend:
            raise DataUnavailableError(
                "live admission simulation evidence run has unexpected backend",
                context={
                    "run_id": run.run_id,
                    "broker_backend": run.broker_backend.value,
                    "expected_backend": expected_backend.value,
                },
            )
        if run.status != SimulationDailyRunStatus.SUCCEEDED:
            raise DataUnavailableError(
                "live admission simulation evidence requires successful simulation runs",
                context={
                    "run_id": run.run_id,
                    "status": run.status.value,
                    "expected_status": SimulationDailyRunStatus.SUCCEEDED.value,
                },
            )
        if not run.execution_plan_id or not run.execution_plan_hash or not run.selection_evidence_id:
            raise DataUnavailableError(
                "live admission simulation evidence requires selection evidence and execution plan linkage",
                context={
                    "run_id": run.run_id,
                    "selection_evidence_id": run.selection_evidence_id,
                    "execution_plan_id": run.execution_plan_id,
                    "execution_plan_hash": run.execution_plan_hash,
                },
            )

    @staticmethod
    def _live_evidence_for_run(run: SimulationDailyRun, *, validation_backend: str) -> dict[str, Any]:
        return {
            "status": "VERIFIED",
            "validation_status": "VERIFIED",
            "validation_backend": validation_backend,
            "run_id": run.run_id,
            "trade_date": run.trade_date.isoformat(),
            "strategy_id": run.strategy_id,
            "broker_backend": run.broker_backend.value,
            "package_id": run.package_id,
            "manifest_sha256": run.manifest_sha256,
            "runtime_release_id": run.release_id,
            "runtime_release_sha256": run.release_hash,
            "binding_id": run.binding_id,
            "binding_hash": run.binding_hash,
            "selection_evidence_id": run.selection_evidence_id,
            "selection_artifact_hash": run.selection_artifact_hash,
            "execution_plan_id": run.execution_plan_id,
            "execution_plan_hash": run.execution_plan_hash,
            "strategy_performance": run.run_payload_json.get("strategy_performance"),
            "reconcile_after_submit": run.run_payload_json.get("reconcile_after_submit"),
        }

    def _plan_summary(self, plan: ExecutionPlan, *, include_intents: bool = False) -> dict[str, Any]:
        buy_count = sum(1 for intent in plan.intents if intent.side.value == "BUY")
        sell_count = sum(1 for intent in plan.intents if intent.side.value == "SELL")
        symbols = sorted({intent.symbol for intent in plan.intents})
        payload = {
            "plan_id": plan.plan_id,
            "plan_hash": plan.plan_hash,
            "strategy_id": plan.strategy_id,
            "portfolio_id": plan.portfolio_id,
            "package_id": plan.package_id,
            "release_id": plan.release_id,
            "release_hash": plan.release_hash,
            "binding_id": plan.binding_id,
            "binding_hash": plan.binding_hash,
            "selection_evidence_id": plan.selection_evidence_id,
            "selection_evidence_hash": plan.selection_evidence_hash,
            "target_trade_date": plan.target_trade_date.isoformat(),
            "execution_policy_version_id": plan.execution_policy_version_id,
            "execution_policy_sha256": plan.execution_policy_sha256,
            "tail_policy_version_id": plan.tail_policy_version_id,
            "tail_policy_sha256": plan.tail_policy_sha256,
            "intent_count": len(plan.intents),
            "buy_intent_count": buy_count,
            "sell_intent_count": sell_count,
            "trading_rule_decision_count": len(plan.trading_rule_decisions),
            "symbols": symbols,
            "created_at": plan.created_at.isoformat(),
        }
        if include_intents:
            payload["intents"] = [
                {
                    "intent_id": intent.intent_id,
                    "symbol": intent.symbol,
                    "side": intent.side.value,
                    "target_quantity": intent.target_quantity,
                    "delta_quantity": intent.delta_quantity,
                    "order_quantity": intent.order_quantity,
                    "current_quantity": intent.current_quantity,
                    "current_available_quantity": intent.current_available_quantity,
                    "rebalance_reason": intent.rebalance_reason,
                    "trading_rule_decision_id": intent.trading_rule_decision_id,
                    "schedule_window": intent.schedule_window,
                    "price_policy": intent.price_policy,
                }
                for intent in plan.intents
            ]
        return payload

    @staticmethod
    def _stage_counts(payload: dict[str, Any]) -> dict[str, int]:
        keys = (
            "target_count",
            "order_intent_count",
            "trading_rule_decision_count",
            "execution_plan_intent_count",
            "submitted_intents",
            "failed_intents",
        )
        counts: dict[str, int] = {}
        for key in keys:
            raw = payload.get(key)
            if raw is None:
                continue
            try:
                counts[key] = int(raw)
            except (TypeError, ValueError) as exc:
                raise DataUnavailableError(
                    "simulation run payload has a non-integer stage count",
                    context={"key": key, "value": raw},
                ) from exc
        return counts

    @staticmethod
    def _broker_context(run: SimulationDailyRun) -> dict[str, Any]:
        payload = run.run_payload_json
        context = {
            "no_rebalance_required": bool(payload.get("no_rebalance_required", False)),
            "broker_called": payload.get("broker_called"),
            "broker_order_handles": payload.get("broker_order_handles"),
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status"),
            "qmt_retry_of_batch_id": payload.get("qmt_retry_of_batch_id"),
            "qmt_batch_result": payload.get("qmt_batch_result"),
            "sync_before_submit": payload.get("sync_before_submit"),
            "reconcile_after_submit": payload.get("reconcile_after_submit"),
            "tail_handling": payload.get("tail_handling"),
        }
        return {key: value for key, value in context.items() if value is not None}

    @staticmethod
    def _strategy_performance(run: SimulationDailyRun) -> dict[str, Any] | None:
        payload = run.run_payload_json
        raw = payload.get("strategy_performance") or payload.get("performance_projection")
        return raw if isinstance(raw, dict) else None

    @staticmethod
    def _reconciliation_context(run: SimulationDailyRun) -> dict[str, Any] | None:
        raw = run.run_payload_json.get("reconcile_after_submit")
        if not isinstance(raw, dict):
            return None
        issues = raw.get("issues") if isinstance(raw.get("issues"), list) else []
        run_payload = raw.get("run") if isinstance(raw.get("run"), dict) else {}
        return {
            "status": run_payload.get("status"),
            "issue_count": len(issues),
            "issues": issues,
            "strategy_lot_quantities": raw.get("strategy_lot_quantities") or {},
            "broker_quantities": raw.get("broker_quantities") or {},
            "overlap_symbols": raw.get("overlap_symbols") or [],
            "unattributed_orders": raw.get("unattributed_orders"),
            "unattributed_trades": raw.get("unattributed_trades"),
        }

    @staticmethod
    def _orders_projection(run: SimulationDailyRun, broker_context: dict[str, Any]) -> list[dict[str, Any]]:
        local_handles = broker_context.get("broker_order_handles")
        if isinstance(local_handles, list):
            return [
                {
                    "source": "local_sim_handle",
                    "handle_id": item.get("handle_id"),
                    "intent_id": item.get("intent_id"),
                    "backend_id": item.get("backend_id"),
                    "state": "submitted",
                    "submitted_at": item.get("submitted_at"),
                }
                for item in local_handles
                if isinstance(item, dict)
            ]
        qmt_batch = broker_context.get("qmt_batch_result")
        results = qmt_batch.get("results") if isinstance(qmt_batch, dict) else None
        if not isinstance(results, list):
            return []
        rows: list[dict[str, Any]] = []
        for item in results:
            if not isinstance(item, dict):
                continue
            preflight = item.get("preflight") if isinstance(item.get("preflight"), dict) else {}
            primary_error = preflight.get("primary_error") if isinstance(preflight.get("primary_error"), dict) else None
            rows.append(
                {
                    "source": "miniqmt_managed_order",
                    "intent_id": item.get("intent_id"),
                    "qmt_order_id": item.get("qmt_order_id"),
                    "success": bool(item.get("success")),
                    "broker_called": bool(item.get("broker_called")),
                    "broker_message": item.get("broker_message"),
                    "preflight_allowed": preflight.get("allowed"),
                    "primary_error_code": preflight.get("primary_error_code"),
                    "primary_error_message": primary_error.get("message") if primary_error else None,
                }
            )
        return rows

    @staticmethod
    def _fills_projection(run: SimulationDailyRun, broker_context: dict[str, Any]) -> list[dict[str, Any]]:
        sync = broker_context.get("sync_before_submit")
        if not isinstance(sync, dict):
            return []
        return [
            {
                "source": "miniqmt_sync_summary",
                "trades_seen": sync.get("trades_seen"),
                "trades_inserted": sync.get("trades_inserted"),
                "trades_existing": sync.get("trades_existing"),
                "cash_entries_appended": sync.get("cash_entries_appended"),
                "buy_fill_settled_amount": sync.get("buy_fill_settled_amount"),
                "sell_fill_received_amount": sync.get("sell_fill_received_amount"),
                "sell_fill_realized_pnl": sync.get("sell_fill_realized_pnl"),
            }
        ]

    @staticmethod
    def _errors_projection(
        run: SimulationDailyRun,
        broker_context: dict[str, Any],
        reconciliation_context: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        errors: list[dict[str, Any]] = []
        qmt_batch = broker_context.get("qmt_batch_result")
        results = qmt_batch.get("results") if isinstance(qmt_batch, dict) else None
        if isinstance(results, list):
            for item in results:
                if not isinstance(item, dict) or item.get("success"):
                    continue
                preflight = item.get("preflight") if isinstance(item.get("preflight"), dict) else {}
                primary = preflight.get("primary_error") if isinstance(preflight.get("primary_error"), dict) else {}
                errors.append(
                    {
                        "source": "miniqmt_order_preflight",
                        "intent_id": item.get("intent_id"),
                        "code": preflight.get("primary_error_code"),
                        "message": primary.get("message") or item.get("broker_message"),
                        "context": primary.get("context") or {},
                    }
                )
        if reconciliation_context:
            for issue in reconciliation_context.get("issues") or []:
                if isinstance(issue, dict):
                    errors.append(
                        {
                            "source": "miniqmt_reconciliation",
                            "code": issue.get("issue_type"),
                            "message": issue.get("message"),
                            "severity": issue.get("severity"),
                            "symbol": issue.get("symbol"),
                            "context": issue.get("context") or {},
                        }
                    )
        if run.status in {SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.FAILED_TERMINAL} and not errors:
            errors.append(
                {
                    "source": "simulation_daily_run",
                    "code": run.status.value,
                    "message": str(run.run_payload_json.get("last_stage") or run.status.value),
                    "context": {},
                }
            )
        return errors

    def start_scheduler(self, *, interval_seconds: int | None = None, default_submit: bool | None = None) -> dict[str, Any]:
        if not isinstance(self.scheduler, SimulationLifecycleBackgroundScheduler):
            raise DataUnavailableError(
                "scheduler start requires SimulationLifecycleBackgroundScheduler",
                context={"scheduler_type": type(self.scheduler).__name__},
            )
        result = self.scheduler.start(interval_seconds=interval_seconds, default_submit=default_submit)
        return {"ok": True, "action": "scheduler_started", **result}

    def stop_scheduler(self) -> dict[str, Any]:
        if not isinstance(self.scheduler, SimulationLifecycleBackgroundScheduler):
            raise DataUnavailableError(
                "scheduler stop requires SimulationLifecycleBackgroundScheduler",
                context={"scheduler_type": type(self.scheduler).__name__},
            )
        result = self.scheduler.shutdown(wait=True)
        return {"ok": True, "action": "scheduler_stopped", **result}

    def scheduler_tick(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        if isinstance(self.scheduler, SimulationLifecycleBackgroundScheduler):
            result = self.scheduler.run_once(as_of_time=as_of_time)
            return {"ok": True, "action": "scheduler_tick", **result}
        tick = self.scheduler.run_once(
            trade_date=(as_of_time or datetime.now()).date(),
            data_source=(os.getenv("SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE") or "DB_HISTORICAL").strip() or "DB_HISTORICAL",
            submit=False,
            as_of_time=as_of_time,
        )
        return {
            "ok": True,
            "action": "scheduler_tick",
            "trade_date": tick.trade_date.isoformat(),
            "data_source": tick.data_source,
            "submit": tick.submit,
            "total_bindings": tick.total_bindings,
            "planned_count": tick.planned_count,
            "reused_count": tick.reused_count,
            "submitted_count": tick.submitted_count,
            "failed_count": tick.failed_count,
            "results": [
                {
                    "binding_id": item.binding_id,
                    "strategy_id": item.strategy_id,
                    "broker_backend": item.broker_backend.value,
                    "status": item.status,
                    "run_id": item.run.run_id if item.run else None,
                    "data_source": item.data_source or tick.data_source,
                    "error": item.error,
                }
                for item in tick.results
            ],
        }
