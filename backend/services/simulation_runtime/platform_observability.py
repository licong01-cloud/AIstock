"""Read-only, bounded observability for the unified simulation platform.

The projection in this module consumes already-durable scheduler/run facts.  It
must never start a feed, mutate a run, acknowledge an alert, or call a broker.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

from backend.services.qmt_strategy_ledger.models import OrderBatchStatus
from backend.services.trading_core.errors import RuntimeConfigInvalidError

from .models import (
    LocalSimExecutionStateV1,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
)


PLATFORM_DIAGNOSTICS_SCHEMA_VERSION = "simulation_platform_diagnostics_v1"
PLATFORM_METRICS_SCHEMA_VERSION = "simulation_platform_metrics_v1"
PLATFORM_ALERTS_SCHEMA_VERSION = "simulation_platform_alerts_v1"
PLATFORM_LAYER_SCHEMA_VERSION = "simulation_platform_health_layer_v1"
PLATFORM_RUNBOOK_SCHEMA_VERSION = "simulation_platform_operator_runbook_ref_v1"

METRIC_LABEL_ALLOWLIST = frozenset(
    {
        "backend",
        "control_revision",
        "status",
        "reason_code",
        "market_phase",
        "source",
    }
)
METRIC_HIGH_CARDINALITY_LABELS = frozenset(
    {
        "binding_id",
        "run_id",
        "runtime_id",
        "plan_id",
        "order_id",
        "symbol",
        "package_id",
        "strategy_id",
    }
)
METRIC_SERIES_LIMIT = 256
ALERT_LIMIT = 100
SCHEDULER_TICK_LAG_MULTIPLIER = 2
LOCAL_SIM_CAUSAL_BAR_LAG_ALERT_SECONDS = 120
LOCAL_SIM_OUTBOX_BACKLOG_ALERT_SECONDS = 120

BLOCKING_RUN_STATUSES = frozenset(
    {
        SimulationDailyRunStatus.FAILED_RETRYABLE,
        SimulationDailyRunStatus.FAILED_TERMINAL,
    }
)
ACTIVE_RUN_STATUSES = frozenset(
    {
        SimulationDailyRunStatus.CREATED,
        SimulationDailyRunStatus.PRECHECKING,
        SimulationDailyRunStatus.SIGNAL_GENERATING,
        SimulationDailyRunStatus.TARGET_GENERATING,
        SimulationDailyRunStatus.PLANNING_EXECUTION,
        SimulationDailyRunStatus.SUBMITTING,
        SimulationDailyRunStatus.INTRADAY_RUNNING,
        SimulationDailyRunStatus.TAIL_HANDLING,
        SimulationDailyRunStatus.RECONCILING,
    }
)
ACTIVE_MARKET_PHASES = frozenset(
    {
        "PRE_OPEN",
        "OPEN_AUCTION",
        "OPEN_AM",
        "LUNCH_BREAK",
        "OPEN_PM",
        "CLOSE_AUCTION",
        "POST_CLOSE_RECONCILIATION",
    }
)
LOCAL_SIM_PERSISTENCE_STATUSES = frozenset(
    {
        "PROJECTION_PENDING",
        "PERSISTED",
        "PERSISTED_WITH_CAPACITY_RESIDUAL",
    }
)
LOCAL_SIM_OUTBOX_STATUSES = frozenset({"PENDING", "PROJECTION_RETRYABLE", "PROJECTED"})
MINIQMT_BATCH_STATUSES = frozenset(item.value for item in OrderBatchStatus)


def _invalid(message: str, *, field: str, value: Any = None, **context: Any) -> RuntimeConfigInvalidError:
    return RuntimeConfigInvalidError(
        message,
        context={
            "reason_code": "SIMULATION_PLATFORM_OBSERVABILITY_PAYLOAD_INVALID",
            "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
            "field": field,
            "value_type": type(value).__name__ if value is not None else None,
            **context,
        },
    )


def _optional_mapping(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> dict[str, Any] | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if not isinstance(value, Mapping):
        raise _invalid(
            f"{field_prefix}.{key} must be a mapping",
            field=f"{field_prefix}.{key}",
            value=value,
        )
    return dict(value)


def _required_mapping(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> dict[str, Any]:
    value = _optional_mapping(payload, key, field_prefix=field_prefix)
    if value is None:
        raise _invalid(
            f"{field_prefix}.{key} is required",
            field=f"{field_prefix}.{key}",
        )
    return value


def _optional_nonnegative_int(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> int | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise _invalid(
            f"{field_prefix}.{key} must be a non-negative integer",
            field=f"{field_prefix}.{key}",
            value=value,
        )
    return value


def _required_nonnegative_int(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> int:
    value = _optional_nonnegative_int(payload, key, field_prefix=field_prefix)
    if value is None:
        raise _invalid(
            f"{field_prefix}.{key} is required",
            field=f"{field_prefix}.{key}",
        )
    return value


def _optional_exact_bool(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> bool | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if not isinstance(value, bool):
        raise _invalid(
            f"{field_prefix}.{key} must be a boolean",
            field=f"{field_prefix}.{key}",
            value=value,
        )
    return value


def _required_text(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise _invalid(
            f"{field_prefix}.{key} must be non-empty text",
            field=f"{field_prefix}.{key}",
            value=value,
        )
    return value.strip()


def _optional_text(payload: Mapping[str, Any], key: str, *, field_prefix: str) -> str | None:
    if key not in payload or payload[key] is None:
        return None
    value = payload[key]
    if not isinstance(value, str) or not value.strip():
        raise _invalid(
            f"{field_prefix}.{key} must be non-empty text when supplied",
            field=f"{field_prefix}.{key}",
            value=value,
        )
    return value.strip()


def _aware_datetime(value: Any, *, field: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip()
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise _invalid("datetime field has invalid ISO format", field=field, value=value) from exc
    else:
        raise _invalid("datetime field must be datetime or ISO text", field=field, value=value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise _invalid("datetime field must be timezone-aware", field=field, value=value)
    return parsed.astimezone(UTC)


def _reason_code_from_mapping(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    for key in ("reason_code", "primary_error_code", "error_code", "code"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    context = payload.get("context")
    if isinstance(context, Mapping):
        value = context.get("reason_code")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _run_reason_code(run: SimulationDailyRun) -> str:
    payload = run.run_payload_json
    for key in (
        "local_sim_projection_terminal_failure",
        "local_sim_projection_readback_terminal_failure",
        "local_sim_projection_readback_failure",
        "submit_failure",
        "selection_inference_failure",
        "binding_precheck_failure",
        "miniqmt_event_loop_tick_driver_timeout",
    ):
        reason_code = _reason_code_from_mapping(payload.get(key))
        if reason_code:
            return reason_code
    return f"SIMULATION_RUN_{run.status.value}"


def _health_for_run(run: SimulationDailyRun) -> tuple[str, str]:
    if run.status in BLOCKING_RUN_STATUSES:
        return "BLOCKED", _run_reason_code(run)
    if run.status == SimulationDailyRunStatus.CANCELLED:
        return "DEGRADED", "SIMULATION_RUN_CANCELLED"
    if run.status == SimulationDailyRunStatus.SUCCEEDED:
        return "HEALTHY", "SIMULATION_RUN_SUCCEEDED"
    return "IN_PROGRESS", f"SIMULATION_RUN_{run.status.value}"


def _alert_id(*, alert_type: str, reason_code: str, identity: Mapping[str, Any]) -> str:
    canonical = json.dumps(
        {
            "alert_type": alert_type,
            "reason_code": reason_code,
            "identity": dict(sorted((str(key), value) for key, value in identity.items())),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"simalert_{hashlib.sha256(canonical.encode('utf-8')).hexdigest()[:24]}"


def _alert(
    *,
    alert_type: str,
    status: str,
    reason_code: str,
    source: str,
    identity: Mapping[str, Any] | None = None,
    context: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    exact_identity = {str(key): value for key, value in dict(identity or {}).items() if value is not None}
    exact_context = {str(key): value for key, value in dict(context or {}).items() if value is not None}
    if len(exact_context) > 20 or any(
        len(str(key)) > 64 or len(str(value)) > 512 for key, value in exact_context.items()
    ):
        raise _invalid(
            "simulation platform alert context exceeds its bounded schema",
            field="alerts.context",
            context_count=len(exact_context),
        )
    return {
        "schema_version": "simulation_platform_alert_v1",
        "alert_id": _alert_id(alert_type=alert_type, reason_code=reason_code, identity=exact_identity),
        "alert_type": alert_type,
        "status": status,
        "reason_code": reason_code,
        "source": source,
        "identity": exact_identity,
        "context": exact_context,
        "active": True,
        "execution_gate": False,
        "acknowledge_required": False,
        "auto_clears_on_recovery": True,
    }


def _metric(
    *,
    name: str,
    kind: str,
    value: int | float,
    labels: Mapping[str, Any],
) -> dict[str, Any]:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise _invalid("metric value must be non-negative numeric", field=f"metrics.{name}.value", value=value)
    label_keys = frozenset(str(key) for key in labels)
    forbidden = sorted(label_keys.intersection(METRIC_HIGH_CARDINALITY_LABELS))
    unexpected = sorted(label_keys.difference(METRIC_LABEL_ALLOWLIST))
    if forbidden or unexpected:
        raise _invalid(
            "metric labels violate the platform cardinality contract",
            field=f"metrics.{name}.labels",
            forbidden_labels=forbidden,
            unexpected_labels=unexpected,
        )
    normalized_labels: dict[str, str] = {}
    for key, raw_value in labels.items():
        label_value = str(raw_value or "").strip()
        if not label_value or len(label_value) > 128:
            raise _invalid(
                "metric label value must be non-empty and bounded",
                field=f"metrics.{name}.labels.{key}",
                value=raw_value,
            )
        normalized_labels[str(key)] = label_value
    return {
        "schema_version": "simulation_platform_metric_v1",
        "name": name,
        "kind": kind,
        "value": value,
        "labels": dict(sorted(normalized_labels.items())),
    }


class SimulationPlatformObservability:
    """Build one exact, read-only health projection for LocalSIM and MiniQMT."""

    @staticmethod
    def run_runtime_id(run: SimulationDailyRun) -> str | None:
        payload = run.run_payload_json
        identity_values: list[tuple[str, str]] = []

        def collect(mapping: Mapping[str, Any] | None, key: str, field: str) -> None:
            if mapping is None or key not in mapping or mapping[key] is None:
                return
            value = mapping[key]
            if not isinstance(value, str) or not value.strip():
                raise _invalid("MiniQMT runtime identity must be non-empty text", field=field, value=value)
            identity_values.append((field, value.strip()))

        collect(payload, "miniqmt_runtime_id", "run_payload_json.miniqmt_runtime_id")
        batch = _optional_mapping(payload, "qmt_batch_result", field_prefix="run_payload_json")
        batch_evidence = (
            _optional_mapping(batch, "runtime_evidence", field_prefix="run_payload_json.qmt_batch_result")
            if batch is not None
            else None
        )
        collect(batch_evidence, "runtime_id", "run_payload_json.qmt_batch_result.runtime_evidence.runtime_id")
        tick_driver = _optional_mapping(payload, "miniqmt_event_loop_tick_driver", field_prefix="run_payload_json")
        collect(tick_driver, "runtime_id", "run_payload_json.miniqmt_event_loop_tick_driver.runtime_id")
        tick_evidence = (
            _optional_mapping(
                tick_driver,
                "runtime_evidence",
                field_prefix="run_payload_json.miniqmt_event_loop_tick_driver",
            )
            if tick_driver is not None
            else None
        )
        collect(
            tick_evidence,
            "runtime_id",
            "run_payload_json.miniqmt_event_loop_tick_driver.runtime_evidence.runtime_id",
        )
        distinct = sorted({value for _, value in identity_values})
        if len(distinct) > 1:
            raise RuntimeConfigInvalidError(
                "MiniQMT runtime identity conflicts across durable run carriers",
                context={
                    "reason_code": "SIMULATION_PLATFORM_RUNTIME_IDENTITY_CONFLICT",
                    "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
                    "run_id": run.run_id,
                    "identity_fields": [field for field, _ in identity_values],
                    "identity_values": distinct,
                },
            )
        return distinct[0] if distinct else None

    def build(
        self,
        *,
        scheduler_status: Mapping[str, Any],
        runs: Sequence[SimulationDailyRun],
        query: Mapping[str, Any],
        quote_diagnostics: Mapping[str, Any] | None = None,
        generated_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = generated_at or datetime.now(UTC)
        if now.tzinfo is None or now.utcoffset() is None:
            raise _invalid("generated_at must be timezone-aware", field="generated_at", value=now)
        exact_scheduler = dict(scheduler_status)
        exact_runs = list(runs)
        process_layer = self._process_layer(exact_scheduler, observed_at=now)
        market_phase = self._market_phase(exact_scheduler)
        binding_layers = [self._binding_layer(run) for run in exact_runs]
        durability_layers = [self._durability_layer(run, observed_at=now) for run in exact_runs]
        business_layers = [self._business_layer(run) for run in exact_runs]
        backend_layers = self._backend_layers(
            runs=exact_runs,
            quote_diagnostics=quote_diagnostics,
        )
        lifecycle_layer = self._lifecycle_layer(
            scheduler_status=exact_scheduler,
            runs=exact_runs,
            query=query,
            market_phase=market_phase,
        )
        alerts = self._alerts(
            process_layer=process_layer,
            lifecycle_layer=lifecycle_layer,
            binding_layers=binding_layers,
            durability_layers=durability_layers,
            business_layers=business_layers,
            backend_layers=backend_layers,
            market_phase=market_phase,
        )
        metrics = self._metrics(
            scheduler_status=exact_scheduler,
            runs=exact_runs,
            binding_layers=binding_layers,
            durability_layers=durability_layers,
            business_layers=business_layers,
            backend_layers=backend_layers,
            market_phase=market_phase,
            active_alert_count=len(alerts),
            generated_at=now,
        )
        overall = self._overall_health(
            process_layer=process_layer,
            lifecycle_layer=lifecycle_layer,
            binding_layers=binding_layers,
            durability_layers=durability_layers,
            business_layers=business_layers,
            backend_layers=backend_layers,
            market_phase=market_phase,
        )
        bounded_alerts = sorted(alerts, key=lambda item: (item["alert_type"], item["alert_id"]))[:ALERT_LIMIT]
        return {
            "schema_version": PLATFORM_DIAGNOSTICS_SCHEMA_VERSION,
            "generated_at": now.astimezone(UTC).isoformat(),
            "query": dict(query),
            "overall_health": overall,
            "layers": {
                "process": process_layer,
                "lifecycle": lifecycle_layer,
                "bindings": binding_layers,
                "backends": backend_layers,
                "durability": durability_layers,
                "business": business_layers,
            },
            "metrics": {
                "schema_version": PLATFORM_METRICS_SCHEMA_VERSION,
                "label_allowlist": sorted(METRIC_LABEL_ALLOWLIST),
                "forbidden_high_cardinality_labels": sorted(METRIC_HIGH_CARDINALITY_LABELS),
                "series_count": len(metrics),
                "bounded_limit": METRIC_SERIES_LIMIT,
                "truncated": False,
                "series": metrics,
                "execution_gate": False,
            },
            "alerts": {
                "schema_version": PLATFORM_ALERTS_SCHEMA_VERSION,
                "observed_count": len(alerts),
                "active_count": len(alerts),
                "returned_count": len(bounded_alerts),
                "bounded_limit": ALERT_LIMIT,
                "truncated": len(alerts) > ALERT_LIMIT,
                "items": bounded_alerts,
                "execution_gate": False,
                "acknowledge_required": False,
                "auto_clear_contract": "recompute_from_current_facts_and_remove_after_recovery",
            },
            "runbook": {
                "schema_version": PLATFORM_RUNBOOK_SCHEMA_VERSION,
                "path": "docs/operations/simulation_platform_operator_runbook_20260717.md",
                "ordered_steps": [
                    "process",
                    "lifecycle",
                    "binding",
                    "data_backend",
                    "durable_facts",
                    "broker_reconcile",
                    "tca",
                ],
                "read_only": True,
                "prohibited_actions": [
                    "restart_first",
                    "manual_state_edit",
                    "delete_durable_fact",
                    "broker_replay",
                    "manual_acknowledge_gate",
                ],
            },
            "side_effect_contract": {
                "read_only": True,
                "starts_feed": False,
                "writes_database": False,
                "calls_broker": False,
                "replays_order": False,
                "execution_gate": False,
            },
        }

    @staticmethod
    def _market_phase(scheduler_status: Mapping[str, Any]) -> str:
        last_result = scheduler_status.get("last_result")
        if isinstance(last_result, Mapping):
            raw_phase = last_result.get("market_phase") or last_result.get("phase")
            if raw_phase is not None:
                if not isinstance(raw_phase, str) or not raw_phase.strip():
                    raise _invalid(
                        "scheduler market phase must be non-empty text",
                        field="scheduler_status.last_result.market_phase",
                        value=raw_phase,
                    )
                return raw_phase.strip().upper()
        return "NOT_YET_OBSERVED"

    @staticmethod
    def _process_layer(
        scheduler_status: Mapping[str, Any],
        *,
        observed_at: datetime,
    ) -> dict[str, Any]:
        loop_health = _required_mapping(scheduler_status, "scheduler_loop_health", field_prefix="scheduler_status")
        loop_status = _required_text(loop_health, "status", field_prefix="scheduler_status.scheduler_loop_health")
        reason_code = _required_text(
            loop_health,
            "reason_code",
            field_prefix="scheduler_status.scheduler_loop_health",
        )
        running = _optional_exact_bool(scheduler_status, "running", field_prefix="scheduler_status")
        thread_alive = _optional_exact_bool(scheduler_status, "thread_alive", field_prefix="scheduler_status")
        if running is None or thread_alive is None:
            raise _invalid(
                "scheduler process state requires exact running and thread_alive booleans",
                field="scheduler_status.process",
            )
        interval_seconds = _optional_nonnegative_int(
            scheduler_status,
            "interval_seconds",
            field_prefix="scheduler_status",
        )
        if interval_seconds == 0:
            raise _invalid(
                "scheduler interval_seconds must be positive when supplied",
                field="scheduler_status.interval_seconds",
                value=interval_seconds,
            )
        last_run_at_raw = scheduler_status.get("last_run_at")
        if last_run_at_raw is None:
            last_run_at = None
            tick_lag_seconds = None
        else:
            last_run_at = _aware_datetime(last_run_at_raw, field="scheduler_status.last_run_at")
            tick_lag_seconds = max(0.0, (observed_at.astimezone(UTC) - last_run_at).total_seconds())
        tick_lag_threshold_seconds = (
            interval_seconds * SCHEDULER_TICK_LAG_MULTIPLIER if interval_seconds is not None else None
        )
        if loop_status == "BLOCKED":
            status = "BLOCKED"
        elif not running or not thread_alive:
            status = "INACTIVE"
            reason_code = "SIMULATION_SCHEDULER_PROCESS_INACTIVE"
        elif (
            tick_lag_seconds is not None
            and tick_lag_threshold_seconds is not None
            and tick_lag_seconds > tick_lag_threshold_seconds
        ):
            status = "DEGRADED"
            reason_code = "SIMULATION_SCHEDULER_TICK_LAG_EXCEEDED"
        elif loop_status == "NOT_YET_RUN":
            status = "NOT_YET_RUN"
        elif loop_status in {"HEALTHY", "NOT_APPLICABLE"}:
            status = "HEALTHY"
        else:
            raise _invalid(
                "scheduler loop health has unsupported status",
                field="scheduler_status.scheduler_loop_health.status",
                value=loop_status,
            )
        return {
            "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
            "layer": "process",
            "status": status,
            "reason_code": reason_code,
            "source": "scheduler_status.scheduler_loop_health",
            "facts": {
                "running": running,
                "thread_alive": thread_alive,
                "last_run_at": last_run_at.isoformat() if last_run_at is not None else None,
                "loop_status": loop_status,
                "interval_seconds": interval_seconds,
                "tick_lag_seconds": tick_lag_seconds,
                "tick_lag_threshold_seconds": tick_lag_threshold_seconds,
            },
            "execution_gate": False,
        }

    @staticmethod
    def _lifecycle_layer(
        *,
        scheduler_status: Mapping[str, Any],
        runs: Sequence[SimulationDailyRun],
        query: Mapping[str, Any],
        market_phase: str,
    ) -> dict[str, Any]:
        blockers = _required_mapping(
            scheduler_status,
            "current_trade_date_blockers",
            field_prefix="scheduler_status",
        )
        blocker_count = _required_nonnegative_int(
            blockers,
            "blocker_count",
            field_prefix="scheduler_status.current_trade_date_blockers",
        )
        failed_run_count = sum(run.status in BLOCKING_RUN_STATUSES for run in runs)
        active_run_count = sum(run.status in ACTIVE_RUN_STATUSES for run in runs)
        if failed_run_count:
            status = "BLOCKED"
            reason_code = "SIMULATION_QUERY_HAS_BLOCKING_RUNS"
        elif not runs:
            status = "NOT_YET_RUN"
            reason_code = "SIMULATION_QUERY_HAS_NO_RUNS"
        elif active_run_count:
            status = "IN_PROGRESS"
            reason_code = "SIMULATION_QUERY_HAS_ACTIVE_RUNS"
        else:
            status = "HEALTHY"
            reason_code = "SIMULATION_QUERY_RUNS_TERMINAL_WITHOUT_BLOCKER"
        return {
            "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
            "layer": "lifecycle",
            "status": status,
            "reason_code": reason_code,
            "source": "simulation_daily_run+current_trade_date_blockers",
            "facts": {
                "trade_date": query.get("trade_date"),
                "market_phase": market_phase,
                "run_count": len(runs),
                "active_run_count": active_run_count,
                "failed_run_count": failed_run_count,
                "current_trade_date_blocker_count": blocker_count,
            },
            "execution_gate": False,
        }

    @staticmethod
    def _binding_layer(run: SimulationDailyRun) -> dict[str, Any]:
        status, reason_code = _health_for_run(run)
        return {
            "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
            "layer": "binding",
            "status": status,
            "reason_code": reason_code,
            "source": "simulation_daily_run",
            "identity": {
                "trade_date": run.trade_date.isoformat(),
                "binding_id": run.binding_id,
                "run_id": run.run_id,
                "runtime_id": SimulationPlatformObservability.run_runtime_id(run),
                "plan_id": run.execution_plan_id,
            },
            "facts": {
                "backend": run.broker_backend.value,
                "run_status": run.status.value,
                "last_stage": str(run.run_payload_json.get("last_stage") or run.status.value),
                "updated_at": run.updated_at.isoformat(),
            },
            "execution_gate": False,
        }

    @staticmethod
    def _durability_layer(
        run: SimulationDailyRun,
        *,
        observed_at: datetime,
    ) -> dict[str, Any]:
        payload = run.run_payload_json
        identity = {"binding_id": run.binding_id, "run_id": run.run_id}
        if run.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            persistence = _optional_mapping(payload, "local_sim_persistence", field_prefix="run_payload_json")
            outbox = _optional_mapping(payload, "local_sim_projection_outbox_v1", field_prefix="run_payload_json")
            terminal_failure = _optional_mapping(
                payload,
                "local_sim_projection_terminal_failure",
                field_prefix="run_payload_json",
            )
            readback_terminal = _optional_mapping(
                payload,
                "local_sim_projection_readback_terminal_failure",
                field_prefix="run_payload_json",
            )
            readback_failure = _optional_mapping(
                payload,
                "local_sim_projection_readback_failure",
                field_prefix="run_payload_json",
            )
            if persistence is not None:
                schema_version = _required_text(
                    persistence,
                    "schema_version",
                    field_prefix="run_payload_json.local_sim_persistence",
                )
                if schema_version not in {"local_sim_persistence_v1", "local_sim_persistence_v2"}:
                    raise _invalid(
                        "LocalSIM persistence schema is unsupported",
                        field="run_payload_json.local_sim_persistence.schema_version",
                        value=schema_version,
                    )
                persistence_status = _required_text(
                    persistence,
                    "status",
                    field_prefix="run_payload_json.local_sim_persistence",
                )
                if persistence_status not in LOCAL_SIM_PERSISTENCE_STATUSES:
                    raise _invalid(
                        "LocalSIM persistence status is unsupported",
                        field="run_payload_json.local_sim_persistence.status",
                        value=persistence_status,
                    )
                for key in (
                    "order_count",
                    "fill_count",
                    "order_event_count",
                    "cash_ledger_count",
                    "position_count",
                ):
                    _optional_nonnegative_int(
                        persistence,
                        key,
                        field_prefix="run_payload_json.local_sim_persistence",
                    )
            else:
                persistence_status = None
            if outbox is not None:
                outbox_status = _required_text(
                    outbox,
                    "status",
                    field_prefix="run_payload_json.local_sim_projection_outbox_v1",
                )
                if outbox_status not in LOCAL_SIM_OUTBOX_STATUSES:
                    raise _invalid(
                        "LocalSIM projection outbox status is unsupported",
                        field="run_payload_json.local_sim_projection_outbox_v1.status",
                        value=outbox_status,
                    )
                outbox_attempt_count = 0
                for key in ("attempt_count", "readback_attempt_count", "generation"):
                    parsed_count = _optional_nonnegative_int(
                        outbox,
                        key,
                        field_prefix="run_payload_json.local_sim_projection_outbox_v1",
                    )
                    if key == "attempt_count" and parsed_count is not None:
                        outbox_attempt_count = parsed_count
            else:
                outbox_status = None
                outbox_attempt_count = 0
            outbox_age_seconds = max(
                0.0,
                (observed_at.astimezone(UTC) - run.updated_at.astimezone(UTC)).total_seconds(),
            )
            if terminal_failure is not None or readback_terminal is not None:
                status = "BLOCKED"
                reason_code = (
                    _reason_code_from_mapping(terminal_failure)
                    or _reason_code_from_mapping(readback_terminal)
                    or "LOCAL_SIM_PROJECTION_TERMINAL_FAILURE"
                )
            elif (
                readback_failure is not None
                or outbox_status == "PROJECTION_RETRYABLE"
                or (outbox_status == "PENDING" and outbox_age_seconds > LOCAL_SIM_OUTBOX_BACKLOG_ALERT_SECONDS)
            ):
                status = "DEGRADED"
                reason_code = _reason_code_from_mapping(readback_failure) or "LOCAL_SIM_PROJECTION_BACKLOG"
            elif outbox_status == "PENDING":
                status = "IN_PROGRESS"
                reason_code = "LOCAL_SIM_PROJECTION_PENDING_WITHIN_THRESHOLD"
            elif run.status == SimulationDailyRunStatus.SUCCEEDED and persistence_status not in {
                "PERSISTED",
                "PERSISTED_WITH_CAPACITY_RESIDUAL",
            }:
                status = "BLOCKED"
                reason_code = "LOCAL_SIM_DURABILITY_EVIDENCE_MISSING"
            elif persistence_status in {"PERSISTED", "PERSISTED_WITH_CAPACITY_RESIDUAL"}:
                status = "HEALTHY"
                reason_code = "LOCAL_SIM_DURABILITY_CLOSED"
            else:
                status = "IN_PROGRESS"
                reason_code = "LOCAL_SIM_DURABILITY_NOT_YET_COMMITTED"
            return {
                "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
                "layer": "durability",
                "status": status,
                "reason_code": reason_code,
                "source": "local_sim_economic_receipt+projection_outbox+readback",
                "identity": identity,
                "facts": {
                    "backend": run.broker_backend.value,
                    "persistence_status": persistence_status,
                    "outbox_status": outbox_status,
                    "outbox_attempt_count": outbox_attempt_count,
                    "outbox_age_seconds": outbox_age_seconds if outbox_status else None,
                    "outbox_backlog_alert_seconds": LOCAL_SIM_OUTBOX_BACKLOG_ALERT_SECONDS,
                    "terminal_failure_present": terminal_failure is not None or readback_terminal is not None,
                    "readback_failure_present": readback_failure is not None,
                },
                "execution_gate": False,
            }

        batch = _optional_mapping(payload, "qmt_batch_result", field_prefix="run_payload_json")
        if batch is None:
            status, reason_code = (
                ("IN_PROGRESS", "MINIQMT_DURABLE_BATCH_NOT_YET_CREATED")
                if run.status in ACTIVE_RUN_STATUSES
                else ("HEALTHY", "MINIQMT_NO_BATCH_REQUIRED_FOR_TERMINAL_RUN")
            )
            return {
                "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
                "layer": "durability",
                "status": status,
                "reason_code": reason_code,
                "source": "simulation_daily_run",
                "identity": {**identity, "runtime_id": self_runtime_id(run)},
                "facts": {"backend": run.broker_backend.value, "batch_status": None, "total": None},
                "execution_gate": False,
            }
        batch_id = _required_text(batch, "batch_id", field_prefix="run_payload_json.qmt_batch_result")
        batch_status = _required_text(batch, "batch_status", field_prefix="run_payload_json.qmt_batch_result")
        if batch_status not in MINIQMT_BATCH_STATUSES:
            raise _invalid(
                "MiniQMT durable batch status is unsupported",
                field="run_payload_json.qmt_batch_result.batch_status",
                value=batch_status,
            )
        results = batch.get("results")
        if not isinstance(results, list) or any(not isinstance(item, Mapping) for item in results):
            raise _invalid(
                "MiniQMT durable batch results must be a list of mappings",
                field="run_payload_json.qmt_batch_result.results",
                value=results,
            )
        total = _required_nonnegative_int(batch, "total", field_prefix="run_payload_json.qmt_batch_result")
        if total != len(results):
            raise RuntimeConfigInvalidError(
                "MiniQMT durable batch result cardinality is inconsistent",
                context={
                    "reason_code": "SIMULATION_PLATFORM_DURABLE_BATCH_CARDINALITY_MISMATCH",
                    "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
                    "run_id": run.run_id,
                    "batch_id": batch_id,
                    "total": total,
                    "result_count": len(results),
                },
            )
        success = _optional_exact_bool(batch, "success", field_prefix="run_payload_json.qmt_batch_result")
        if success is None:
            raise _invalid(
                "MiniQMT durable batch success flag is required",
                field="run_payload_json.qmt_batch_result.success",
            )
        batch_counts = {
            key: _required_nonnegative_int(
                batch,
                key,
                field_prefix="run_payload_json.qmt_batch_result",
            )
            for key in ("succeeded", "failed", "pending")
        }
        if sum(batch_counts.values()) != total:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable batch counters do not close to total",
                context={
                    "reason_code": "SIMULATION_PLATFORM_DURABLE_BATCH_COUNT_MISMATCH",
                    "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
                    "run_id": run.run_id,
                    "batch_id": batch_id,
                    "total": total,
                    **batch_counts,
                },
            )
        status = (
            "BLOCKED"
            if batch_status in {OrderBatchStatus.FAILED.value, OrderBatchStatus.PREFLIGHT_FAILED.value}
            else "HEALTHY"
        )
        reason_code = "MINIQMT_DURABLE_BATCH_FAILED" if status == "BLOCKED" else "MINIQMT_DURABLE_BATCH_VALID"
        return {
            "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
            "layer": "durability",
            "status": status,
            "reason_code": reason_code,
            "source": "qmt_batch_result",
            "identity": {**identity, "runtime_id": self_runtime_id(run), "batch_id": batch_id},
            "facts": {
                "backend": run.broker_backend.value,
                "batch_status": batch_status,
                "total": total,
                "success": success,
                **batch_counts,
            },
            "execution_gate": False,
        }

    @staticmethod
    def _business_layer(run: SimulationDailyRun) -> dict[str, Any]:
        payload = run.run_payload_json
        submitted = _optional_nonnegative_int(payload, "submitted_intents", field_prefix="run_payload_json")
        failed = _optional_nonnegative_int(payload, "failed_intents", field_prefix="run_payload_json")
        pending = _optional_nonnegative_int(payload, "pending_intents", field_prefix="run_payload_json")
        if run.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            batch = _optional_mapping(payload, "qmt_batch_result", field_prefix="run_payload_json")
            if batch is not None:
                aliases = {
                    "submitted": (
                        submitted,
                        _required_nonnegative_int(batch, "succeeded", field_prefix="run_payload_json.qmt_batch_result"),
                    ),
                    "failed": (
                        failed,
                        _required_nonnegative_int(batch, "failed", field_prefix="run_payload_json.qmt_batch_result"),
                    ),
                    "pending": (
                        pending,
                        _required_nonnegative_int(batch, "pending", field_prefix="run_payload_json.qmt_batch_result"),
                    ),
                }
                for name, (top_value, batch_value) in aliases.items():
                    if top_value is not None and top_value != batch_value:
                        raise RuntimeConfigInvalidError(
                            "MiniQMT business counters conflict with the durable batch",
                            context={
                                "reason_code": "SIMULATION_PLATFORM_BUSINESS_COUNT_CONFLICT",
                                "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
                                "run_id": run.run_id,
                                "field": name,
                                "top_level_value": top_value,
                                "batch_value": batch_value,
                            },
                        )
                submitted = aliases["submitted"][1]
                failed = aliases["failed"][1]
                pending = aliases["pending"][1]
        execution_states = payload.get("local_sim_execution_states_v1")
        if execution_states is not None:
            if not isinstance(execution_states, list) or any(
                not isinstance(item, Mapping) for item in execution_states
            ):
                raise _invalid(
                    "LocalSIM execution states must be a list of mappings",
                    field="run_payload_json.local_sim_execution_states_v1",
                    value=execution_states,
                )
            parsed_states: list[LocalSimExecutionStateV1] = []
            for index, item in enumerate(execution_states):
                try:
                    parsed_states.append(LocalSimExecutionStateV1.model_validate(item))
                except Exception as exc:  # noqa: BLE001 - durable state validation must use one stable reason.
                    raise RuntimeConfigInvalidError(
                        "LocalSIM durable execution state is invalid",
                        context={
                            "reason_code": "SIMULATION_PLATFORM_LOCAL_SIM_STATE_INVALID",
                            "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
                            "run_id": run.run_id,
                            "state_index": index,
                            "error_type": type(exc).__name__,
                            "error": str(exc)[:2048],
                        },
                    ) from exc
            state_statuses = Counter(state.runtime_status.value for state in parsed_states)
        else:
            parsed_states = []
            state_statuses = Counter()
        residual_count = state_statuses.get("EXPIRED_WITH_RESIDUAL", 0)
        active_algo_count = sum(state_statuses.get(status, 0) for status in ("WAITING_FOR_CAUSAL_BAR", "ACTIVE"))
        partial_count = sum(state.filled_quantity > 0 and state.remaining_quantity > 0 for state in parsed_states)
        bar_lag_candidates = [
            max(0.0, (state.causality_cursor - state.last_processed_bar_time).total_seconds())
            for state in parsed_states
            if state.last_processed_bar_time is not None
        ]
        max_bar_lag_seconds = max(bar_lag_candidates) if bar_lag_candidates else None
        reconciliation = _optional_mapping(payload, "reconcile_after_submit", field_prefix="run_payload_json")
        reconcile_status = None
        mismatch_count = 0
        if reconciliation is not None:
            run_reconciliation = _optional_mapping(
                reconciliation,
                "run",
                field_prefix="run_payload_json.reconcile_after_submit",
            )
            if run_reconciliation is not None:
                reconcile_status = _required_text(
                    run_reconciliation,
                    "status",
                    field_prefix="run_payload_json.reconcile_after_submit.run",
                )
            issues = reconciliation.get("issues")
            if issues is not None:
                if not isinstance(issues, list) or any(not isinstance(item, Mapping) for item in issues):
                    raise _invalid(
                        "MiniQMT reconciliation issues must be a list of mappings",
                        field="run_payload_json.reconcile_after_submit.issues",
                        value=issues,
                    )
                mismatch_count = len(issues)
        if run.status == SimulationDailyRunStatus.SUCCEEDED and (pending or active_algo_count):
            status = "BLOCKED"
            reason_code = "SIMULATION_TERMINAL_RUN_HAS_ACTIVE_WORK"
        elif run.status in BLOCKING_RUN_STATUSES:
            status = "BLOCKED"
            reason_code = _run_reason_code(run)
        elif residual_count or reconcile_status in {"WARNING", "FAILED", "BLOCKED"}:
            status = "DEGRADED"
            reason_code = (
                "LOCAL_SIM_TERMINAL_CAPACITY_RESIDUAL" if residual_count else "MINIQMT_RECONCILIATION_NOT_CLEAN"
            )
        elif run.status in ACTIVE_RUN_STATUSES:
            status = "IN_PROGRESS"
            reason_code = "SIMULATION_BUSINESS_WORK_IN_PROGRESS"
        else:
            status = "HEALTHY"
            reason_code = "SIMULATION_BUSINESS_FACTS_CLOSED"
        return {
            "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
            "layer": "business",
            "status": status,
            "reason_code": reason_code,
            "source": "simulation_daily_run_business_facts",
            "identity": {"binding_id": run.binding_id, "run_id": run.run_id},
            "facts": {
                "backend": run.broker_backend.value,
                "submitted_intents": submitted,
                "failed_intents": failed,
                "pending_intents": pending,
                "active_algo_count": active_algo_count,
                "partial_count": partial_count,
                "residual_count": residual_count,
                "max_bar_lag_seconds": max_bar_lag_seconds,
                "causal_bar_lag_alert_seconds": LOCAL_SIM_CAUSAL_BAR_LAG_ALERT_SECONDS,
                "reconcile_status": reconcile_status,
                "reconcile_mismatch_count": mismatch_count,
            },
            "execution_gate": False,
        }

    @staticmethod
    def _backend_layers(
        *,
        runs: Sequence[SimulationDailyRun],
        quote_diagnostics: Mapping[str, Any] | None,
    ) -> list[dict[str, Any]]:
        layers: list[dict[str, Any]] = []
        for backend in SimulationBrokerBackend:
            backend_runs = [run for run in runs if run.broker_backend == backend]
            blocked = sum(run.status in BLOCKING_RUN_STATUSES for run in backend_runs)
            active = sum(run.status in ACTIVE_RUN_STATUSES for run in backend_runs)
            status = "BLOCKED" if blocked else "IN_PROGRESS" if active else "HEALTHY"
            reason_code = (
                "SIMULATION_BACKEND_HAS_BLOCKING_RUNS"
                if blocked
                else "SIMULATION_BACKEND_HAS_ACTIVE_RUNS"
                if active
                else "SIMULATION_BACKEND_RUNS_CLEAR"
            )
            facts: dict[str, Any] = {
                "backend": backend.value,
                "run_count": len(backend_runs),
                "blocking_run_count": blocked,
                "active_run_count": active,
            }
            if backend == SimulationBrokerBackend.MINIQMT_SIM and quote_diagnostics is not None:
                health = _required_mapping(quote_diagnostics, "health", field_prefix="quote_diagnostics")
                quote_status = _required_text(health, "status", field_prefix="quote_diagnostics.health")
                if quote_status not in {"HEALTHY", "DEGRADED", "FAILED", "INACTIVE"}:
                    raise _invalid(
                        "MiniQMT quote diagnostics health status is unsupported",
                        field="quote_diagnostics.health.status",
                        value=quote_status,
                    )
                reason_codes = health.get("reason_codes")
                if not isinstance(reason_codes, list) or any(
                    not isinstance(item, str) or not item.strip() for item in reason_codes
                ):
                    raise _invalid(
                        "MiniQMT quote diagnostics reason_codes must be a text list",
                        field="quote_diagnostics.health.reason_codes",
                        value=reason_codes,
                    )
                facts["quote_health_status"] = quote_status
                facts["quote_health_reason_codes"] = list(reason_codes)
                facts["runtime_id"] = health.get("runtime_id")
                durable_health = _required_mapping(
                    health,
                    "durable_health",
                    field_prefix="quote_diagnostics.health",
                )
                facts["callback_age_ms"] = _optional_nonnegative_int(
                    durable_health,
                    "age_ms",
                    field_prefix="quote_diagnostics.health.durable_health",
                )
                runtime_projection = _required_mapping(
                    health,
                    "runtime_projection",
                    field_prefix="quote_diagnostics.health",
                )
                facts["control_revision"] = _optional_text(
                    runtime_projection,
                    "control_revision",
                    field_prefix="quote_diagnostics.health.runtime_projection",
                )
                events = quote_diagnostics.get("events")
                if not isinstance(events, list) or any(not isinstance(item, Mapping) for item in events):
                    raise _invalid(
                        "MiniQMT quote diagnostics events must be a list of mappings",
                        field="quote_diagnostics.events",
                        value=events,
                    )
                event_types = Counter(
                    _required_text(item, "event_type", field_prefix="quote_diagnostics.events[]") for item in events
                )
                facts["recent_normalized_count"] = event_types.get("QUOTE_NORMALIZED", 0)
                facts["recent_rejected_count"] = sum(
                    event_types.get(event_type, 0)
                    for event_type in ("QUOTE_REJECTED", "QUOTE_UNAVAILABLE", "QUOTE_INVALID")
                )
                if quote_status == "FAILED":
                    status = "BLOCKED"
                    reason_code = reason_codes[0] if reason_codes else "MINIQMT_QUOTE_HEALTH_FAILED"
                elif quote_status == "DEGRADED" and status != "BLOCKED":
                    status = "DEGRADED"
                    reason_code = reason_codes[0] if reason_codes else "MINIQMT_QUOTE_HEALTH_DEGRADED"
            layers.append(
                {
                    "schema_version": PLATFORM_LAYER_SCHEMA_VERSION,
                    "layer": "backend",
                    "status": status,
                    "reason_code": reason_code,
                    "source": "simulation_daily_run+canonical_backend_diagnostics",
                    "identity": {"backend": backend.value},
                    "facts": facts,
                    "execution_gate": False,
                }
            )
        return layers

    @staticmethod
    def _alerts(
        *,
        process_layer: Mapping[str, Any],
        lifecycle_layer: Mapping[str, Any],
        binding_layers: Sequence[Mapping[str, Any]],
        durability_layers: Sequence[Mapping[str, Any]],
        business_layers: Sequence[Mapping[str, Any]],
        backend_layers: Sequence[Mapping[str, Any]],
        market_phase: str,
    ) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        if process_layer["status"] == "BLOCKED" or (
            process_layer["status"] == "INACTIVE" and market_phase in ACTIVE_MARKET_PHASES
        ):
            alerts.append(
                _alert(
                    alert_type="SIMULATION_SCHEDULER_HEALTH",
                    status="CRITICAL",
                    reason_code=str(process_layer["reason_code"]),
                    source=str(process_layer["source"]),
                    context={"market_phase": market_phase},
                )
            )
        elif (
            process_layer["status"] == "DEGRADED"
            and process_layer["reason_code"] == "SIMULATION_SCHEDULER_TICK_LAG_EXCEEDED"
            and market_phase in ACTIVE_MARKET_PHASES
        ):
            alerts.append(
                _alert(
                    alert_type="SIMULATION_SCHEDULER_TICK_LAG",
                    status="WARNING",
                    reason_code=str(process_layer["reason_code"]),
                    source=str(process_layer["source"]),
                    context={
                        "market_phase": market_phase,
                        "tick_lag_seconds": process_layer["facts"].get("tick_lag_seconds"),
                        "threshold_seconds": process_layer["facts"].get("tick_lag_threshold_seconds"),
                    },
                )
            )
        if lifecycle_layer["status"] == "BLOCKED":
            alerts.append(
                _alert(
                    alert_type="SIMULATION_LIFECYCLE_BLOCKED",
                    status="CRITICAL",
                    reason_code=str(lifecycle_layer["reason_code"]),
                    source=str(lifecycle_layer["source"]),
                    identity={"trade_date": lifecycle_layer["facts"].get("trade_date")},
                )
            )
        for layer in binding_layers:
            if layer["status"] == "BLOCKED":
                alerts.append(
                    _alert(
                        alert_type="SIMULATION_BINDING_BLOCKED",
                        status="CRITICAL",
                        reason_code=str(layer["reason_code"]),
                        source=str(layer["source"]),
                        identity=layer.get("identity"),
                    )
                )
                if "ROUTE_RETIRED" in str(layer["reason_code"]).upper():
                    alerts.append(
                        _alert(
                            alert_type="SIMULATION_RETIRED_ROUTE_CALLED",
                            status="CRITICAL",
                            reason_code=str(layer["reason_code"]),
                            source=str(layer["source"]),
                            identity=layer.get("identity"),
                        )
                    )
        for layer in durability_layers:
            if layer["status"] in {"BLOCKED", "DEGRADED"}:
                alerts.append(
                    _alert(
                        alert_type="SIMULATION_DURABILITY_FAILURE",
                        status="CRITICAL" if layer["status"] == "BLOCKED" else "WARNING",
                        reason_code=str(layer["reason_code"]),
                        source=str(layer["source"]),
                        identity=layer.get("identity"),
                    )
                )
        for layer in business_layers:
            if layer["status"] in {"BLOCKED", "DEGRADED"}:
                alerts.append(
                    _alert(
                        alert_type="SIMULATION_BUSINESS_CLOSURE",
                        status="CRITICAL" if layer["status"] == "BLOCKED" else "WARNING",
                        reason_code=str(layer["reason_code"]),
                        source=str(layer["source"]),
                        identity=layer.get("identity"),
                    )
                )
            facts = layer.get("facts") if isinstance(layer.get("facts"), Mapping) else {}
            bar_lag = facts.get("max_bar_lag_seconds")
            if (
                facts.get("backend") == SimulationBrokerBackend.LOCAL_SIM.value
                and isinstance(bar_lag, (int, float))
                and not isinstance(bar_lag, bool)
                and bar_lag > LOCAL_SIM_CAUSAL_BAR_LAG_ALERT_SECONDS
                and market_phase in ACTIVE_MARKET_PHASES
            ):
                alerts.append(
                    _alert(
                        alert_type="LOCAL_SIM_CAUSAL_BAR_NOT_PROGRESSING",
                        status="WARNING",
                        reason_code="LOCAL_SIM_CAUSAL_BAR_LAG_EXCEEDED",
                        source=str(layer["source"]),
                        identity=layer.get("identity"),
                        context={
                            "bar_lag_seconds": bar_lag,
                            "threshold_seconds": LOCAL_SIM_CAUSAL_BAR_LAG_ALERT_SECONDS,
                            "market_phase": market_phase,
                        },
                    )
                )
            if int(facts.get("active_algo_count") or 0) > 0 and market_phase in {"POST_CLOSE_RECONCILIATION", "CLOSED"}:
                alerts.append(
                    _alert(
                        alert_type="SIMULATION_ACTIVE_ALGO_AFTER_CLOSE",
                        status="CRITICAL",
                        reason_code="SIMULATION_ACTIVE_ALGO_MISSING_TERMINAL_CLASSIFICATION",
                        source=str(layer["source"]),
                        identity=layer.get("identity"),
                        context={"active_algo_count": facts.get("active_algo_count"), "market_phase": market_phase},
                    )
                )
        for layer in backend_layers:
            facts = layer.get("facts") if isinstance(layer.get("facts"), Mapping) else {}
            if layer["status"] in {"BLOCKED", "DEGRADED"} and facts.get("quote_health_status") in {
                "FAILED",
                "DEGRADED",
            }:
                alerts.append(
                    _alert(
                        alert_type="MINIQMT_QUOTE_PROGRESS",
                        status="CRITICAL" if layer["status"] == "BLOCKED" else "WARNING",
                        reason_code=str(layer["reason_code"]),
                        source=str(layer["source"]),
                        identity={
                            "backend": facts.get("backend"),
                            "runtime_id": facts.get("runtime_id"),
                        },
                    )
                )
        return alerts

    @staticmethod
    def _metrics(
        *,
        scheduler_status: Mapping[str, Any],
        runs: Sequence[SimulationDailyRun],
        binding_layers: Sequence[Mapping[str, Any]],
        durability_layers: Sequence[Mapping[str, Any]],
        business_layers: Sequence[Mapping[str, Any]],
        backend_layers: Sequence[Mapping[str, Any]],
        market_phase: str,
        active_alert_count: int,
        generated_at: datetime,
    ) -> list[dict[str, Any]]:
        loop_health = _required_mapping(scheduler_status, "scheduler_loop_health", field_prefix="scheduler_status")
        loop_status = _required_text(loop_health, "status", field_prefix="scheduler_status.scheduler_loop_health")
        loop_reason = _required_text(loop_health, "reason_code", field_prefix="scheduler_status.scheduler_loop_health")
        total_success = _required_nonnegative_int(
            loop_health,
            "total_success_count",
            field_prefix="scheduler_status.scheduler_loop_health",
        )
        total_failure = _required_nonnegative_int(
            loop_health,
            "total_failure_count",
            field_prefix="scheduler_status.scheduler_loop_health",
        )
        metrics = [
            _metric(
                name="simulation_scheduler_loop_status",
                kind="gauge",
                value=1,
                labels={
                    "status": loop_status,
                    "reason_code": loop_reason,
                    "market_phase": market_phase,
                    "source": "scheduler_loop_health",
                },
            ),
            _metric(
                name="simulation_scheduler_tick_success_total",
                kind="counter",
                value=total_success,
                labels={"status": loop_status, "source": "scheduler_loop_health"},
            ),
            _metric(
                name="simulation_scheduler_tick_failure_total",
                kind="counter",
                value=total_failure,
                labels={"status": loop_status, "source": "scheduler_loop_health"},
            ),
            _metric(
                name="simulation_active_alert_count",
                kind="gauge",
                value=active_alert_count,
                labels={"status": "ACTIVE", "source": "platform_diagnostics"},
            ),
        ]
        last_run_at_raw = scheduler_status.get("last_run_at")
        if last_run_at_raw is not None:
            last_run_at = _aware_datetime(last_run_at_raw, field="scheduler_status.last_run_at")
            metrics.append(
                _metric(
                    name="simulation_scheduler_tick_lag_seconds",
                    kind="gauge",
                    value=max(0.0, (generated_at.astimezone(UTC) - last_run_at).total_seconds()),
                    labels={
                        "status": loop_status,
                        "market_phase": market_phase,
                        "source": "scheduler_status.last_run_at",
                    },
                )
            )
        binding_counts = Counter(
            (
                str(layer["facts"]["backend"]),
                str(layer["status"]),
                str(layer["reason_code"]),
            )
            for layer in binding_layers
        )
        for (backend, status, reason_code), count in sorted(binding_counts.items()):
            metrics.append(
                _metric(
                    name="simulation_binding_health_count",
                    kind="gauge",
                    value=count,
                    labels={
                        "backend": backend,
                        "status": status,
                        "reason_code": reason_code,
                        "market_phase": market_phase,
                        "source": "simulation_daily_run",
                    },
                )
            )
        run_status_counts = Counter((run.broker_backend.value, run.status.value) for run in runs)
        for (backend, run_status), count in sorted(run_status_counts.items()):
            metrics.append(
                _metric(
                    name="simulation_binding_run_status_count",
                    kind="gauge",
                    value=count,
                    labels={
                        "backend": backend,
                        "status": run_status,
                        "market_phase": market_phase,
                        "source": "simulation_daily_run",
                    },
                )
            )
        for backend_layer in backend_layers:
            facts = backend_layer["facts"]
            metrics.append(
                _metric(
                    name="simulation_backend_run_count",
                    kind="gauge",
                    value=int(facts["run_count"]),
                    labels={
                        "backend": facts["backend"],
                        "status": backend_layer["status"],
                        "reason_code": backend_layer["reason_code"],
                        "source": "simulation_daily_run",
                    },
                )
            )
        local_active = sum(
            int(layer["facts"]["active_algo_count"])
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
        )
        local_residual = sum(
            int(layer["facts"]["residual_count"])
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
        )
        local_partial = sum(
            int(layer["facts"]["partial_count"])
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
        )
        local_bar_lags = [
            float(layer["facts"]["max_bar_lag_seconds"])
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
            and layer["facts"].get("max_bar_lag_seconds") is not None
        ]
        local_max_bar_lag = max(local_bar_lags) if local_bar_lags else 0.0
        outbox_backlog = sum(
            layer["facts"].get("outbox_status") in {"PENDING", "PROJECTION_RETRYABLE"}
            for layer in durability_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
        )
        miniqmt_pending = sum(
            int(layer["facts"].get("pending_intents") or 0)
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.MINIQMT_SIM.value
        )
        miniqmt_submitted = sum(
            int(layer["facts"].get("submitted_intents") or 0)
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.MINIQMT_SIM.value
        )
        reconcile_mismatch = sum(
            int(layer["facts"].get("reconcile_mismatch_count") or 0)
            for layer in business_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.MINIQMT_SIM.value
        )
        invalid_payload_count = sum(
            "INVALID" in str(layer["reason_code"]).upper()
            for layer in [*binding_layers, *durability_layers, *business_layers]
        )
        false_green_prevention_count = sum(
            layer["status"] == "BLOCKED" for layer in [*binding_layers, *durability_layers, *business_layers]
        )
        durable_readback_mismatch = sum(
            _optional_exact_bool(
                layer["facts"],
                "readback_failure_present",
                field_prefix="durability_layer.facts",
            )
            is True
            for layer in durability_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
        )
        local_transaction_failure = sum(
            layer["status"] == "BLOCKED"
            for layer in durability_layers
            if layer["facts"]["backend"] == SimulationBrokerBackend.LOCAL_SIM.value
        )
        aggregate_specs = (
            ("simulation_localsim_active_algo_count", local_active, SimulationBrokerBackend.LOCAL_SIM.value),
            ("simulation_localsim_partial_count", local_partial, SimulationBrokerBackend.LOCAL_SIM.value),
            ("simulation_localsim_residual_count", local_residual, SimulationBrokerBackend.LOCAL_SIM.value),
            ("simulation_localsim_causal_bar_lag_seconds", local_max_bar_lag, SimulationBrokerBackend.LOCAL_SIM.value),
            (
                "simulation_localsim_transaction_failure_count",
                local_transaction_failure,
                SimulationBrokerBackend.LOCAL_SIM.value,
            ),
            ("simulation_localsim_outbox_backlog_count", outbox_backlog, SimulationBrokerBackend.LOCAL_SIM.value),
            ("simulation_miniqmt_pending_algo_count", miniqmt_pending, SimulationBrokerBackend.MINIQMT_SIM.value),
            ("simulation_miniqmt_submitted_child_count", miniqmt_submitted, SimulationBrokerBackend.MINIQMT_SIM.value),
            (
                "simulation_miniqmt_reconcile_mismatch_count",
                reconcile_mismatch,
                SimulationBrokerBackend.MINIQMT_SIM.value,
            ),
            ("simulation_invalid_payload_count", invalid_payload_count, "all"),
            ("simulation_false_green_prevention_count", false_green_prevention_count, "all"),
            ("simulation_durable_readback_mismatch_count", durable_readback_mismatch, "all"),
        )
        for name, value, backend in aggregate_specs:
            metrics.append(
                _metric(
                    name=name,
                    kind="gauge",
                    value=value,
                    labels={
                        "backend": backend,
                        "status": "OBSERVED",
                        "market_phase": market_phase,
                        "source": "platform_diagnostics",
                    },
                )
            )
        miniqmt_layer = next(
            (
                layer
                for layer in backend_layers
                if layer["facts"]["backend"] == SimulationBrokerBackend.MINIQMT_SIM.value
            ),
            None,
        )
        if miniqmt_layer is not None:
            quote_facts = miniqmt_layer["facts"]
            quote_metrics = (
                ("simulation_miniqmt_callback_age_ms", quote_facts.get("callback_age_ms")),
                ("simulation_miniqmt_recent_normalized_count", quote_facts.get("recent_normalized_count")),
                ("simulation_miniqmt_recent_rejected_count", quote_facts.get("recent_rejected_count")),
            )
            for name, value in quote_metrics:
                if value is None:
                    continue
                labels = {
                    "backend": SimulationBrokerBackend.MINIQMT_SIM.value,
                    "status": str(quote_facts.get("quote_health_status") or miniqmt_layer["status"]),
                    "source": "miniqmt_quote_diagnostics",
                }
                control_revision = quote_facts.get("control_revision")
                if control_revision:
                    labels["control_revision"] = control_revision
                metrics.append(_metric(name=name, kind="gauge", value=value, labels=labels))
        if len(metrics) > METRIC_SERIES_LIMIT:
            raise RuntimeConfigInvalidError(
                "simulation platform metric series exceed the bounded contract",
                context={
                    "reason_code": "SIMULATION_PLATFORM_METRIC_CARDINALITY_EXCEEDED",
                    "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_PROJECTION",
                    "series_count": len(metrics),
                    "bounded_limit": METRIC_SERIES_LIMIT,
                },
            )
        return metrics

    @staticmethod
    def _overall_health(
        *,
        process_layer: Mapping[str, Any],
        lifecycle_layer: Mapping[str, Any],
        binding_layers: Sequence[Mapping[str, Any]],
        durability_layers: Sequence[Mapping[str, Any]],
        business_layers: Sequence[Mapping[str, Any]],
        backend_layers: Sequence[Mapping[str, Any]],
        market_phase: str,
    ) -> dict[str, Any]:
        all_layers = [
            process_layer,
            lifecycle_layer,
            *binding_layers,
            *durability_layers,
            *business_layers,
            *backend_layers,
        ]
        blocked_reasons = sorted({str(layer["reason_code"]) for layer in all_layers if layer["status"] == "BLOCKED"})
        degraded_reasons = sorted({str(layer["reason_code"]) for layer in all_layers if layer["status"] == "DEGRADED"})
        if blocked_reasons:
            status = "BLOCKED"
            reason_codes = blocked_reasons
        elif process_layer["status"] == "INACTIVE" and market_phase in ACTIVE_MARKET_PHASES:
            status = "BLOCKED"
            reason_codes = ["SIMULATION_SCHEDULER_INACTIVE_DURING_ACTIVE_MARKET_PHASE"]
        elif degraded_reasons:
            status = "DEGRADED"
            reason_codes = degraded_reasons
        elif lifecycle_layer["status"] == "NOT_YET_RUN":
            status = "NOT_YET_RUN"
            reason_codes = [str(lifecycle_layer["reason_code"])]
        elif any(layer["status"] == "IN_PROGRESS" for layer in all_layers):
            status = "IN_PROGRESS"
            reason_codes = ["SIMULATION_PLATFORM_WORK_IN_PROGRESS"]
        elif process_layer["status"] == "INACTIVE":
            status = "INACTIVE"
            reason_codes = [str(process_layer["reason_code"])]
        else:
            status = "HEALTHY"
            reason_codes = ["SIMULATION_PLATFORM_HEALTHY"]
        return {
            "schema_version": "simulation_platform_overall_health_v1",
            "status": status,
            "reason_codes": reason_codes,
            "market_phase": market_phase,
            "execution_gate": False,
            "acknowledge_required": False,
        }


def self_runtime_id(run: SimulationDailyRun) -> str | None:
    """Avoid duplicating the exact runtime-id validation inside layer builders."""

    return SimulationPlatformObservability.run_runtime_id(run)
