"""Operator projections and controlled scheduler operations for the unified simulation runtime."""

from __future__ import annotations

import os
from collections import Counter
from datetime import UTC, date, datetime
from typing import Any, Mapping

from backend.services.miniqmt_execution_runtime.repository import MiniQMTExecutionRuntimeRepository
from backend.services.trading_core.errors import DataUnavailableError, RuntimeConfigInvalidError

from .models import ExecutionPlan, SimulationBrokerBackend, SimulationDailyRun, SimulationDailyRunStatus
from .platform_observability import SimulationPlatformObservability
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
MINIQMT_DURABLE_HEALTH_STALE_CADENCE_MULTIPLIER = 2


def _required_scheduler_status_mapping(status: dict[str, Any], key: str) -> dict[str, Any]:
    value = status.get(key)
    if not isinstance(value, dict):
        raise RuntimeConfigInvalidError(
            f"scheduler status {key} must be a mapping",
            context={
                "reason_code": "SIMULATION_SCHEDULER_STATUS_INVALID",
                "stage": "SCHEDULER_STATUS_PROJECTION",
                "field": key,
            },
        )
    return dict(value)


def _enum_value(value: Any) -> str | None:
    if value is None:
        return None
    return str(getattr(value, "value", value))


def _required_positive_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeConfigInvalidError(
            f"{field} must be a positive integer",
            context={
                "reason_code": "MINIQMT_QUOTE_DIAGNOSTICS_CONFIG_INVALID",
                "stage": "MINIQMT_QUOTE_HEALTH_PROJECTION",
                "field": field,
                "value": value,
            },
        )
    return value


def _scheduler_bool(status: dict[str, Any], key: str, *, default: bool = False) -> bool:
    if key not in status:
        return default
    value = status[key]
    if not isinstance(value, bool):
        raise RuntimeConfigInvalidError(
            f"scheduler status {key} must be a boolean",
            context={
                "reason_code": "SIMULATION_SCHEDULER_STATUS_INVALID",
                "stage": "SCHEDULER_STATUS_PROJECTION",
                "field": key,
                "value_type": type(value).__name__,
            },
        )
    return value


def _scheduler_positive_int(status: dict[str, Any], key: str) -> int | None:
    if key not in status or status[key] is None:
        return None
    value = status[key]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise RuntimeConfigInvalidError(
            f"scheduler status {key} must be a positive integer",
            context={
                "reason_code": "SIMULATION_SCHEDULER_STATUS_INVALID",
                "stage": "SCHEDULER_STATUS_PROJECTION",
                "field": key,
                "value": value,
            },
        )
    return value


def _run_projection_bool(
    payload: Mapping[str, Any],
    key: str,
    *,
    field_prefix: str,
    required: bool,
    default: bool = False,
) -> bool:
    if key not in payload or payload[key] is None:
        if not required:
            return default
        raise DataUnavailableError(
            f"{field_prefix}.{key} is required",
            context={
                "reason_code": "SIMULATION_RUN_PAYLOAD_BOOLEAN_MISSING",
                "stage": "RUN_DETAIL_PROJECTION",
                "field": f"{field_prefix}.{key}",
            },
        )
    value = payload[key]
    if not isinstance(value, bool):
        raise DataUnavailableError(
            f"{field_prefix}.{key} must be a boolean",
            context={
                "reason_code": "SIMULATION_RUN_PAYLOAD_BOOLEAN_INVALID",
                "stage": "RUN_DETAIL_PROJECTION",
                "field": f"{field_prefix}.{key}",
                "value_type": type(value).__name__,
            },
        )
    return value


def _runtime_controller_health(registry: dict[str, Any], runtime_id: str) -> dict[str, Any] | None:
    controllers = registry.get("controllers")
    if isinstance(controllers, dict) and isinstance(controllers.get(runtime_id), dict):
        return dict(controllers[runtime_id])
    for key in ("delegate", "drain_factory"):
        nested = registry.get(key)
        if isinstance(nested, dict):
            found = _runtime_controller_health(nested, runtime_id)
            if found is not None:
                return found
    return None


def _canonical_miniqmt_quote_health(
    *,
    runtime: Any,
    runtime_id: str,
    durable_health: dict[str, Any] | None,
    durable_health_event: dict[str, Any] | None,
    scheduler_status: dict[str, Any],
) -> dict[str, Any]:
    activation = _required_scheduler_status_mapping(scheduler_status, "miniqmt_quote_ingress_activation")
    controller_registry = _required_scheduler_status_mapping(scheduler_status, "b0_quote_v2_controllers")
    ingress = dict(activation.get("ingress")) if isinstance(activation.get("ingress"), dict) else None
    subscription = (
        dict(ingress.get("subscription"))
        if isinstance(ingress, dict) and isinstance(ingress.get("subscription"), dict)
        else None
    )
    writer = (
        dict(ingress.get("writer")) if isinstance(ingress, dict) and isinstance(ingress.get("writer"), dict) else None
    )
    controller = _runtime_controller_health(controller_registry, runtime_id)
    durable_health_present = isinstance(durable_health, dict)
    cadence_seconds = (
        _required_positive_int(
            activation.get("evidence_cadence_seconds"),
            field="miniqmt_quote_ingress_activation.evidence_cadence_seconds",
        )
        if durable_health_present
        else None
    )
    durable_health_max_age_ms = (
        cadence_seconds * MINIQMT_DURABLE_HEALTH_STALE_CADENCE_MULTIPLIER * 1000
        if cadence_seconds is not None
        else None
    )
    event_time = durable_health_event.get("event_time") if isinstance(durable_health_event, dict) else None
    event_id = durable_health_event.get("event_id") if isinstance(durable_health_event, dict) else None
    event_sequence = durable_health_event.get("sequence") if isinstance(durable_health_event, dict) else None
    durable_event_valid = bool(
        isinstance(event_id, str)
        and event_id.strip()
        and isinstance(event_sequence, int)
        and not isinstance(event_sequence, bool)
        and event_sequence > 0
        and isinstance(event_time, datetime)
        and event_time.tzinfo is not None
    )
    if durable_event_valid:
        assert isinstance(event_time, datetime)
        event_time_utc = event_time.astimezone(UTC)
        event_time_text = event_time_utc.isoformat()
        age_ms = int((datetime.now(UTC) - event_time_utc).total_seconds() * 1000)
    else:
        event_time_text = None
        age_ms = None
    durable_reported = durable_health_present and durable_event_valid
    durable_status = str(durable_health.get("status") or "").strip().upper() if durable_health_present else None
    runtime_projection = {
        "event_loop_state": _enum_value(getattr(runtime, "event_loop_state", None)),
        "gateway_state": _enum_value(getattr(runtime, "gateway_state", None)),
        "oms_state": _enum_value(getattr(runtime, "oms_state", None)),
        "last_event_sequence": int(getattr(runtime, "last_event_sequence", 0) or 0),
        "updated_at": getattr(runtime, "updated_at", None).isoformat()
        if isinstance(getattr(runtime, "updated_at", None), datetime)
        else None,
    }
    runtime_state = str(runtime_projection["event_loop_state"] or "UNKNOWN")
    inactive_runtime = runtime_state == "STOPPED"
    reasons: list[str] = []
    durable_projection_failed = False
    if durable_health_present and not durable_event_valid:
        reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_READBACK_INVALID")
        durable_projection_failed = True
    elif not durable_reported:
        reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_NOT_DURABLY_REPORTED")
    if durable_reported:
        if durable_status not in {"HEALTHY", "DEGRADED", "FAILED"}:
            reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_STATUS_INVALID")
            durable_projection_failed = True
        elif durable_status == "FAILED":
            reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_REPORTED_FAILED")
            durable_projection_failed = True
        elif durable_status == "DEGRADED":
            reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_REPORTED_DEGRADED")
        if age_ms is not None and age_ms < 0:
            reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_EVENT_TIME_IN_FUTURE")
            durable_projection_failed = True
        elif age_ms is not None and durable_health_max_age_ms is not None and age_ms > durable_health_max_age_ms:
            reasons.append("MINIQMT_QUOTE_INGRESS_HEALTH_STALE")
    activation_status = str(activation.get("status") or "UNKNOWN")
    writer_status = str(writer.get("status") or "UNKNOWN") if writer is not None else "UNKNOWN"
    subscription_status = str(subscription.get("status") or "UNKNOWN") if subscription is not None else "UNKNOWN"
    controller_status = str(controller.get("status") or "UNKNOWN") if controller is not None else "UNKNOWN"
    if not inactive_runtime and activation_status not in {"READY", "DRAINING"}:
        reasons.append("MINIQMT_QUOTE_INGRESS_ACTIVATION_NOT_READY")
    if not inactive_runtime and writer_status not in {"ACTIVE", "STARTING"}:
        reasons.append("MINIQMT_QUOTE_INGRESS_WRITER_NOT_ACTIVE")
    if not inactive_runtime and subscription_status not in {"ACTIVE", "READY"}:
        reasons.append("MINIQMT_QUOTE_CALLBACK_SUBSCRIPTION_NOT_ACTIVE")
    if not inactive_runtime and controller_status not in {"HEALTHY", "READY"}:
        reasons.append("MINIQMT_B0_QUOTE_CONTROLLER_NOT_HEALTHY")
    if not inactive_runtime and runtime_projection["gateway_state"] != "CONNECTED":
        reasons.append("MINIQMT_RUNTIME_GATEWAY_NOT_CONNECTED")
    if runtime_projection["oms_state"] != "RECONCILED":
        reasons.append("MINIQMT_RUNTIME_OMS_NOT_RECONCILED")

    explicit_failure_states = {"FAILED", "BLOCKED"}
    component_states = {
        activation_status,
        writer_status,
        subscription_status,
        controller_status,
        str(runtime_projection["event_loop_state"] or "UNKNOWN"),
        str(runtime_projection["gateway_state"] or "UNKNOWN"),
        str(runtime_projection["oms_state"] or "UNKNOWN"),
    }
    if durable_projection_failed or component_states.intersection(explicit_failure_states):
        status = "FAILED"
    elif inactive_runtime:
        status = "INACTIVE"
    else:
        status = "HEALTHY" if not reasons else "DEGRADED"
    return {
        "schema_version": "miniqmt_quote_canonical_health_v1",
        "authority": "simulation_runtime_miniqmt_quote_diagnostics",
        "authoritative": True,
        "runtime_id": runtime_id,
        "status": status,
        "reason_codes": sorted(set(reasons)),
        "durable_health": {
            "reported": durable_reported,
            "durable_ack": durable_reported,
            "readback_verified": durable_reported,
            "status": durable_status,
            "event_id": event_id,
            "sequence": event_sequence,
            "event_time": event_time_text,
            "age_ms": age_ms,
            "max_age_ms": durable_health_max_age_ms,
            "stale": (
                age_ms is not None and durable_health_max_age_ms is not None and age_ms > durable_health_max_age_ms
            ),
            "health_or_aggregate": dict(durable_health) if durable_health is not None else None,
        },
        "live_components": {
            "activation": activation,
            "callback_subscription": subscription,
            "writer": writer,
            "controller": controller,
        },
        "runtime_projection": runtime_projection,
        "legacy_status": {
            "authority": "legacy_miniqmt_interface",
            "authoritative": False,
            "retired_for_simulation_runtime": True,
            "scope": "manual legacy MiniQMT interface connectivity only",
            "canonical_endpoint": "/api/v1/simulation-runtime/miniqmt/quote-diagnostics",
        },
    }


def _quote_evidence(event: Any) -> dict[str, Any] | None:
    payload = getattr(event, "payload", None)
    evidence = payload.get("evidence") if isinstance(payload, dict) else None
    return dict(evidence) if isinstance(evidence, dict) else None


def _quote_event_symbol(event: Any) -> str | None:
    evidence = _quote_evidence(event)
    return str(evidence.get("symbol")) if evidence and evidence.get("symbol") else None


def _quote_event_summary(event: Any) -> dict[str, Any]:
    evidence = _quote_evidence(event)
    return {
        "event_id": event.event_id,
        "runtime_id": event.runtime_id,
        "sequence": event.sequence,
        "event_type": event.event_type.value,
        "event_time": event.event_time.isoformat(),
        "source": event.source,
        "evidence_id": evidence.get("evidence_id") if evidence else None,
        "market_data_id": evidence.get("market_data_id") if evidence else None,
        "symbol": evidence.get("symbol") if evidence else None,
        "capture_type": evidence.get("capture_type") if evidence else None,
        "reason_code": (evidence.get("quality_reason_code") or evidence.get("unavailable_reason"))
        if evidence
        else None,
        "stage": evidence.get("stage") if evidence else None,
    }


def _page_runtime_events(events: list[Any], *, cursor: str | None, limit: int) -> tuple[list[Any], str | None]:
    if limit < 1 or limit > 500:
        raise DataUnavailableError(
            "quote diagnostics limit must be between 1 and 500",
            context={"reason_code": "MINIQMT_QUOTE_DIAGNOSTICS_LIMIT_INVALID", "limit": limit},
        )
    ordered = sorted(events, key=lambda item: (int(item.sequence), item.event_id))
    if cursor:
        sequence_raw, separator, event_id = cursor.partition(":")
        if not separator or not sequence_raw.isdigit() or not event_id:
            raise DataUnavailableError(
                "quote diagnostics cursor is invalid",
                context={"reason_code": "MINIQMT_QUOTE_DIAGNOSTICS_CURSOR_INVALID"},
            )
        boundary = (int(sequence_raw), event_id)
        ordered = [item for item in ordered if (int(item.sequence), item.event_id) > boundary]
    page = ordered[:limit]
    next_cursor = f"{page[-1].sequence}:{page[-1].event_id}" if len(ordered) > len(page) and page else None
    return page, next_cursor


def _decode_quote_cursor(cursor: str | None, *, limit: int) -> tuple[int, str]:
    if limit < 1 or limit > 500:
        raise DataUnavailableError(
            "quote diagnostics limit must be between 1 and 500",
            context={"reason_code": "MINIQMT_QUOTE_DIAGNOSTICS_LIMIT_INVALID", "limit": limit},
        )
    if cursor is None:
        return 0, ""
    sequence_raw, separator, event_id = cursor.partition(":")
    if not separator or not sequence_raw.isdigit() or not event_id:
        raise DataUnavailableError(
            "quote diagnostics cursor is invalid",
            context={"reason_code": "MINIQMT_QUOTE_DIAGNOSTICS_CURSOR_INVALID"},
        )
    return int(sequence_raw), event_id


def _bounded_page(items: list[Any], *, limit: int) -> tuple[list[Any], str | None]:
    page = items[:limit]
    next_cursor = f"{page[-1].sequence}:{page[-1].event_id}" if len(items) > limit and page else None
    return page, next_cursor


def _missing_required_evidence_links(
    evidence: dict[str, Any],
    *,
    known_evidence_ids: set[str],
    known_event_ids: set[str],
) -> list[str]:
    capture_type = str(evidence.get("capture_type") or "")
    missing: list[str] = []

    def require_value(field_name: str) -> None:
        if not evidence.get(field_name):
            missing.append(field_name)

    def require_evidence(field_name: str) -> None:
        value = evidence.get(field_name)
        if not value:
            missing.append(field_name)
        elif str(value) not in known_evidence_ids:
            missing.append(f"{field_name}:unresolved")

    def require_event(field_name: str) -> None:
        value = evidence.get(field_name)
        if not value:
            missing.append(field_name)
        elif str(value) not in known_event_ids:
            missing.append(f"{field_name}:unresolved")

    for field_name in ("evidence_id", "policy_sha256", "mark_policy_version"):
        require_value(field_name)
    if capture_type == "ACTION_INPUT":
        for field_name in ("evaluation_id", "action_id", "parent_intent_id", "market_data_id", "clock_event_id"):
            require_value(field_name)
    elif capture_type == "ACTION_REJECT":
        for field_name in ("evaluation_id", "parent_intent_id", "clock_event_id", "quality_reason_code", "stage"):
            require_value(field_name)
    elif capture_type in {"CHILD_RECEIPT", "PROTECTION_BAND_TRIGGER"}:
        for field_name in ("child_order_id", "action_id", "anchor_market_data_id"):
            require_value(field_name)
        require_evidence("action_evidence_id")
        require_event("source_child_event_id")
        if capture_type == "CHILD_RECEIPT":
            require_value("broker_order_id")
    elif capture_type.startswith("MARKOUT_"):
        for field_name in (
            "child_order_id",
            "trade_id",
            "anchor_market_data_id",
            "mark_series_key",
            "horizon_seconds",
            "target_time_utc",
            "mark_status",
        ):
            require_value(field_name)
        require_evidence("action_evidence_id")
        require_event("anchor_trade_event_id")
    elif capture_type == "CADENCE_AGGREGATE":
        for field_name in ("cadence_window_start_utc", "cadence_counts", "source_session_id", "ingress_generation"):
            if evidence.get(field_name) is None:
                missing.append(field_name)
    else:
        missing.append("capture_type:unsupported")
    return sorted(set(missing))


HUMAN_RUN_STATUS_LABELS = {
    SimulationDailyRunStatus.PLANNING_EXECUTION: "\u6267\u884c\u8ba1\u5212\u5df2\u751f\u6210",
    SimulationDailyRunStatus.INTRADAY_RUNNING: "\u76d8\u4e2d\u8fd0\u884c\u4e2d",
    SimulationDailyRunStatus.SUCCEEDED: "\u5df2\u5b8c\u6210 / \u65e0\u5f85\u5904\u7406\u9519\u8bef",
    SimulationDailyRunStatus.FAILED_RETRYABLE: "\u5f53\u65e5\u5931\u8d25 / \u53ef\u91cd\u8bd5",
    SimulationDailyRunStatus.FAILED_TERMINAL: "\u7ec8\u6b62\u5931\u8d25",
    SimulationDailyRunStatus.CANCELLED: "\u5df2\u53d6\u6d88",
}


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
        default_submit = _scheduler_bool(status, "default_submit")
        autostart = _scheduler_bool(status, "autostart")
        running = _scheduler_bool(status, "running")
        thread_alive = _scheduler_bool(status, "thread_alive")
        scheduler_control_api_enabled = _scheduler_bool(status, "scheduler_control_api_enabled")
        manual_tick_endpoint_enabled = _scheduler_bool(status, "manual_tick_endpoint_enabled")
        interval_seconds = _scheduler_positive_int(status, "interval_seconds")
        scheduler_loop_health = self._scheduler_loop_health(status)
        last_result = status.get("last_result") if isinstance(status.get("last_result"), dict) else None
        last_blocking_result = (
            status.get("last_blocking_result") if isinstance(status.get("last_blocking_result"), dict) else None
        )
        last_result_errors = (
            list(last_result.get("errors") or [])
            if isinstance(last_result, dict) and isinstance(last_result.get("errors"), list)
            else []
        )
        current_trade_date_blockers = self._current_trade_date_blockers(
            status=status,
            last_result=last_result,
            last_blocking_result=last_blocking_result,
            scheduler_loop_health=scheduler_loop_health,
        )
        return {
            "ok": True,
            "scheduler": status.get("scheduler") or "simulation_lifecycle_scheduler",
            "autostart": autostart,
            "running": running,
            "thread_alive": thread_alive,
            "interval_seconds": interval_seconds,
            "last_run_at": status.get("last_run_at"),
            "last_result": last_result,
            "last_blocking_result": last_blocking_result,
            "last_result_errors": last_result_errors,
            "last_error_count": len(last_result_errors),
            "scheduler_loop_health": scheduler_loop_health,
            "current_trade_date_blockers": current_trade_date_blockers,
            "effective_runtime_health": self._effective_runtime_health(
                status=status,
                current_trade_date_blockers=current_trade_date_blockers,
                scheduler_loop_health=scheduler_loop_health,
            ),
            "default_submit": default_submit,
            "approval_states": list(status.get("approval_states") or []),
            "sim_binding_selection_policy": status.get("sim_binding_selection_policy"),
            "schedule_windows": list(status.get("schedule_windows") or []),
            "restart_recovery_mode": status.get("restart_recovery_mode") or "persisted_state_only",
            "window_orchestration": status.get("window_orchestration") or {},
            "read_only_status_api": True,
            "read_only_ops_api": False,
            "controlled_ops_api": True,
            "scheduler_control_api_enabled": scheduler_control_api_enabled,
            "manual_tick_endpoint_enabled": manual_tick_endpoint_enabled,
            "context_provider": status.get("context_provider") or {},
            "context_provider_mode": status.get("context_provider_mode"),
            "data_source": status.get("data_source"),
            "data_source_policy": status.get("data_source_policy") or {},
            "selection_inference": _required_scheduler_status_mapping(
                status,
                "selection_inference",
            ),
            "binding_watchdog": _required_scheduler_status_mapping(
                status,
                "binding_watchdog",
            ),
            "miniqmt_sim_runtime": _required_scheduler_status_mapping(
                status,
                "miniqmt_sim_runtime",
            ),
            "miniqmt_quote_context": _required_scheduler_status_mapping(
                status,
                "miniqmt_quote_context",
            ),
            "miniqmt_quote_ingress_activation": _required_scheduler_status_mapping(
                status,
                "miniqmt_quote_ingress_activation",
            ),
            "b0_quote_v2_controllers": _required_scheduler_status_mapping(
                status,
                "b0_quote_v2_controllers",
            ),
            "miniqmt_quote_health_authority": {
                "authority": "simulation_scheduler_live_components",
                "authoritative": False,
                "scope": "live in-process component telemetry",
                "canonical_endpoint": "/api/v1/simulation-runtime/miniqmt/quote-diagnostics",
            },
            "account_slot_persistence": {
                "enabled": True,
                "release_binding_columns": ["account_group_id", "strategy_slot_id"],
                "daily_run_columns": ["account_group_id", "strategy_slot_id"],
                "status_api_exposes_slots": True,
                "miniqmt_unified_binding_mode": "account_group_slots",
            },
            "summary": {
                "label": "simulation lifecycle scheduler",
                "next_action": "monitor scheduler windows, or use the controlled start/stop/tick APIs",
                "safety_note": (
                    "Status is read-only. start/stop/tick are controlled operations; "
                    f"default_submit is {'enabled' if default_submit else 'disabled'}."
                ),
            },
        }

    def platform_diagnostics(
        self,
        *,
        trade_date: date | None = None,
        binding_id: str | None = None,
        run_id: str | None = None,
        runtime_id: str | None = None,
        plan_id: str | None = None,
        limit: int = 100,
        runtime_repository: MiniQMTExecutionRuntimeRepository | None = None,
        generated_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Read current durable facts without starting feeds or mutating execution state."""

        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
            raise RuntimeConfigInvalidError(
                "platform diagnostics limit must be between 1 and 100",
                context={
                    "reason_code": "SIMULATION_PLATFORM_DIAGNOSTIC_QUERY_INVALID",
                    "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_QUERY",
                    "field": "limit",
                    "value": limit,
                },
            )

        def normalized(value: str | None, field: str) -> str | None:
            if value is None:
                return None
            if not isinstance(value, str) or not value.strip():
                raise RuntimeConfigInvalidError(
                    "platform diagnostics identity filters must be non-empty text",
                    context={
                        "reason_code": "SIMULATION_PLATFORM_DIAGNOSTIC_QUERY_INVALID",
                        "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_QUERY",
                        "field": field,
                    },
                )
            return value.strip()

        binding_id = normalized(binding_id, "binding_id")
        run_id = normalized(run_id, "run_id")
        runtime_id = normalized(runtime_id, "runtime_id")
        plan_id = normalized(plan_id, "plan_id")
        scheduler_status = self.scheduler_status()
        scan_trade_date = trade_date
        exact_identity_filter = any((binding_id, run_id, runtime_id, plan_id))
        runtime_only_query = runtime_id is not None and not any((binding_id, run_id, plan_id))

        if run_id is not None:
            runs = [self.repository.get_simulation_daily_run(run_id)]
            scan_trade_date = scan_trade_date or runs[0].trade_date
            scan_count = 1
        else:
            if scan_trade_date is None and (not exact_identity_filter or runtime_only_query):
                blocker_projection = scheduler_status["current_trade_date_blockers"]
                scan_trade_date = date.fromisoformat(str(blocker_projection["trade_date"]))
            scan_limit = 501
            try:
                scanned_runs = self.repository.list_simulation_daily_runs(
                    trade_date=scan_trade_date,
                    limit=scan_limit,
                )
            except Exception as exc:  # noqa: BLE001 - diagnostics cannot downgrade repository failures.
                raise DataUnavailableError(
                    "failed to read simulation runs for platform diagnostics",
                    context={
                        "reason_code": "SIMULATION_PLATFORM_DIAGNOSTIC_READBACK_FAILED",
                        "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_QUERY",
                        "trade_date": scan_trade_date.isoformat() if scan_trade_date else None,
                    },
                ) from exc
            scan_count = len(scanned_runs)
            if scan_count >= scan_limit:
                raise DataUnavailableError(
                    "platform diagnostics scan exceeded its bounded exact-query contract",
                    context={
                        "reason_code": "SIMULATION_PLATFORM_DIAGNOSTIC_SCAN_TRUNCATED",
                        "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_QUERY",
                        "bounded_limit": scan_limit - 1,
                        "trade_date": scan_trade_date.isoformat() if scan_trade_date else None,
                        "next_action": "supply trade_date or run_id to narrow the read-only query",
                    },
                )
            runs = list(scanned_runs)

        if binding_id is not None:
            runs = [run for run in runs if run.binding_id == binding_id]
        if plan_id is not None:
            self.repository.get_execution_plan(plan_id)
            runs = [run for run in runs if run.execution_plan_id == plan_id]
        if runtime_id is not None:
            runs = [run for run in runs if SimulationPlatformObservability.run_runtime_id(run) == runtime_id]
        if trade_date is not None:
            runs = [run for run in runs if run.trade_date == trade_date]
        if exact_identity_filter and not runs and not runtime_only_query:
            raise DataUnavailableError(
                "no simulation run matches the exact platform diagnostics query",
                context={
                    "reason_code": "SIMULATION_PLATFORM_DIAGNOSTIC_RUN_NOT_FOUND",
                    "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_QUERY",
                    "trade_date": trade_date.isoformat() if trade_date else None,
                    "binding_id": binding_id,
                    "run_id": run_id,
                    "runtime_id": runtime_id,
                    "plan_id": plan_id,
                },
            )
        runs.sort(key=lambda item: (item.updated_at, item.created_at, item.run_id), reverse=True)
        observed_match_count = len(runs)
        selected_runs = runs[:limit]
        quote_diagnostics = None
        if runtime_id is not None:
            if runtime_repository is None:
                raise RuntimeConfigInvalidError(
                    "runtime_repository is required for an exact MiniQMT runtime query",
                    context={
                        "reason_code": "SIMULATION_PLATFORM_RUNTIME_REPOSITORY_REQUIRED",
                        "stage": "SIMULATION_PLATFORM_DIAGNOSTICS_QUERY",
                        "runtime_id": runtime_id,
                    },
                )
            quote_diagnostics = self.list_miniqmt_quote_diagnostics(
                runtime_repository=runtime_repository,
                runtime_id=runtime_id,
                limit=min(limit, 100),
                scheduler_status_snapshot=scheduler_status,
            )
        effective_trade_date = trade_date or scan_trade_date or (selected_runs[0].trade_date if selected_runs else None)
        query = {
            "schema_version": "simulation_platform_diagnostic_query_v1",
            "trade_date": effective_trade_date.isoformat() if effective_trade_date else None,
            "binding_id": binding_id,
            "run_id": run_id,
            "runtime_id": runtime_id,
            "plan_id": plan_id,
            "limit": limit,
            "scan_count": scan_count,
            "observed_match_count": observed_match_count,
            "returned_count": len(selected_runs),
            "truncated": observed_match_count > limit,
            "read_only": True,
        }
        return SimulationPlatformObservability().build(
            scheduler_status=scheduler_status,
            runs=selected_runs,
            query=query,
            quote_diagnostics=quote_diagnostics,
            generated_at=generated_at,
        )

    @staticmethod
    def _effective_runtime_health(
        *,
        status: dict[str, Any],
        current_trade_date_blockers: dict[str, Any],
        scheduler_loop_health: dict[str, Any],
    ) -> str:
        if scheduler_loop_health["status"] == "BLOCKED":
            return "BLOCKED"
        if current_trade_date_blockers["blocker_count"] > 0:
            return "BLOCKED"
        if not _scheduler_bool(status, "running") or not _scheduler_bool(status, "thread_alive"):
            return "SCHEDULER_INACTIVE"
        return "NO_CURRENT_DAY_BLOCKER"

    def _current_trade_date_blockers(
        self,
        *,
        status: dict[str, Any],
        last_result: dict[str, Any] | None,
        last_blocking_result: dict[str, Any] | None,
        scheduler_loop_health: dict[str, Any],
    ) -> dict[str, Any]:
        observed_trade_dates: list[str] = []
        for candidate in (last_result, last_blocking_result):
            if isinstance(candidate, dict) and candidate.get("trade_date"):
                raw_trade_date = str(candidate["trade_date"])
                try:
                    date.fromisoformat(raw_trade_date)
                except ValueError as exc:
                    raise RuntimeConfigInvalidError(
                        "scheduler status has an invalid trade_date",
                        context={
                            "reason_code": "SIMULATION_SCHEDULER_STATUS_TRADE_DATE_INVALID",
                            "stage": "SCHEDULER_STATUS_PROJECTION",
                            "trade_date": raw_trade_date,
                        },
                    ) from exc
                observed_trade_dates.append(raw_trade_date)
        trade_date = SimulationLifecycleBackgroundScheduler._trade_date(SimulationLifecycleScheduler._scheduler_now())
        blocking_statuses = (
            SimulationDailyRunStatus.FAILED_RETRYABLE,
            SimulationDailyRunStatus.FAILED_TERMINAL,
        )
        try:
            runs_by_status = {
                blocking_status: self.repository.list_simulation_daily_runs(
                    trade_date=trade_date,
                    status=blocking_status,
                    limit=100,
                )
                for blocking_status in blocking_statuses
            }
        except Exception as exc:  # noqa: BLE001 - diagnostics must fail loudly, never return false green.
            raise DataUnavailableError(
                "failed to read current-trade-date simulation blockers",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_BLOCKER_READBACK_FAILED",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "trade_date": trade_date.isoformat(),
                    "repository": type(self.repository).__name__,
                },
            ) from exc
        all_blockers = sorted(
            (run for runs in runs_by_status.values() for run in runs),
            key=lambda run: (run.updated_at, run.created_at, run.run_id),
            reverse=True,
        )
        active_loop_failure = scheduler_loop_health.get("active_failure")
        loop_blockers: list[dict[str, Any]] = []
        if scheduler_loop_health["status"] == "BLOCKED":
            loop_blockers.append(
                {
                    "component": "simulation_background_scheduler_run_loop",
                    "status": "BLOCKED",
                    "reason_code": scheduler_loop_health["reason_code"],
                    "stage": active_loop_failure.get("stage"),
                    "exception_type": active_loop_failure.get("exception_type"),
                    "exception_message": active_loop_failure.get("exception_message"),
                    "underlying_reason_code": active_loop_failure.get("underlying_reason_code"),
                    "underlying_stage": active_loop_failure.get("underlying_stage"),
                    "failure_trade_date": active_loop_failure.get("trade_date"),
                    "first_failure_at": active_loop_failure.get("first_failure_at"),
                    "failure_at": active_loop_failure.get("failure_at"),
                    "consecutive_failure_count": scheduler_loop_health["consecutive_failure_count"],
                    "execution_gate": False,
                }
            )
        database_limit = 100 - len(loop_blockers)
        bounded_blockers = all_blockers[:database_limit]
        database_blockers = [
            {
                "run_id": run.run_id,
                "strategy_id": run.strategy_id,
                "binding_id": run.binding_id,
                "broker_backend": run.broker_backend.value,
                "status": run.status.value,
                "last_stage": run.run_payload_json.get("last_stage"),
                "reason_code": self._run_blocker_reason_code(run),
                "execution_plan_id": run.execution_plan_id,
            }
            for run in bounded_blockers
        ]
        blockers = [*loop_blockers, *database_blockers]
        observed_blocker_count = len(loop_blockers) + len(all_blockers)
        return {
            "schema_version": "simulation_scheduler_current_day_blockers_v1",
            "trade_date": trade_date.isoformat(),
            "status": "BLOCKED" if blockers else "CLEAR",
            "blocker_count": len(blockers),
            "blockers": blockers,
            "observed_blocker_count": observed_blocker_count,
            "bounded_limit": 100,
            "truncated": observed_blocker_count > 100 or any(len(runs) >= 100 for runs in runs_by_status.values()),
            "execution_gate": False,
            "source": (
                "scheduler_loop_health+simulation_daily_run_readback"
                if loop_blockers
                else "simulation_daily_run_readback"
            ),
            "scheduler_running": _scheduler_bool(status, "running"),
            "last_observed_trade_dates": list(dict.fromkeys(observed_trade_dates)),
        }

    @staticmethod
    def _scheduler_loop_health(status: dict[str, Any]) -> dict[str, Any]:
        value = status.get("scheduler_loop_health")
        if value is None:
            if _scheduler_bool(status, "scheduler_control_api_enabled"):
                raise RuntimeConfigInvalidError(
                    "background scheduler status is missing scheduler_loop_health",
                    context={
                        "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_MISSING",
                        "stage": "SCHEDULER_STATUS_PROJECTION",
                    },
                )
            return {
                "schema_version": "simulation_background_scheduler_loop_health_v1",
                "status": "NOT_APPLICABLE",
                "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_LOOP_NOT_APPLICABLE",
                "active_failure": None,
                "last_failure": None,
                "last_successful_tick_at": None,
                "consecutive_failure_count": 0,
                "total_failure_count": 0,
                "total_success_count": 0,
                "execution_gate": False,
                "auto_clears_on_success": True,
            }
        if not isinstance(value, dict):
            raise RuntimeConfigInvalidError(
                "scheduler status scheduler_loop_health must be a mapping",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "scheduler_loop_health",
                },
            )
        projected = dict(value)
        if projected.get("schema_version") != "simulation_background_scheduler_loop_health_v1":
            raise RuntimeConfigInvalidError(
                "scheduler_loop_health schema_version is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "schema_version",
                    "value": projected.get("schema_version"),
                },
            )
        health_status = projected.get("status")
        if health_status not in {"BLOCKED", "HEALTHY", "NOT_YET_RUN"}:
            raise RuntimeConfigInvalidError(
                "scheduler_loop_health status is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "status",
                    "value": health_status,
                },
            )
        expected_reason_code = {
            "BLOCKED": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
            "HEALTHY": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_OK",
            "NOT_YET_RUN": "SIMULATION_BACKGROUND_SCHEDULER_LOOP_NOT_YET_RUN",
        }[health_status]
        if projected.get("reason_code") != expected_reason_code:
            raise RuntimeConfigInvalidError(
                "scheduler_loop_health reason_code does not match status",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "reason_code",
                    "status": health_status,
                    "value": projected.get("reason_code"),
                },
            )
        if projected.get("execution_gate") is not False:
            raise RuntimeConfigInvalidError(
                "scheduler_loop_health execution_gate must be false",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "execution_gate",
                    "value": projected.get("execution_gate"),
                },
            )
        if projected.get("auto_clears_on_success") is not True:
            raise RuntimeConfigInvalidError(
                "scheduler_loop_health auto_clears_on_success must be true",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "auto_clears_on_success",
                    "value": projected.get("auto_clears_on_success"),
                },
            )
        active_failure = projected.get("active_failure")
        if health_status == "BLOCKED" and not isinstance(active_failure, dict):
            raise RuntimeConfigInvalidError(
                "blocked scheduler_loop_health requires active_failure",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "active_failure",
                },
            )
        if health_status != "BLOCKED" and active_failure is not None:
            raise RuntimeConfigInvalidError(
                "non-blocked scheduler_loop_health cannot have active_failure",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "active_failure",
                    "status": health_status,
                },
            )
        if isinstance(active_failure, dict):
            expected_failure_fields = {
                "schema_version": "simulation_background_scheduler_loop_failure_v1",
                "status": "BLOCKED",
                "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
                "stage": "BACKGROUND_SCHEDULER_RUN_LOOP",
                "execution_gate": False,
                "auto_clears_on_success": True,
            }
            for field, expected_value in expected_failure_fields.items():
                if active_failure.get(field) != expected_value:
                    raise RuntimeConfigInvalidError(
                        f"scheduler_loop_health active_failure {field} is invalid",
                        context={
                            "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                            "stage": "SCHEDULER_STATUS_PROJECTION",
                            "field": f"active_failure.{field}",
                            "value": active_failure.get(field),
                        },
                    )
            exception_message = active_failure.get("exception_message")
            if not isinstance(exception_message, str) or len(exception_message) > 2048:
                raise RuntimeConfigInvalidError(
                    "scheduler_loop_health active_failure exception_message is invalid",
                    context={
                        "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                        "stage": "SCHEDULER_STATUS_PROJECTION",
                        "field": "active_failure.exception_message",
                    },
                )
            failure_context = active_failure.get("context")
            if not isinstance(failure_context, dict) or any(
                len(str(key)) > 64 or len(str(value)) > 512 for key, value in failure_context.items()
            ):
                raise RuntimeConfigInvalidError(
                    "scheduler_loop_health active_failure context is invalid",
                    context={
                        "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                        "stage": "SCHEDULER_STATUS_PROJECTION",
                        "field": "active_failure.context",
                    },
                )
        for field in (
            "consecutive_failure_count",
            "total_failure_count",
            "total_success_count",
        ):
            count = projected.get(field)
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise RuntimeConfigInvalidError(
                    f"scheduler_loop_health {field} must be a non-negative integer",
                    context={
                        "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                        "stage": "SCHEDULER_STATUS_PROJECTION",
                        "field": field,
                        "value": count,
                    },
                )
        consecutive_failure_count = projected["consecutive_failure_count"]
        if health_status == "BLOCKED" and consecutive_failure_count <= 0:
            raise RuntimeConfigInvalidError(
                "blocked scheduler_loop_health requires a positive consecutive failure count",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "consecutive_failure_count",
                    "value": consecutive_failure_count,
                },
            )
        if health_status != "BLOCKED" and consecutive_failure_count != 0:
            raise RuntimeConfigInvalidError(
                "non-blocked scheduler_loop_health requires zero consecutive failures",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_LOOP_HEALTH_INVALID",
                    "stage": "SCHEDULER_STATUS_PROJECTION",
                    "field": "consecutive_failure_count",
                    "value": consecutive_failure_count,
                },
            )
        return projected

    @staticmethod
    def _run_blocker_reason_code(run: SimulationDailyRun) -> str:
        payload = run.run_payload_json
        candidates = (
            payload.get("pre_run_failure"),
            payload.get("submit_failure"),
            payload.get("local_sim_retry_diagnostics"),
            payload.get("miniqmt_event_loop_tick_driver_timeout"),
        )
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if candidate.get("reason_code"):
                return str(candidate["reason_code"])
            context = candidate.get("context")
            if isinstance(context, dict) and context.get("reason_code"):
                return str(context["reason_code"])
        return run.status.value

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
            payload["execution_plan"] = self._plan_summary(self.repository.get_execution_plan(run.execution_plan_id))
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

    def list_miniqmt_runtime_events(
        self,
        *,
        runtime_repository: MiniQMTExecutionRuntimeRepository,
        runtime_id: str,
    ) -> dict[str, Any]:
        runtime = runtime_repository.get_runtime(runtime_id)
        if runtime is None:
            raise DataUnavailableError(
                "MiniQMT execution runtime does not exist",
                context={"reason_code": "MINIQMT_RUNTIME_NOT_FOUND", "runtime_id": runtime_id},
            )
        events = [
            {
                "event_id": event.event_id,
                "runtime_id": event.runtime_id,
                "sequence": event.sequence,
                "event_type": event.event_type.value,
                "event_time": event.event_time.isoformat(),
                "source": event.source,
                "payload": event.payload,
            }
            for event in runtime_repository.list_events(runtime_id)
        ]
        return {
            "schema_version": "miniqmt_runtime_events_readonly_v1",
            "runtime": runtime.model_dump(mode="json"),
            "count": len(events),
            "events": events,
            "read_only": True,
        }

    def list_miniqmt_quote_diagnostics(
        self,
        *,
        runtime_repository: MiniQMTExecutionRuntimeRepository,
        runtime_id: str,
        symbol: str | None = None,
        cursor: str | None = None,
        limit: int = 100,
        scheduler_status_snapshot: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Project durable journal facts only; never construct quote runtime objects."""

        runtime = runtime_repository.get_runtime(runtime_id)
        if runtime is None:
            raise DataUnavailableError(
                "MiniQMT execution runtime does not exist",
                context={"reason_code": "MINIQMT_RUNTIME_NOT_FOUND", "runtime_id": runtime_id},
            )
        after_sequence, after_event_id = _decode_quote_cursor(cursor, limit=limit)
        page, next_cursor = _bounded_page(
            runtime_repository.list_quote_events_page(
                runtime_id,
                symbol=symbol,
                after_sequence=after_sequence,
                after_event_id=after_event_id,
                limit=limit + 1,
            ),
            limit=limit,
        )
        summary = runtime_repository.quote_diagnostics_summary(runtime_id, symbol=symbol)
        scheduler_status = dict(scheduler_status_snapshot or self.scheduler.status())
        markout = dict(summary["markout"])
        markout_due = int(markout["terminal_due_count"])
        markout_captured = int(markout["captured_count"])
        per_symbol = [
            {
                **item,
                "capture_count": int(item["capture_count"]),
                "last_event_time": item["last_event_time"].isoformat(),
            }
            for item in summary["per_symbol"]
            if item.get("symbol")
        ]
        last_reason = summary["last_reason"]
        if last_reason is not None:
            last_reason = {
                **last_reason,
                "event_time": last_reason["event_time"].isoformat(),
            }
        return {
            "schema_version": "miniqmt_quote_diagnostics_v1",
            "runtime_id": runtime_id,
            "read_only": True,
            "production_ddl_gate": runtime_repository.quote_event_schema_gate(),
            "health": _canonical_miniqmt_quote_health(
                runtime=runtime,
                runtime_id=runtime_id,
                durable_health=summary["health"],
                durable_health_event=summary.get("health_event"),
                scheduler_status=scheduler_status,
            ),
            "per_symbol": sorted(per_symbol, key=lambda item: item["symbol"]),
            "recent_reason": last_reason,
            "reason_counts": dict(sorted(summary["reason_counts"].items())),
            "markout": {
                "terminal_due_count": markout_due,
                "captured_count": markout_captured,
                "coverage_ratio": (markout_captured / markout_due) if markout_due else None,
            },
            "events": [_quote_event_summary(event) for event in page],
            "next_cursor": next_cursor,
            "limit": limit,
        }

    def list_miniqmt_quote_evidence(
        self,
        *,
        runtime_repository: MiniQMTExecutionRuntimeRepository,
        runtime_id: str,
        market_data_id: str | None = None,
        evidence_id: str | None = None,
        include_archived: bool = False,
        cursor: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """Read evidence envelopes and reconstruct links without repairing them."""

        if (market_data_id is None) == (evidence_id is None):
            raise DataUnavailableError(
                "exactly one of market_data_id or evidence_id is required",
                context={"reason_code": "MINIQMT_QUOTE_EVIDENCE_QUERY_INVALID"},
            )
        if runtime_repository.get_runtime(runtime_id) is None:
            raise DataUnavailableError(
                "MiniQMT execution runtime does not exist",
                context={"reason_code": "MINIQMT_RUNTIME_NOT_FOUND", "runtime_id": runtime_id},
            )
        after_sequence, after_event_id = _decode_quote_cursor(cursor, limit=limit)
        receipts = runtime_repository.list_evidence_receipts(
            runtime_id,
            market_data_id=market_data_id,
            evidence_id=evidence_id,
            include_archived=include_archived,
            after_sequence=after_sequence,
            after_event_id=after_event_id,
            limit=limit + 1,
        )
        receipt_page, next_cursor = _bounded_page([receipt.event for receipt in receipts], limit=limit)
        page_ids = {event.event_id for event in receipt_page}
        receipt_by_event_id = {receipt.event.event_id: receipt for receipt in receipts}
        page_evidence = [_quote_evidence(event) for event in receipt_page]
        linked_event_ids = tuple(
            sorted(
                {
                    str(value)
                    for evidence in page_evidence
                    if evidence is not None
                    for value in (evidence.get("source_child_event_id"), evidence.get("anchor_trade_event_id"))
                    if value
                }
            )
        )
        known_event_ids = page_ids | {
            event.event_id
            for event in runtime_repository.list_events_by_ids(
                runtime_id,
                event_ids=linked_event_ids,
                include_archived=include_archived,
            )
        }
        known_evidence_ids = {
            str(evidence.get("evidence_id"))
            for receipt in receipts
            for event in (receipt.event,)
            if (evidence := _quote_evidence(event)) is not None and evidence.get("evidence_id")
        }
        referenced_evidence_ids = tuple(
            sorted(
                {
                    str(value)
                    for evidence in page_evidence
                    if evidence is not None
                    for value in (
                        evidence.get("action_evidence_id"),
                        evidence.get("child_receipt_evidence_id"),
                        evidence.get("supersedes_evidence_id"),
                    )
                    if value
                }
            )
        )
        known_evidence_ids.update(
            runtime_repository.existing_evidence_ids(
                runtime_id,
                evidence_ids=referenced_evidence_ids,
                include_archived=include_archived,
            )
        )
        records: list[dict[str, Any]] = []
        for event in receipt_page:
            receipt = receipt_by_event_id[event.event_id]
            evidence = _quote_evidence(event)
            if evidence is None:
                raise DataUnavailableError(
                    "quote evidence readback returned an invalid event envelope",
                    context={"reason_code": "MINIQMT_QUOTE_EVIDENCE_READBACK_INVALID", "event_id": event.event_id},
                )
            links = {
                "action_evidence_id": evidence.get("action_evidence_id"),
                "child_receipt_evidence_id": evidence.get("child_receipt_evidence_id"),
                "source_child_event_id": evidence.get("source_child_event_id"),
                "anchor_trade_event_id": evidence.get("anchor_trade_event_id"),
                "anchor_market_data_id": evidence.get("anchor_market_data_id"),
                "trade_id": evidence.get("trade_id"),
                "policy_sha256": evidence.get("policy_sha256"),
                "mark_policy_version": evidence.get("mark_policy_version"),
            }
            missing_links = _missing_required_evidence_links(
                evidence,
                known_evidence_ids=known_evidence_ids,
                known_event_ids=known_event_ids,
            )
            records.append(
                {
                    "event": _quote_event_summary(event),
                    "durable_receipt": {
                        "persisted_at_utc": receipt.persisted_at_utc.isoformat(),
                        "durable_ack": receipt.durable_ack,
                        "readback_verified": receipt.readback_verified,
                    },
                    "evidence": evidence,
                    "links": links,
                    "link_complete": not missing_links,
                    "missing_links": missing_links,
                }
            )
        return {
            "schema_version": "miniqmt_quote_evidence_readback_v1",
            "runtime_id": runtime_id,
            "query": {
                "market_data_id": market_data_id,
                "evidence_id": evidence_id,
                "include_archived": include_archived,
            },
            "read_only": True,
            "records": records,
            "next_cursor": next_cursor,
            "limit": limit,
        }

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
                "account_group_id": qmt_run.account_group_id,
                "strategy_slot_id": qmt_run.strategy_slot_id,
                "simulation_release_id": qmt_run.release_id,
                "simulation_release_hash": qmt_run.release_hash,
                "miniqmt_sim_run_id": qmt_run.run_id,
            },
        }

    def _run_list_summary(self, runs: list[SimulationDailyRun]) -> dict[str, Any]:
        by_status = Counter(run.status.value for run in runs)
        by_backend = Counter(run.broker_backend.value for run in runs)
        by_account_group = Counter(run.account_group_id for run in runs if run.account_group_id)
        by_strategy_slot = Counter(run.strategy_slot_id for run in runs if run.strategy_slot_id)
        active = sum(1 for run in runs if run.status not in TERMINAL_RUN_STATUSES)
        capacity_residual = [
            observability
            for observability in (
                SimulationLifecycleScheduler._miniqmt_capacity_residual_observability(run.run_payload_json)
                for run in runs
                if run.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            )
            if observability
        ]
        return {
            "run_count": len(runs),
            "active_run_count": active,
            "terminal_run_count": len(runs) - active,
            "by_status": dict(sorted(by_status.items())),
            "by_broker_backend": dict(sorted(by_backend.items())),
            "by_account_group": dict(sorted(by_account_group.items())),
            "by_strategy_slot": dict(sorted(by_strategy_slot.items())),
            "succeeded_with_capacity_residual_count": len(capacity_residual),
            "capacity_residual_count": sum(int(item.get("capacity_residual_count") or 0) for item in capacity_residual),
            "capacity_residual_failed_intents": sum(int(item.get("failed_intents") or 0) for item in capacity_residual),
        }

    @staticmethod
    def _readable_identifier(value: str | None) -> str:
        raw = str(value or "").strip()
        if not raw:
            return "-"
        ignored = {"strategy", "simrun", "srr", "simbind", "dse", "plan", "pkg", "ag", "slot"}
        words: list[str] = []
        for part in raw.replace("-", "_").split("_"):
            if not part or part.lower() in ignored:
                continue
            lower = part.lower()
            if lower == "local":
                words.append("Local")
            elif lower in {"miniqmt", "minqmt"}:
                words.append("MiniQMT")
            elif lower == "qmt":
                words.append("QMT")
            elif lower == "sim":
                words.append("SIM")
            elif lower == "ops":
                words.append("Ops")
            elif len(part) == 8 and part.isdigit() and part.startswith(("19", "20")):
                words.append(f"{part[:4]}-{part[4:6]}-{part[6:]}")
            else:
                words.append(part[:1].upper() + part[1:])
        return " ".join(words) or raw[:12]

    def _run_display(self, run: SimulationDailyRun, stage_counts: dict[str, int]) -> dict[str, Any]:
        target_count = int(stage_counts.get("target_count") or 0)
        intent_count = int(
            stage_counts.get("execution_plan_intent_count") or stage_counts.get("order_intent_count") or 0
        )
        submitted = int(stage_counts.get("submitted_intents") or 0)
        failed = int(stage_counts.get("failed_intents") or 0)
        broker_label = (
            "MiniQMT \u6a21\u62df\u76d8"
            if run.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            else "LocalSim \u672c\u5730\u6a21\u62df"
        )
        account_label = (
            self._readable_identifier(run.account_group_id)
            if run.account_group_id
            else "\u672c\u5730\u6a21\u62df\u8d26\u6237"
        )
        slot_label = (
            self._readable_identifier(run.strategy_slot_id)
            if run.strategy_slot_id
            else "\u9ed8\u8ba4\u7b56\u7565\u69fd"
        )
        return {
            "run_title": f"{run.trade_date.isoformat()} - {HUMAN_RUN_STATUS_LABELS.get(run.status, run.status.value)}",
            "status_label": HUMAN_RUN_STATUS_LABELS.get(run.status, run.status.value),
            "broker_label": broker_label,
            "strategy_label": self._readable_identifier(run.strategy_id),
            "package_label": f"\u7b56\u7565\u5305 {self._readable_identifier(run.package_id)}",
            "account_slot_label": f"{account_label} / {slot_label}",
            "selection_label": f"\u9009\u51fa {target_count} \u53ea\u5019\u9009",
            "execution_plan_label": f"\u4ea4\u6613\u610f\u56fe {intent_count} / \u5df2\u63d0\u4ea4 {submitted} / \u5931\u8d25 {failed}",
            "stage_label": f"\u9009\u80a1 {target_count} / \u610f\u56fe {intent_count} / \u5df2\u63d0\u4ea4 {submitted} / \u5931\u8d25 {failed}",
        }

    def _run_summary(self, run: SimulationDailyRun) -> dict[str, Any]:
        stage_counts = self._stage_counts(run.run_payload_json)
        broker_context = self._broker_context(run)
        reconciliation_context = self._reconciliation_context(run)
        capacity_residual_observability = None
        if run.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            capacity_residual_observability = SimulationLifecycleScheduler._miniqmt_capacity_residual_observability(
                run.run_payload_json
            )
        payload = {
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
            "account_group_id": run.account_group_id,
            "strategy_slot_id": run.strategy_slot_id,
            "slot_attribution": {
                "account_group_id": run.account_group_id,
                "strategy_slot_id": run.strategy_slot_id,
                "unified_path_active": bool(run.account_group_id and run.strategy_slot_id),
            },
            "selection_evidence_id": run.selection_evidence_id,
            "selection_artifact_hash": run.selection_artifact_hash,
            "execution_plan_id": run.execution_plan_id,
            "execution_plan_hash": run.execution_plan_hash,
            "status": run.status.value,
            "last_stage": str(run.run_payload_json.get("last_stage") or run.status.value),
            "stage_counts": stage_counts,
            "display": self._run_display(run, stage_counts),
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
        if capacity_residual_observability:
            payload.update(
                {
                    "succeeded_with_capacity_residual": True,
                    "capacity_residual_count": int(capacity_residual_observability.get("capacity_residual_count") or 0),
                    "capacity_residual_failed_intents": int(capacity_residual_observability.get("failed_intents") or 0),
                    "miniqmt_capacity_residual_observability": capacity_residual_observability,
                    "alerts": [capacity_residual_observability["alert"]],
                }
            )
        return payload

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
        if expected_backend == SimulationBrokerBackend.LOCAL_SIM:
            SimulationRuntimeOpsService._require_localsim_persisted_effects_for_live_evidence(run)

    @staticmethod
    def _require_localsim_persisted_effects_for_live_evidence(run: SimulationDailyRun) -> None:
        payload = run.run_payload_json
        broker_called = SimulationRuntimeOpsService._live_evidence_bool(
            run,
            payload,
            "broker_called",
        )
        submitted_intents = SimulationRuntimeOpsService._live_evidence_count(
            run,
            payload,
            "submitted_intents",
        )
        planned_intents = SimulationRuntimeOpsService._live_evidence_count(
            run,
            payload,
            "execution_plan_intent_count",
        )
        no_rebalance_required = SimulationRuntimeOpsService._live_evidence_bool(
            run,
            payload,
            "no_rebalance_required",
        )
        expected_order_count = max(submitted_intents, planned_intents)
        if not broker_called and submitted_intents <= 0 and (planned_intents <= 0 or no_rebalance_required):
            return
        persistence = payload.get("local_sim_persistence")
        if not isinstance(persistence, dict):
            raise DataUnavailableError(
                "live admission LocalSim evidence requires durable Paper v2 order/fill/cash/position persistence",
                context={
                    "run_id": run.run_id,
                    "broker_called": broker_called,
                    "submitted_intents": submitted_intents,
                    "execution_plan_intent_count": planned_intents,
                },
            )
        raw_status = persistence.get("status")
        if not isinstance(raw_status, str) or not raw_status.strip():
            raise RuntimeConfigInvalidError(
                "live admission LocalSim persistence status must be non-empty text",
                context={
                    "reason_code": "SIMULATION_LIVE_EVIDENCE_PERSISTENCE_STATUS_INVALID",
                    "stage": "SIMULATION_LIVE_EVIDENCE_PROJECTION",
                    "run_id": run.run_id,
                    "field": "local_sim_persistence.status",
                    "value_type": type(raw_status).__name__,
                },
            )
        status = raw_status.strip().upper()
        order_count = SimulationRuntimeOpsService._live_evidence_count(
            run,
            persistence,
            "order_count",
            required=True,
            field_prefix="local_sim_persistence",
        )
        fill_count = SimulationRuntimeOpsService._live_evidence_count(
            run,
            persistence,
            "fill_count",
            required=True,
            field_prefix="local_sim_persistence",
        )
        cash_count = SimulationRuntimeOpsService._live_evidence_count(
            run,
            persistence,
            "cash_ledger_count",
            required=True,
            field_prefix="local_sim_persistence",
        )
        if status != "PERSISTED" or order_count < expected_order_count or fill_count <= 0 or cash_count <= 0:
            raise DataUnavailableError(
                "live admission LocalSim evidence has incomplete durable execution effects",
                context={
                    "run_id": run.run_id,
                    "submitted_intents": submitted_intents,
                    "execution_plan_intent_count": planned_intents,
                    "local_sim_persistence": persistence,
                },
            )

    @staticmethod
    def _live_evidence_bool(
        run: SimulationDailyRun,
        payload: dict[str, Any],
        key: str,
    ) -> bool:
        if key not in payload or payload[key] is None:
            return False
        value = payload[key]
        if not isinstance(value, bool):
            raise RuntimeConfigInvalidError(
                "live admission simulation evidence boolean is invalid",
                context={
                    "reason_code": "SIMULATION_LIVE_EVIDENCE_BOOLEAN_INVALID",
                    "stage": "SIMULATION_LIVE_EVIDENCE_PROJECTION",
                    "run_id": run.run_id,
                    "field": key,
                    "value_type": type(value).__name__,
                },
            )
        return value

    @staticmethod
    def _live_evidence_count(
        run: SimulationDailyRun,
        payload: dict[str, Any],
        key: str,
        *,
        required: bool = False,
        field_prefix: str = "run_payload_json",
    ) -> int:
        if key not in payload or payload[key] is None:
            if not required:
                return 0
            raise RuntimeConfigInvalidError(
                "live admission simulation evidence count is missing",
                context={
                    "reason_code": "SIMULATION_LIVE_EVIDENCE_COUNT_MISSING",
                    "stage": "SIMULATION_LIVE_EVIDENCE_PROJECTION",
                    "run_id": run.run_id,
                    "field": f"{field_prefix}.{key}",
                },
            )
        value = payload[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeConfigInvalidError(
                "live admission simulation evidence count must be a non-negative integer",
                context={
                    "reason_code": "SIMULATION_LIVE_EVIDENCE_COUNT_INVALID",
                    "stage": "SIMULATION_LIVE_EVIDENCE_PROJECTION",
                    "run_id": run.run_id,
                    "field": f"{field_prefix}.{key}",
                    "value_type": type(value).__name__,
                    "value": value,
                },
            )
        return value

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
            "account_group_id": run.account_group_id,
            "strategy_slot_id": run.strategy_slot_id,
            "selection_evidence_id": run.selection_evidence_id,
            "selection_artifact_hash": run.selection_artifact_hash,
            "execution_plan_id": run.execution_plan_id,
            "execution_plan_hash": run.execution_plan_hash,
            "strategy_performance": run.run_payload_json.get("strategy_performance"),
            "local_sim_persistence": run.run_payload_json.get("local_sim_persistence"),
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
            "account_group_id": plan.account_group_id,
            "strategy_slot_id": plan.strategy_slot_id,
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
            "no_rebalance_required": _run_projection_bool(
                payload,
                "no_rebalance_required",
                field_prefix="run_payload_json",
                required=False,
            ),
            "broker_called": payload.get("broker_called"),
            "broker_order_handles": payload.get("broker_order_handles"),
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status"),
            "qmt_retry_of_batch_id": payload.get("qmt_retry_of_batch_id"),
            "qmt_batch_result": payload.get("qmt_batch_result"),
            "account_group_id": run.account_group_id or payload.get("account_group_id"),
            "strategy_slot_id": run.strategy_slot_id or payload.get("strategy_slot_id"),
            "sync_before_submit": payload.get("sync_before_submit"),
            "local_sim_cash_fit": payload.get("local_sim_cash_fit"),
            "local_sim_retry_diagnostics": payload.get("local_sim_retry_diagnostics"),
            "local_sim_persistence": payload.get("local_sim_persistence"),
            "submit_failure": payload.get("submit_failure"),
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
            persistence = broker_context.get("local_sim_persistence")
            persisted = isinstance(persistence, dict) and str(persistence.get("status") or "").upper() == "PERSISTED"
            return [
                {
                    "source": "local_sim_handle",
                    "handle_id": item.get("handle_id"),
                    "intent_id": item.get("intent_id"),
                    "backend_id": item.get("backend_id"),
                    "state": "persisted" if persisted else "submitted",
                    "submitted_at": item.get("submitted_at"),
                    "paper_v2_run_id": persistence.get("paper_v2_run_id") if isinstance(persistence, dict) else None,
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
                    "success": _run_projection_bool(
                        item,
                        "success",
                        field_prefix="qmt_batch_result.results[]",
                        required=True,
                    ),
                    "broker_called": _run_projection_bool(
                        item,
                        "broker_called",
                        field_prefix="qmt_batch_result.results[]",
                        required=True,
                    ),
                    "broker_message": item.get("broker_message"),
                    "preflight_allowed": preflight.get("allowed"),
                    "primary_error_code": preflight.get("primary_error_code"),
                    "primary_error_message": primary_error.get("message") if primary_error else None,
                }
            )
        return rows

    @staticmethod
    def _fills_projection(run: SimulationDailyRun, broker_context: dict[str, Any]) -> list[dict[str, Any]]:
        local_persistence = broker_context.get("local_sim_persistence")
        if isinstance(local_persistence, dict):
            return [
                {
                    "source": "local_sim_persistence_summary",
                    "status": local_persistence.get("status"),
                    "paper_v2_run_id": local_persistence.get("paper_v2_run_id"),
                    "fill_count": local_persistence.get("fill_count"),
                    "cash_ledger_count": local_persistence.get("cash_ledger_count"),
                    "position_count": local_persistence.get("position_count"),
                    "snapshot_time": local_persistence.get("snapshot_time"),
                }
            ]
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
        submit_failure = broker_context.get("submit_failure")
        if isinstance(submit_failure, dict):
            errors.append(
                {
                    "source": "local_sim_submit_failure"
                    if run.broker_backend == SimulationBrokerBackend.LOCAL_SIM
                    else "simulation_submit_failure",
                    "code": submit_failure.get("stage") or run.status.value,
                    "message": submit_failure.get("message") or run.status.value,
                    "context": submit_failure.get("context") or {},
                }
            )
        if (
            run.status in {SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.FAILED_TERMINAL}
            and not errors
        ):
            errors.append(
                {
                    "source": "simulation_daily_run",
                    "code": run.status.value,
                    "message": str(run.run_payload_json.get("last_stage") or run.status.value),
                    "context": {},
                }
            )
        return errors

    def start_scheduler(
        self, *, interval_seconds: int | None = None, default_submit: bool | None = None
    ) -> dict[str, Any]:
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
            data_source=(os.getenv("SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE") or "DB_HISTORICAL").strip()
            or "DB_HISTORICAL",
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
