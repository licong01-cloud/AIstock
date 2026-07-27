"""Strict, low-cardinality projection for K2 durable diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

from .kernel_repository import PostgresMiniQMTKernelRepository


@dataclass(frozen=True)
class KernelDiagnosticsProjectionV1:
    layer: dict[str, Any]
    metrics: tuple[dict[str, Any], ...]
    alerts: tuple[dict[str, Any], ...]


class KernelDiagnosticsReadServiceV1:
    """Read K2 facts only; never starts workers, repairs rows, or calls broker."""

    def __init__(self, repository: PostgresMiniQMTKernelRepository | None = None) -> None:
        self.repository = repository if repository is not None else PostgresMiniQMTKernelRepository()

    def read(self, *, runtime_id: str, trade_date: date, limit: int = 100) -> dict[str, Any]:
        return self.repository.read_kernel_diagnostics(runtime_id=runtime_id, trade_date=trade_date, limit=limit)


def project_kernel_diagnostics_v1(payload: Mapping[str, Any]) -> KernelDiagnosticsProjectionV1:
    if not isinstance(payload, Mapping):
        raise TypeError("kernel diagnostics payload must be a mapping")
    exact = dict(payload)
    if exact.get("schema_version") != "miniqmt_kernel_diagnostics_v1":
        raise ValueError("kernel diagnostics schema_version is unsupported")
    runtime_id = _text(exact.get("runtime_id"), "runtime_id")
    trade_date = _text(exact.get("trade_date"), "trade_date")
    schema_status = _text(exact.get("schema_status"), "schema_status")
    if _strict_bool(exact.get("read_only"), "read_only") is not True:
        raise ValueError("kernel diagnostics must be explicitly read-only")
    limit = _positive_bounded_int(exact.get("limit"), "limit", maximum=500)
    if len(_chains(exact)) > limit:
        raise ValueError("recent_command_chains exceeds the declared limit")
    if schema_status == "NOT_APPLIED":
        missing = exact.get("missing_tables")
        if not isinstance(missing, list) or not missing or any(type(item) is not str or not item for item in missing):
            raise ValueError("NOT_APPLIED diagnostics must expose missing_tables")
        return KernelDiagnosticsProjectionV1(
            layer={
                "schema_version": "simulation_platform_health_layer_v1",
                "layer": "miniqmt_kernel",
                "status": "NOT_DEPLOYED",
                "reason_code": "MINIQMT_KERNEL_SCHEMA_NOT_APPLIED",
                "source": "miniqmt_kernel_read_only_diagnostics",
                "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                "facts": {"schema_status": schema_status, "missing_tables": sorted(missing)},
                "execution_gate": False,
            },
            metrics=(
                {
                    "name": "simulation_miniqmt_kernel_schema_ready",
                    "kind": "gauge",
                    "value": 0,
                    "labels": {"backend": "MINIQMT_SIM", "status": "NOT_DEPLOYED", "source": "kernel_diagnostics"},
                },
            ),
            alerts=(),
        )
    if schema_status != "READY":
        raise ValueError("kernel diagnostics schema_status is unsupported")
    if exact.get("missing_tables") != []:
        raise ValueError("READY diagnostics cannot report missing tables")
    runtime_status = _text(exact.get("runtime_status"), "runtime_status")
    if runtime_status in {"NOT_FOUND", "NOT_ACTIVATED"}:
        return KernelDiagnosticsProjectionV1(
            layer={
                "schema_version": "simulation_platform_health_layer_v1",
                "layer": "miniqmt_kernel",
                "status": "NOT_DEPLOYED",
                "reason_code": f"MINIQMT_KERNEL_RUNTIME_{runtime_status}",
                "source": "miniqmt_kernel_read_only_diagnostics",
                "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                "facts": {"schema_status": schema_status, "runtime_status": runtime_status},
                "execution_gate": False,
            },
            metrics=(
                {
                    "name": "simulation_miniqmt_kernel_runtime_active",
                    "kind": "gauge",
                    "value": 0,
                    "labels": {
                        "backend": "MINIQMT_SIM",
                        "status": runtime_status,
                        "source": "kernel_diagnostics",
                    },
                },
            ),
            alerts=(),
        )
    if runtime_status != "ACTIVE":
        raise ValueError("kernel diagnostics runtime_status is unsupported")
    event_counts = _counts(exact, "event_type_counts")
    delivery_counts = _counts(exact, "delivery_status_counts")
    outbox_counts = _counts(exact, "outbox_status_counts")
    command_counts = _counts(exact, "outbox_command_type_counts")
    timer_counts = _counts(exact, "timer_status_counts")
    timer_occurrence_counts = _counts(exact, "timer_occurrence_status_counts")
    reason_counts = _counts(exact, "diagnostic_reason_family_counts")
    predecessor_gap_count = _nonnegative_int(exact.get("predecessor_gap_count"), "predecessor_gap_count")
    mapping_lineage_pending_count = _nonnegative_int(
        exact.get("mapping_lineage_pending_count"),
        "mapping_lineage_pending_count",
    )
    oldest_delivery_lag_seconds = _nonnegative_int(
        exact.get("oldest_delivery_lag_seconds"),
        "oldest_delivery_lag_seconds",
    )
    oldest_due_timer_lag_seconds = _nonnegative_int(
        exact.get("oldest_due_timer_lag_seconds"),
        "oldest_due_timer_lag_seconds",
    )
    blocked_count = (
        delivery_counts.get("FAILED_TERMINAL", 0) + outbox_counts.get("FAILED_TERMINAL", 0) + predecessor_gap_count
    )
    degraded_count = (
        delivery_counts.get("FAILED_RETRYABLE", 0)
        + outbox_counts.get("FAILED_RETRYABLE", 0)
        + outbox_counts.get("OUTCOME_UNKNOWN", 0)
        + outbox_counts.get("RECONCILING", 0)
        + mapping_lineage_pending_count
    )
    if blocked_count:
        status = "BLOCKED"
        reason_code = "MINIQMT_KERNEL_TERMINAL_OR_PREDECESSOR_FAILURE"
    elif degraded_count:
        status = "DEGRADED"
        reason_code = "MINIQMT_KERNEL_RETRY_OR_RECONCILE_PENDING"
    else:
        status = "HEALTHY"
        reason_code = "MINIQMT_KERNEL_DURABLE_FACTS_CLEAR"
    facts = {
        "schema_status": schema_status,
        "event_type_counts": event_counts,
        "delivery_status_counts": delivery_counts,
        "outbox_status_counts": outbox_counts,
        "outbox_command_type_counts": command_counts,
        "timer_status_counts": timer_counts,
        "timer_occurrence_status_counts": timer_occurrence_counts,
        "diagnostic_reason_family_counts": reason_counts,
        "predecessor_gap_count": predecessor_gap_count,
        "mapping_lineage_pending_count": mapping_lineage_pending_count,
        "oldest_delivery_lag_seconds": oldest_delivery_lag_seconds,
        "oldest_due_timer_lag_seconds": oldest_due_timer_lag_seconds,
        "runtime_status": runtime_status,
        "returned_command_chain_count": len(_chains(exact)),
        "truncated": _strict_bool(exact.get("truncated"), "truncated"),
    }
    metrics: list[dict[str, Any]] = [
        {
            "name": "simulation_miniqmt_kernel_schema_ready",
            "kind": "gauge",
            "value": 1,
            "labels": {"backend": "MINIQMT_SIM", "status": status, "source": "kernel_diagnostics"},
        },
        {
            "name": "simulation_miniqmt_kernel_runtime_active",
            "kind": "gauge",
            "value": 1,
            "labels": {"backend": "MINIQMT_SIM", "status": status, "source": "kernel_diagnostics"},
        },
        {
            "name": "simulation_miniqmt_kernel_predecessor_gap_count",
            "kind": "gauge",
            "value": predecessor_gap_count,
            "labels": {
                "backend": "MINIQMT_SIM",
                "status": status,
                "reason_family": "PREDECESSOR",
                "source": "kernel_diagnostics",
            },
        },
        {
            "name": "simulation_miniqmt_kernel_mapping_lineage_pending_count",
            "kind": "gauge",
            "value": mapping_lineage_pending_count,
            "labels": {
                "backend": "MINIQMT_SIM",
                "status": status,
                "reason_family": "RECONCILE",
                "source": "kernel_diagnostics",
            },
        },
        {
            "name": "simulation_miniqmt_kernel_delivery_lag_seconds",
            "kind": "gauge",
            "value": oldest_delivery_lag_seconds,
            "labels": {"backend": "MINIQMT_SIM", "status": status, "source": "kernel_diagnostics"},
        },
        {
            "name": "simulation_miniqmt_kernel_timer_due_lag_seconds",
            "kind": "gauge",
            "value": oldest_due_timer_lag_seconds,
            "labels": {"backend": "MINIQMT_SIM", "status": status, "source": "kernel_diagnostics"},
        },
    ]
    for event_type, count in event_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_event_count",
                "kind": "gauge",
                "value": count,
                "labels": {
                    "backend": "MINIQMT_SIM",
                    "event_type": event_type,
                    "status": "OBSERVED",
                    "source": "kernel_diagnostics",
                },
            }
        )
    for outbox_status, count in outbox_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_outbox_count",
                "kind": "gauge",
                "value": count,
                "labels": {"backend": "MINIQMT_SIM", "status": outbox_status, "source": "kernel_diagnostics"},
            }
        )
    for delivery_status, count in delivery_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_delivery_count",
                "kind": "gauge",
                "value": count,
                "labels": {"backend": "MINIQMT_SIM", "status": delivery_status, "source": "kernel_diagnostics"},
            }
        )
    for timer_status, count in timer_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_timer_count",
                "kind": "gauge",
                "value": count,
                "labels": {"backend": "MINIQMT_SIM", "status": timer_status, "source": "kernel_diagnostics"},
            }
        )
    for occurrence_status, count in timer_occurrence_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_timer_occurrence_count",
                "kind": "gauge",
                "value": count,
                "labels": {"backend": "MINIQMT_SIM", "status": occurrence_status, "source": "kernel_diagnostics"},
            }
        )
    for command_type, count in command_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_command_count",
                "kind": "gauge",
                "value": count,
                "labels": {
                    "backend": "MINIQMT_SIM",
                    "command_type": command_type,
                    "status": "OBSERVED",
                    "source": "kernel_diagnostics",
                },
            }
        )
    for reason_family, count in reason_counts.items():
        metrics.append(
            {
                "name": "simulation_miniqmt_kernel_diagnostic_count",
                "kind": "gauge",
                "value": count,
                "labels": {
                    "backend": "MINIQMT_SIM",
                    "reason_family": reason_family,
                    "status": "OBSERVED",
                    "source": "kernel_diagnostics",
                },
            }
        )
    alerts: list[dict[str, Any]] = []
    if status in {"BLOCKED", "DEGRADED"}:
        alerts.append(
            {
                "alert_type": "MINIQMT_KERNEL_DURABLE_HEALTH",
                "status": "CRITICAL" if status == "BLOCKED" else "WARNING",
                "reason_code": reason_code,
                "source": "miniqmt_kernel_read_only_diagnostics",
                "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                "context": {
                    "predecessor_gap_count": predecessor_gap_count,
                    "terminal_delivery_count": delivery_counts.get("FAILED_TERMINAL", 0),
                    "terminal_outbox_count": outbox_counts.get("FAILED_TERMINAL", 0),
                    "unknown_outbox_count": outbox_counts.get("OUTCOME_UNKNOWN", 0),
                    "reconciling_outbox_count": outbox_counts.get("RECONCILING", 0),
                    "mapping_lineage_pending_count": mapping_lineage_pending_count,
                    "oldest_delivery_lag_seconds": oldest_delivery_lag_seconds,
                    "oldest_due_timer_lag_seconds": oldest_due_timer_lag_seconds,
                    "stale_fence_rejection_count": reason_counts.get("FENCE", 0),
                },
            }
        )
    return KernelDiagnosticsProjectionV1(
        layer={
            "schema_version": "simulation_platform_health_layer_v1",
            "layer": "miniqmt_kernel",
            "status": status,
            "reason_code": reason_code,
            "source": "miniqmt_kernel_read_only_diagnostics",
            "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
            "facts": facts,
            "execution_gate": False,
        },
        metrics=tuple(metrics),
        alerts=tuple(alerts),
    )


def _counts(payload: Mapping[str, Any], key: str) -> dict[str, int]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise ValueError(f"{key} must be a mapping")
    result: dict[str, int] = {}
    for member_key, count in value.items():
        result[_text(member_key, f"{key}.key")] = _nonnegative_int(count, f"{key}.{member_key}")
    return dict(sorted(result.items()))


def _chains(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = payload.get("recent_command_chains")
    if not isinstance(value, list) or any(type(item) is not dict for item in value):
        raise ValueError("recent_command_chains must be a list of exact mappings")
    return value


def _text(value: Any, field: str) -> str:
    if type(value) is not str or not value.strip() or value != value.strip():
        raise ValueError(f"{field} must be non-empty trim-stable text")
    return value


def _nonnegative_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative strict integer")
    return value


def _strict_bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{field} must be an exact boolean")
    return value


def _positive_bounded_int(value: Any, field: str, *, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be a strict integer in [1, {maximum}]")
    return value
