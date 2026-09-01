"""Strict, low-cardinality projection for K2 durable diagnostics."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

from .kernel_repository import PostgresMiniQMTKernelRepository
from .plugin_canonical import hash_hex_v1


@dataclass(frozen=True)
class KernelDiagnosticsProjectionV1:
    layer: dict[str, Any]
    metrics: tuple[dict[str, Any], ...]
    alerts: tuple[dict[str, Any], ...]


class KernelDiagnosticsReadServiceV1:
    """Read K2 facts only; never starts workers, repairs rows, or calls broker."""

    def __init__(self, repository: PostgresMiniQMTKernelRepository | None = None) -> None:
        self.repository = repository if repository is not None else PostgresMiniQMTKernelRepository()

    def read(self, *, runtime_id: str, trade_date: date, limit: int = 100, cursor: str | None = None) -> dict[str, Any]:
        return self.repository.read_kernel_diagnostics(
            runtime_id=runtime_id, trade_date=trade_date, limit=limit, cursor=cursor
        )


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
    if schema_status == "READBACK_FAILED":
        reason_code = _text(exact.get("reason_code"), "reason_code")
        failure_type = _text(exact.get("failure_type"), "failure_type")
        return KernelDiagnosticsProjectionV1(
            layer={
                "schema_version": "simulation_platform_health_layer_v1",
                "layer": "miniqmt_kernel",
                "status": "BLOCKED",
                "reason_code": reason_code,
                "source": "miniqmt_kernel_read_only_diagnostics",
                "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                "facts": {"schema_status": schema_status, "failure_type": failure_type},
                "execution_gate": False,
            },
            metrics=(
                {
                    "name": "simulation_miniqmt_kernel_schema_ready",
                    "kind": "gauge",
                    "value": 0,
                    "labels": {"backend": "MINIQMT_SIM", "status": "BLOCKED", "source": "kernel_diagnostics"},
                },
            ),
            alerts=(
                {
                    "alert_type": "MINIQMT_KERNEL_READBACK",
                    "status": "CRITICAL",
                    "reason_code": reason_code,
                    "source": "miniqmt_kernel_read_only_diagnostics",
                    "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                    "context": {"schema_status": schema_status, "failure_type": failure_type},
                },
            ),
        )
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
    expired_dispatching_lease_count = _nonnegative_int(
        exact.get("expired_dispatching_lease_count"),
        "expired_dispatching_lease_count",
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
        delivery_counts.get("FAILED_TERMINAL", 0)
        + outbox_counts.get("FAILED_TERMINAL", 0)
        + predecessor_gap_count
        + expired_dispatching_lease_count
        + outbox_counts.get("OUTCOME_UNKNOWN", 0)
    )
    degraded_count = (
        delivery_counts.get("FAILED_RETRYABLE", 0)
        + outbox_counts.get("FAILED_RETRYABLE", 0)
        + outbox_counts.get("OUTCOME_UNKNOWN", 0)
        + outbox_counts.get("RECONCILING", 0)
        + mapping_lineage_pending_count
    )
    if predecessor_gap_count:
        status = "BLOCKED"
        reason_code = "MINIQMT_KERNEL_PREDECESSOR_GAP"
    elif expired_dispatching_lease_count:
        status = "BLOCKED"
        reason_code = "MINIQMT_COMMAND_OUTBOX_LEASE_EXPIRED"
    elif outbox_counts.get("OUTCOME_UNKNOWN", 0):
        status = "BLOCKED"
        reason_code = "MINIQMT_COMMAND_OUTCOME_UNKNOWN"
    elif blocked_count:
        status = "BLOCKED"
        reason_code = "MINIQMT_KERNEL_TERMINAL_FAILURE"
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
        "expired_dispatching_lease_count": expired_dispatching_lease_count,
        "oldest_delivery_lag_seconds": oldest_delivery_lag_seconds,
        "oldest_due_timer_lag_seconds": oldest_due_timer_lag_seconds,
        "runtime_status": runtime_status,
        "returned_command_chain_count": len(_chains(exact)),
        "truncated": _strict_bool(exact.get("truncated"), "truncated"),
        "next_cursor": _optional_text(exact.get("next_cursor"), "next_cursor"),
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
    lag_alerts = (
        (oldest_delivery_lag_seconds, 30, 5, "MINIQMT_COMMAND_OUTBOX_DELIVERY_LAG", "delivery"),
        (oldest_due_timer_lag_seconds, 10, 2, "MINIQMT_KERNEL_TIMER_DUE_LAG", "timer"),
    )
    for lag, critical_threshold, warning_threshold, alert_reason, kind in lag_alerts:
        if lag > warning_threshold:
            alerts.append(
                {
                    "alert_type": "MINIQMT_KERNEL_CADENCE",
                    "status": "CRITICAL" if lag > critical_threshold else "WARNING",
                    "reason_code": alert_reason,
                    "source": "miniqmt_kernel_read_only_diagnostics",
                    "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                    "context": {"kind": kind, "lag_seconds": lag, "auto_clear": True},
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


def project_k6d_product_diagnostics_v1(
    payload: Mapping[str, Any],
    *,
    quote_activation: Mapping[str, Any],
) -> KernelDiagnosticsProjectionV1:
    """Project the final K6-D owner and live source as one read-only authority.

    Durable route facts come only from ``read_kernel_diagnostics``.  The live
    source capability comes only from the in-process quote activation health.
    Neither side may manufacture the other, and a disagreement is visible as
    an automatically clearing alert rather than an execution gate.
    """

    if not isinstance(payload, Mapping) or not isinstance(quote_activation, Mapping):
        raise TypeError("K6-D diagnostics inputs must be mappings")
    runtime_id = _text(payload.get("runtime_id"), "runtime_id")
    trade_date = _text(payload.get("trade_date"), "trade_date")
    route = payload.get("product_route")
    if not isinstance(route, Mapping):
        raise ValueError("product_route must be a mapping")
    if route.get("schema_version") != "miniqmt_k6d_product_route_diagnostics_v1":
        raise ValueError("product_route schema_version is unsupported")
    if _strict_bool(route.get("read_only"), "product_route.read_only") is not True:
        raise ValueError("product_route must be explicitly read-only")
    route_status = _text(route.get("status"), "product_route.status")
    if route_status in {"SCHEMA_NOT_APPLIED", "NOT_ACTIVATED"}:
        facts = {
            "route_status": route_status,
            "source_registered": False,
            "active_failure": None,
            "last_failure": None,
        }
        return KernelDiagnosticsProjectionV1(
            layer={
                "schema_version": "miniqmt_k6d_platform_diagnostics_v1",
                "layer": "miniqmt_k6d",
                "status": "NOT_DEPLOYED",
                "reason_code": f"MINIQMT_K6_PRODUCT_ROUTE_{route_status}",
                "source": "miniqmt_k6d_route_and_live_source_readback",
                "identity": {"runtime_id": runtime_id, "trade_date": trade_date},
                "facts": facts,
                "projection_sha256": hash_hex_v1("miniqmt_k6d_platform_diagnostics_v1", facts),
                "execution_gate": False,
            },
            metrics=(
                {
                    "name": "miniqmt_k6_route_owner_info",
                    "kind": "gauge",
                    "value": 0,
                    "labels": {"route": "NONE", "status": route_status, "source": "kernel_diagnostics"},
                },
            ),
            alerts=(),
        )
    if route_status != "ACTIVE":
        raise ValueError("product_route status is unsupported")
    for field, expected in (("runtime_id", runtime_id), ("trade_date", trade_date)):
        if _text(route.get(field), f"product_route.{field}") != expected:
            raise ValueError(f"product_route {field} differs from diagnostics owner")
    binding_id = _text(route.get("binding_id"), "product_route.binding_id")
    route_owner = _text(route.get("route_owner"), "product_route.route_owner")
    route_epoch = _nonnegative_int(route.get("route_epoch"), "product_route.route_epoch")
    cutoff = _nonnegative_int(
        route.get("effective_new_instance_sequence"),
        "product_route.effective_new_instance_sequence",
    )
    owner_row_version = _nonnegative_int(route.get("owner_row_version"), "product_route.owner_row_version")
    legacy_count = _nonnegative_int(
        route.get("legacy_active_instance_count"),
        "product_route.legacy_active_instance_count",
    )
    kernel_count = _nonnegative_int(
        route.get("kernel_active_instance_count"),
        "product_route.kernel_active_instance_count",
    )
    cutover_legacy_count = _nonnegative_int(
        route.get("cutover_legacy_active_instance_count"),
        "product_route.cutover_legacy_active_instance_count",
    )
    cutover_kernel_count = _nonnegative_int(
        route.get("cutover_kernel_active_instance_count"),
        "product_route.cutover_kernel_active_instance_count",
    )
    hashes = {
        name: _sha256(route.get(name), f"product_route.{name}")
        for name in (
            "owner_sha256",
            "current_receipt_sha256",
            "catalog_sha256",
            "gateway_capability_catalog_sha256",
            "exchange_session_authority_sha256",
            "migration_readback_sha256",
            "product_authority_schema_sha256",
        )
    }
    coordination_counts = _counts(route, "coordination_status_counts")
    live_runtimes = quote_activation.get("kernel_product_runtimes")
    if not isinstance(live_runtimes, list) or any(not isinstance(item, Mapping) for item in live_runtimes):
        raise ValueError("quote activation kernel_product_runtimes must be a list of mappings")
    matches = [item for item in live_runtimes if item.get("runtime_id") == runtime_id]
    if len(matches) > 1:
        raise ValueError("quote activation repeats one K6-D runtime identity")
    source_registered = len(matches) == 1
    source_capability_sha256 = None
    source_failure: dict[str, Any] | None = None
    if source_registered:
        live = matches[0]
        if _text(live.get("binding_id"), "quote_activation.binding_id") != binding_id:
            raise ValueError("live K6-D binding differs from durable route owner")
        if _text(live.get("trade_date"), "quote_activation.trade_date") != trade_date:
            raise ValueError("live K6-D trade_date differs from durable route owner")
        source_capability_sha256 = _sha256(
            live.get("source_capability_sha256"),
            "quote_activation.source_capability_sha256",
        )
    else:
        source_failure = {
            "reason_code": "MINIQMT_K6_PRODUCT_RUNTIME_NOT_REGISTERED",
            "reason_family": "SOURCE",
        }
    if route_owner != "KERNEL_V2":
        source_failure = {
            "reason_code": "MINIQMT_K6_PRODUCT_ROUTE_OWNER_INVALID",
            "reason_family": "ROUTE",
        }
    elif legacy_count:
        source_failure = {
            "reason_code": "MINIQMT_K6_LEGACY_ACTIVE_AFTER_CUTOVER",
            "reason_family": "ROUTE",
        }
    status = "HEALTHY" if source_failure is None else "BLOCKED"
    reason_code = "MINIQMT_K6_PRODUCT_ROUTE_CLEAR" if source_failure is None else str(source_failure["reason_code"])
    facts = {
        "runtime_id": runtime_id,
        "binding_id": binding_id,
        "trade_date": trade_date,
        "route_status": route_status,
        "route_owner": route_owner,
        "route_epoch": route_epoch,
        "effective_new_instance_sequence": cutoff,
        "owner_row_version": owner_row_version,
        "legacy_active_instance_count": legacy_count,
        "kernel_active_instance_count": kernel_count,
        "cutover_legacy_active_instance_count": cutover_legacy_count,
        "cutover_kernel_active_instance_count": cutover_kernel_count,
        **hashes,
        "coordination_status_counts": coordination_counts,
        "source_registered": source_registered,
        "source_capability_sha256": source_capability_sha256,
        "active_failure": source_failure,
        "last_failure": None,
    }
    metrics = (
        {
            "name": "miniqmt_k6_route_owner_info",
            "kind": "gauge",
            "value": 1,
            "labels": {"route": route_owner, "status": status, "source": "kernel_diagnostics"},
        },
        {
            "name": "miniqmt_k6_legacy_active_instances",
            "kind": "gauge",
            "value": legacy_count,
            "labels": {"status": status, "source": "kernel_diagnostics"},
        },
        {
            "name": "miniqmt_k6_kernel_active_instances",
            "kind": "gauge",
            "value": kernel_count,
            "labels": {"status": status, "source": "kernel_diagnostics"},
        },
        {
            "name": "miniqmt_k6_active_failure",
            "kind": "gauge",
            "value": 0 if source_failure is None else 1,
            "labels": {
                "reason_family": "NONE" if source_failure is None else str(source_failure["reason_family"]),
                "status": status,
                "source": "kernel_diagnostics",
            },
        },
    )
    alerts = ()
    if source_failure is not None:
        alerts = (
            {
                "alert_type": "MINIQMT_K6_PRODUCT_ROUTE",
                "status": "CRITICAL",
                "reason_code": reason_code,
                "source": "miniqmt_k6d_route_and_live_source_readback",
                "identity": {"runtime_id": runtime_id, "binding_id": binding_id, "trade_date": trade_date},
                "context": {
                    "route_owner": route_owner,
                    "legacy_active_instance_count": legacy_count,
                    "source_registered": source_registered,
                    "auto_clear": True,
                },
            },
        )
    return KernelDiagnosticsProjectionV1(
        layer={
            "schema_version": "miniqmt_k6d_platform_diagnostics_v1",
            "layer": "miniqmt_k6d",
            "status": status,
            "reason_code": reason_code,
            "source": "miniqmt_k6d_route_and_live_source_readback",
            "identity": {"runtime_id": runtime_id, "binding_id": binding_id, "trade_date": trade_date},
            "facts": facts,
            "projection_sha256": hash_hex_v1("miniqmt_k6d_platform_diagnostics_v1", facts),
            "execution_gate": False,
        },
        metrics=metrics,
        alerts=alerts,
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


def _sha256(value: Any, field: str) -> str:
    normalized = _text(value, field)
    if len(normalized) != 64 or any(character not in "0123456789abcdef" for character in normalized):
        raise ValueError(f"{field} must be lowercase SHA-256")
    return normalized


def _nonnegative_int(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a non-negative strict integer")
    return value


def _strict_bool(value: Any, field: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{field} must be an exact boolean")
    return value


def _optional_text(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field)


def _positive_bounded_int(value: Any, field: str, *, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError(f"{field} must be a strict integer in [1, {maximum}]")
    return value
