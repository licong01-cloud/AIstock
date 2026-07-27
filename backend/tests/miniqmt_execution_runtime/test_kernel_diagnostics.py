from __future__ import annotations

from datetime import date

import pytest

from backend.services.miniqmt_execution_runtime.kernel_diagnostics import project_kernel_diagnostics_v1
from backend.services.miniqmt_execution_runtime.kernel_repository_diagnostics import (
    KernelRepositoryDiagnosticsMixin,
    _decode_kernel_cursor,
    _encode_kernel_cursor,
    _reason_family,
)


def _payload(**updates):
    payload = {
        "schema_version": "miniqmt_kernel_diagnostics_v1",
        "schema_status": "READY",
        "runtime_id": "runtime_k2d",
        "trade_date": "2026-07-27",
        "missing_tables": [],
        "event_type_counts": {"TIMER": 2},
        "delivery_status_counts": {"APPLIED": 2},
        "outbox_status_counts": {"ACKED": 1},
        "outbox_command_type_counts": {"SUBMIT_LIMIT": 1},
        "timer_status_counts": {"EMITTED": 2},
        "timer_occurrence_status_counts": {"EVENT_COMMITTED": 2},
        "diagnostic_reason_family_counts": {},
        "predecessor_gap_count": 0,
        "mapping_lineage_pending_count": 0,
        "expired_dispatching_lease_count": 0,
        "oldest_delivery_lag_seconds": 0,
        "oldest_due_timer_lag_seconds": 0,
        "runtime_status": "ACTIVE",
        "recent_command_chains": [],
        "limit": 100,
        "truncated": False,
        "next_cursor": None,
        "read_only": True,
    }
    payload.update(updates)
    return payload


def test_kernel_diagnostics_not_applied_is_explicit_and_non_blocking() -> None:
    projection = project_kernel_diagnostics_v1(
        _payload(
            schema_status="NOT_APPLIED",
            missing_tables=["execution_algo_command_outbox"],
            event_type_counts={},
            delivery_status_counts={},
            outbox_status_counts={},
            outbox_command_type_counts={},
            timer_status_counts={},
        )
    )
    assert projection.layer["status"] == "NOT_DEPLOYED"
    assert projection.layer["execution_gate"] is False
    assert projection.alerts == ()
    assert projection.metrics[0]["value"] == 0


def test_kernel_unknown_outcome_emits_low_cardinality_auto_clear_alert() -> None:
    degraded = project_kernel_diagnostics_v1(
        _payload(
            outbox_status_counts={"OUTCOME_UNKNOWN": 2, "RECONCILING": 1},
            diagnostic_reason_family_counts={"OUTCOME_UNKNOWN": 2},
        )
    )
    assert degraded.layer["status"] == "BLOCKED"
    assert degraded.alerts[0]["reason_code"] == "MINIQMT_COMMAND_OUTCOME_UNKNOWN"
    assert degraded.alerts[0]["identity"] == {"runtime_id": "runtime_k2d", "trade_date": "2026-07-27"}
    forbidden = {"runtime_id", "algo_instance_id", "command_id", "symbol", "order_id"}
    assert all(not (set(metric["labels"]) & forbidden) for metric in degraded.metrics)

    recovered = project_kernel_diagnostics_v1(_payload())
    assert recovered.layer["status"] == "HEALTHY"
    assert recovered.alerts == ()


def test_kernel_schema_ready_without_runtime_activation_is_not_false_green() -> None:
    projection = project_kernel_diagnostics_v1(_payload(runtime_status="NOT_ACTIVATED"))
    assert projection.layer["status"] == "NOT_DEPLOYED"
    assert projection.layer["reason_code"] == "MINIQMT_KERNEL_RUNTIME_NOT_ACTIVATED"
    assert projection.layer["execution_gate"] is False
    assert projection.alerts == ()


def test_kernel_terminal_and_predecessor_facts_are_blocking_but_not_execution_gates() -> None:
    projection = project_kernel_diagnostics_v1(
        _payload(
            delivery_status_counts={"FAILED_TERMINAL": 1},
            outbox_status_counts={"FAILED_TERMINAL": 1},
            predecessor_gap_count=1,
        )
    )
    assert projection.layer["status"] == "BLOCKED"
    assert projection.layer["execution_gate"] is False
    assert projection.alerts[0]["status"] == "CRITICAL"
    assert projection.alerts[0].get("acknowledge_required") is None

    expired = project_kernel_diagnostics_v1(_payload(expired_dispatching_lease_count=1))
    assert expired.layer["status"] == "BLOCKED"
    assert expired.alerts[0]["reason_code"] == "MINIQMT_COMMAND_OUTBOX_LEASE_EXPIRED"


@pytest.mark.parametrize(
    ("updates", "expected_status", "reason_code"),
    [
        ({"oldest_delivery_lag_seconds": 6}, "WARNING", "MINIQMT_COMMAND_OUTBOX_DELIVERY_LAG"),
        ({"oldest_delivery_lag_seconds": 31}, "CRITICAL", "MINIQMT_COMMAND_OUTBOX_DELIVERY_LAG"),
        ({"oldest_due_timer_lag_seconds": 3}, "WARNING", "MINIQMT_KERNEL_TIMER_DUE_LAG"),
        ({"oldest_due_timer_lag_seconds": 11}, "CRITICAL", "MINIQMT_KERNEL_TIMER_DUE_LAG"),
    ],
)
def test_kernel_cadence_thresholds_emit_auto_clear_alerts(updates, expected_status, reason_code) -> None:
    projection = project_kernel_diagnostics_v1(_payload(**updates))
    alert = next(item for item in projection.alerts if item["reason_code"] == reason_code)
    assert alert["status"] == expected_status
    assert alert["context"]["auto_clear"] is True
    assert project_kernel_diagnostics_v1(_payload()).alerts == ()


def test_kernel_readback_failure_is_critical_and_never_false_green() -> None:
    projection = project_kernel_diagnostics_v1(
        _payload(
            schema_status="READBACK_FAILED",
            reason_code="MINIQMT_KERNEL_READBACK_SCALAR_DRIFT",
            failure_type="KernelRepositoryConflict",
        )
    )
    assert projection.layer["status"] == "BLOCKED"
    assert projection.alerts[0]["status"] == "CRITICAL"
    assert projection.alerts[0]["reason_code"] == "MINIQMT_KERNEL_READBACK_SCALAR_DRIFT"


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"predecessor_gap_count": True}, "non-negative strict integer"),
        ({"outbox_status_counts": {"ACKED": -1}}, "non-negative strict integer"),
        ({"truncated": 1}, "exact boolean"),
        ({"runtime_status": "UNKNOWN"}, "runtime_status is unsupported"),
        ({"recent_command_chains": [{}, "bad"]}, "exact mappings"),
    ],
)
def test_kernel_diagnostics_rejects_malformed_facts_without_false_green(updates, message) -> None:
    with pytest.raises(ValueError, match=message):
        project_kernel_diagnostics_v1(_payload(**updates))


@pytest.mark.parametrize(
    ("reason_code", "expected"),
    [
        ("MINIQMT_KERNEL_FENCE_STALE", "FENCE"),
        ("MINIQMT_PREDECESSOR_GAP", "PREDECESSOR"),
        ("MINIQMT_COMMAND_OUTCOME_UNKNOWN", "OUTCOME_UNKNOWN"),
        ("MINIQMT_RECONCILE_CONFLICT", "RECONCILE"),
        ("MINIQMT_DISPATCH_FAILED", "DISPATCH"),
        ("MINIQMT_TIMER_LATE", "TIMER"),
        ("MINIQMT_INGRESS_FAILED", "INGRESS"),
        ("MINIQMT_OTHER_FAILURE", "OTHER"),
        ("   ", "UNCLASSIFIED"),
    ],
)
def test_repository_diagnostic_reason_family_is_stable_and_low_cardinality(reason_code, expected) -> None:
    assert _reason_family(reason_code) == expected


def test_repository_diagnostics_rejects_invalid_query_before_database_access() -> None:
    diagnostics = KernelRepositoryDiagnosticsMixin()
    with pytest.raises(ValueError, match="runtime_id"):
        diagnostics.read_kernel_diagnostics(runtime_id=" ", trade_date=date(2026, 7, 27))
    with pytest.raises(TypeError, match="exact date"):
        diagnostics.read_kernel_diagnostics(runtime_id="runtime_k2d", trade_date="2026-07-27")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match=r"\[1, 500\]"):
        diagnostics.read_kernel_diagnostics(
            runtime_id="runtime_k2d",
            trade_date=date(2026, 7, 27),
            limit=501,
        )


def test_kernel_diagnostics_cursor_is_strict_stable_and_round_trips() -> None:
    cursor = _encode_kernel_cursor("2026-07-27T01:30:00Z", "mqcommand_cursor")
    assert _decode_kernel_cursor(cursor) == ("2026-07-27T01:30:00.000000Z", "mqcommand_cursor")
    with pytest.raises(ValueError, match="cursor"):
        _decode_kernel_cursor("2026-07-27T01:30:00Z|command|extra")


def test_repository_diagnostic_aggregate_rejects_non_authoritative_sql_fragments() -> None:
    with pytest.raises(ValueError, match="aggregate authority"):
        KernelRepositoryDiagnosticsMixin._diagnostic_group_counts(
            object(),
            table="execution_runtime",
            key_column="status",
            runtime_id="runtime_k2d",
            trade_date=date(2026, 7, 27),
        )
    with pytest.raises(ValueError, match="predicate"):
        KernelRepositoryDiagnosticsMixin._diagnostic_group_counts(
            object(),
            table="execution_runtime_event",
            key_column="event_type",
            runtime_id="runtime_k2d",
            trade_date=date(2026, 7, 27),
            extra_predicate="FALSE OR TRUE",
        )
