from __future__ import annotations

import pytest

from backend.services.miniqmt_execution_runtime.plugin_canonical import thaw_json_v1
from backend.services.miniqmt_execution_runtime.vnpy_facade_diagnostics import (
    _reset_vnpy_facade_diagnostics_for_tests_v1,
    read_vnpy_facade_diagnostics_v1,
    record_vnpy_facade_characterization_build_v1,
    record_vnpy_facade_conformance_v1,
    record_vnpy_facade_repository_read_v1,
    record_vnpy_facade_runtime_invocation_v1,
    record_vnpy_facade_source_execution_v1,
)


def test_diagnostics_are_bounded_low_cardinality_and_keep_active_last_failure_separate() -> None:
    _reset_vnpy_facade_diagnostics_for_tests_v1()
    record_vnpy_facade_source_execution_v1(
        algo_code="ICEBERG",
        status="FAILED",
        reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
    )
    record_vnpy_facade_characterization_build_v1(
        status="FAILED",
        reason_code="MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
    )
    record_vnpy_facade_repository_read_v1(read_kind="LATEST_PRIOR_TICK", outcome="UNAVAILABLE")
    record_vnpy_facade_runtime_invocation_v1(
        phase="TRANSITION",
        outcome="FAILED",
        reason_code="MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
    )
    record_vnpy_facade_conformance_v1(
        status="FAILED",
        reason_code="MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
    )

    snapshot = read_vnpy_facade_diagnostics_v1()
    labels = [thaw_json_v1(item.labels) for item in snapshot.metrics]

    assert snapshot.read_only is True
    assert snapshot.active_failure is not None
    assert snapshot.last_failure == snapshot.active_failure
    assert snapshot.runtime_invocation_count == 1
    assert all(
        not ({"symbol", "runtime_id", "algo_instance_id", "event_id", "delivery_id"} & set(item)) for item in labels
    )
    assert {item.name for item in snapshot.metrics} == {
        "miniqmt_vnpy_facade_source_execution_total",
        "miniqmt_vnpy_facade_characterization_build_total",
        "miniqmt_vnpy_facade_repository_read_total",
        "miniqmt_vnpy_facade_runtime_invocation_total",
        "miniqmt_vnpy_facade_conformance_build_total",
        "miniqmt_vnpy_facade_active_failure",
    }


def test_diagnostics_reject_unknown_labels_instead_of_cardinality_fallback() -> None:
    with pytest.raises(ValueError, match="allowlist"):
        record_vnpy_facade_source_execution_v1(
            algo_code="USER_SUPPLIED_ALGO",
            status="PASSED",
            reason_code="NONE",
        )
    with pytest.raises(ValueError, match="allowlist"):
        record_vnpy_facade_runtime_invocation_v1(
            phase="TRANSITION",
            outcome="FAILED",
            reason_code="UNBOUNDED_USER_REASON",
        )
