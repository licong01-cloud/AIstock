"""Bounded process-local K4 diagnostics projected through the existing metrics shape."""

from __future__ import annotations

from collections import Counter
from threading import RLock
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import model_validator

from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeConformanceAuthorityV2

from .plugin_canonical import hash_hex_v1
from .plugin_contracts import FrozenJsonObjectFieldV1, FrozenStrictModel

if TYPE_CHECKING:
    from backend.execution_algos.vnpy_compat.facade_characterization import (
        VnpyFacadeCharacterizationAuthorityV2,
    )


_ALGO_CODES = (
    "BEST_LIMIT_MINIQMT",
    "ICEBERG",
    "SNIPER_MINIQMT",
    "STOP",
    "TWAP_LITE_MINIQMT",
)
_REASON_CODES = frozenset(
    {
        "NONE",
        "MINIQMT_VNPY_FACADE_BINDING_INVALID",
        "MINIQMT_VNPY_FACADE_CANCEL_OWNERSHIP_INVALID",
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_DRIFT",
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_EXECUTION_UNAVAILABLE",
        "MINIQMT_VNPY_FACADE_CHARACTERIZATION_FAILED",
        "MINIQMT_VNPY_FACADE_CONFORMANCE_AUTHORITY_INVALID",
        "MINIQMT_VNPY_FACADE_CONFORMANCE_DRIFT",
        "MINIQMT_VNPY_FACADE_CONFORMANCE_RECEIPT_INVALID",
        "MINIQMT_VNPY_FACADE_CONTRACT_INVALID",
        "MINIQMT_VNPY_FACADE_CONTRACT_UNAVAILABLE",
        "MINIQMT_VNPY_FACADE_DETERMINISTIC_INPUT_INVALID",
        "MINIQMT_VNPY_FACADE_DTO_MAPPING_INVALID",
        "MINIQMT_VNPY_FACADE_EFFECT_CONFLICT",
        "MINIQMT_VNPY_FACADE_ISOLATED_MODULE_INVALID",
        "MINIQMT_VNPY_FACADE_MARKET_DATA_INVALID",
        "MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
        "MINIQMT_VNPY_FACADE_SOURCE_EXECUTION_FAILED",
        "MINIQMT_VNPY_FACADE_SOURCE_EXECUTOR_INVALID",
        "MINIQMT_VNPY_FACADE_SOURCE_INVALID",
        "MINIQMT_VNPY_FACADE_STATE_MAPPING_INVALID",
        "MINIQMT_VNPY_FACADE_TICK_UNAVAILABLE",
    }
)


class VnpyFacadeMetricV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_metric_v1"] = "miniqmt_vnpy_facade_metric_v1"
    name: str
    labels: FrozenJsonObjectFieldV1
    value: int


class VnpyFacadeFailureSummaryV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_failure_summary_v1"] = "miniqmt_vnpy_facade_failure_summary_v1"
    stage: str
    reason_code: str
    outcome: str


class VnpyFacadeDiagnosticsSnapshotV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_vnpy_facade_diagnostics_v1"] = "miniqmt_vnpy_facade_diagnostics_v1"
    read_only: Literal[True] = True
    source_manifest_sha256: str | None
    source_executor_binding_sha256: str | None
    vector_artifact_sha256: str | None
    vector_artifact_file_sha256: str | None
    ordered_execution_set_sha256s: tuple[str, ...]
    ordered_characterization_receipt_sha256s: tuple[str, ...]
    conformance_set_sha256: str | None
    algorithm_statuses: FrozenJsonObjectFieldV1
    active_failure: VnpyFacadeFailureSummaryV1 | None
    last_failure: VnpyFacadeFailureSummaryV1 | None
    runtime_invocation_count: int
    metrics: tuple[VnpyFacadeMetricV1, ...]
    snapshot_sha256: str

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_vnpy_facade_diagnostics_v1",
            "read_only": True,
            **values,
        }
        hash_payload = {
            **payload,
            "ordered_execution_set_sha256s": list(values["ordered_execution_set_sha256s"]),
            "ordered_characterization_receipt_sha256s": list(values["ordered_characterization_receipt_sha256s"]),
            "active_failure": (
                None if values["active_failure"] is None else values["active_failure"].model_dump(mode="json")
            ),
            "last_failure": (
                None if values["last_failure"] is None else values["last_failure"].model_dump(mode="json")
            ),
            "metrics": [item.model_dump(mode="json") for item in values["metrics"]],
        }
        return cls(
            **payload,
            snapshot_sha256=hash_hex_v1("miniqmt_vnpy_facade_diagnostics_v1", hash_payload),
        )

    @model_validator(mode="after")
    def _validate_snapshot(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_vnpy_facade_diagnostics_v1",
            self.canonical_payload_v1(exclude={"snapshot_sha256"}),
        )
        if self.snapshot_sha256 != expected:
            raise ValueError("vn.py facade diagnostics snapshot hash mismatch")
        return self


def _strict_member(value: str, allowed: frozenset[str] | tuple[str, ...], *, field_name: str) -> str:
    if type(value) is not str or value not in allowed:
        raise ValueError(f"{field_name} is outside the code-owned diagnostics allowlist")
    return value


class _VnpyFacadeDiagnosticsStateV1:
    def __init__(self) -> None:
        self._lock = RLock()
        self._counters: Counter[tuple[str, tuple[tuple[str, str], ...]]] = Counter()
        self._source_manifest_sha256: str | None = None
        self._source_executor_binding_sha256: str | None = None
        self._vector_artifact_sha256: str | None = None
        self._vector_artifact_file_sha256: str | None = None
        self._execution_set_sha256s: tuple[str, ...] = ()
        self._characterization_receipt_sha256s: tuple[str, ...] = ()
        self._conformance_set_sha256: str | None = None
        self._algorithm_statuses: dict[str, str] = {}
        self._active_failure: VnpyFacadeFailureSummaryV1 | None = None
        self._last_failure: VnpyFacadeFailureSummaryV1 | None = None

    def _increment(self, name: str, labels: dict[str, str]) -> None:
        self._counters[(name, tuple(sorted(labels.items())))] += 1

    def _record_failure(self, *, stage: str, reason_code: str, outcome: str) -> None:
        summary = VnpyFacadeFailureSummaryV1(
            stage=stage,
            reason_code=reason_code,
            outcome=outcome,
        )
        self._active_failure = summary
        self._last_failure = summary

    def record_source_execution(self, *, algo_code: str, status: str, reason_code: str) -> None:
        algo_code = _strict_member(algo_code, _ALGO_CODES, field_name="algo_code")
        status = _strict_member(status, ("PASSED", "FAILED"), field_name="status")
        reason_code = _strict_member(reason_code, _REASON_CODES, field_name="reason_code")
        with self._lock:
            self._increment(
                "miniqmt_vnpy_facade_source_execution_total",
                {"algo_code": algo_code, "status": status, "reason_code": reason_code},
            )
            self._algorithm_statuses[algo_code] = status
            if status == "FAILED":
                self._record_failure(stage="SOURCE_EXECUTION", reason_code=reason_code, outcome=status)

    def record_conformance(self, *, status: str, reason_code: str) -> None:
        status = _strict_member(status, ("PASSED", "FAILED"), field_name="status")
        reason_code = _strict_member(reason_code, _REASON_CODES, field_name="reason_code")
        with self._lock:
            self._increment(
                "miniqmt_vnpy_facade_conformance_build_total",
                {"status": status, "reason_code": reason_code},
            )
            if status == "FAILED":
                self._record_failure(stage="CONFORMANCE", reason_code=reason_code, outcome=status)

    def record_runtime_invocation(self, *, phase: str, outcome: str, reason_code: str) -> None:
        phase = _strict_member(phase, ("INITIALIZE", "TRANSITION"), field_name="phase")
        outcome = _strict_member(outcome, ("PASSED", "FAILED"), field_name="outcome")
        reason_code = _strict_member(reason_code, _REASON_CODES, field_name="reason_code")
        with self._lock:
            self._increment(
                "miniqmt_vnpy_facade_runtime_invocation_total",
                {"phase": phase, "outcome": outcome, "reason_code": reason_code},
            )

    def record_repository_read(self, *, read_kind: str, outcome: str) -> None:
        read_kind = _strict_member(read_kind, ("ALGO_START", "LATEST_PRIOR_TICK"), field_name="read_kind")
        outcome = _strict_member(outcome, ("FOUND", "UNAVAILABLE", "INVALID"), field_name="outcome")
        with self._lock:
            self._increment(
                "miniqmt_vnpy_facade_repository_read_total",
                {"read_kind": read_kind, "outcome": outcome},
            )
            if outcome == "INVALID":
                self._record_failure(
                    stage="REPOSITORY_READ",
                    reason_code="MINIQMT_VNPY_FACADE_REPOSITORY_READ_INVALID",
                    outcome=outcome,
                )

    def publish_characterization(
        self,
        *,
        authority: VnpyFacadeCharacterizationAuthorityV2,
        source_manifest_sha256: str,
        vector_artifact_sha256: str,
        vector_artifact_file_sha256: str,
    ) -> None:
        from backend.execution_algos.vnpy_compat.facade_characterization import (
            VnpyFacadeCharacterizationAuthorityV2,
        )

        if not isinstance(authority, VnpyFacadeCharacterizationAuthorityV2):
            raise TypeError("authority must be VnpyFacadeCharacterizationAuthorityV2")
        with self._lock:
            self._source_manifest_sha256 = source_manifest_sha256
            self._source_executor_binding_sha256 = authority.source_executor_binding.binding_sha256
            self._vector_artifact_sha256 = vector_artifact_sha256
            self._vector_artifact_file_sha256 = vector_artifact_file_sha256
            self._execution_set_sha256s = tuple(item.execution_set_sha256 for item in authority.source_execution_sets)
            self._characterization_receipt_sha256s = tuple(item.receipt_sha256 for item in authority.receipts)
            self._algorithm_statuses = {item.algo_code: item.status.value for item in authority.receipts}
            self._active_failure = None

    def publish_conformance(self, authority: VnpyFacadeConformanceAuthorityV2) -> None:
        if not isinstance(authority, VnpyFacadeConformanceAuthorityV2):
            raise TypeError("authority must be VnpyFacadeConformanceAuthorityV2")
        with self._lock:
            self._conformance_set_sha256 = authority.conformance_set.receipt_set_sha256
            self._active_failure = None

    def snapshot(self) -> VnpyFacadeDiagnosticsSnapshotV1:
        with self._lock:
            metrics = tuple(
                VnpyFacadeMetricV1(
                    name=name,
                    labels=dict(labels),
                    value=value,
                )
                for (name, labels), value in sorted(self._counters.items())
            )
            runtime_count = sum(
                value
                for (name, _labels), value in self._counters.items()
                if name == "miniqmt_vnpy_facade_runtime_invocation_total"
            )
            active_metric = VnpyFacadeMetricV1(
                name="miniqmt_vnpy_facade_active_failure",
                labels={
                    "stage": "NONE" if self._active_failure is None else self._active_failure.stage,
                    "reason_code": ("NONE" if self._active_failure is None else self._active_failure.reason_code),
                },
                value=0 if self._active_failure is None else 1,
            )
            return VnpyFacadeDiagnosticsSnapshotV1.create(
                source_manifest_sha256=self._source_manifest_sha256,
                source_executor_binding_sha256=self._source_executor_binding_sha256,
                vector_artifact_sha256=self._vector_artifact_sha256,
                vector_artifact_file_sha256=self._vector_artifact_file_sha256,
                ordered_execution_set_sha256s=self._execution_set_sha256s,
                ordered_characterization_receipt_sha256s=(self._characterization_receipt_sha256s),
                conformance_set_sha256=self._conformance_set_sha256,
                algorithm_statuses=dict(sorted(self._algorithm_statuses.items())),
                active_failure=self._active_failure,
                last_failure=self._last_failure,
                runtime_invocation_count=runtime_count,
                metrics=(*metrics, active_metric),
            )

    def reset_for_tests(self) -> None:
        with self._lock:
            self._counters.clear()
            self._source_manifest_sha256 = None
            self._source_executor_binding_sha256 = None
            self._vector_artifact_sha256 = None
            self._vector_artifact_file_sha256 = None
            self._execution_set_sha256s = ()
            self._characterization_receipt_sha256s = ()
            self._conformance_set_sha256 = None
            self._algorithm_statuses = {}
            self._active_failure = None
            self._last_failure = None


_STATE = _VnpyFacadeDiagnosticsStateV1()


def record_vnpy_facade_source_execution_v1(*, algo_code: str, status: str, reason_code: str) -> None:
    _STATE.record_source_execution(algo_code=algo_code, status=status, reason_code=reason_code)


def record_vnpy_facade_conformance_v1(*, status: str, reason_code: str) -> None:
    _STATE.record_conformance(status=status, reason_code=reason_code)


def record_vnpy_facade_runtime_invocation_v1(*, phase: str, outcome: str, reason_code: str) -> None:
    _STATE.record_runtime_invocation(phase=phase, outcome=outcome, reason_code=reason_code)


def record_vnpy_facade_repository_read_v1(*, read_kind: str, outcome: str) -> None:
    _STATE.record_repository_read(read_kind=read_kind, outcome=outcome)


def publish_vnpy_facade_characterization_v1(
    *,
    authority: VnpyFacadeCharacterizationAuthorityV2,
    source_manifest_sha256: str,
    vector_artifact_sha256: str,
    vector_artifact_file_sha256: str,
) -> None:
    _STATE.publish_characterization(
        authority=authority,
        source_manifest_sha256=source_manifest_sha256,
        vector_artifact_sha256=vector_artifact_sha256,
        vector_artifact_file_sha256=vector_artifact_file_sha256,
    )


def publish_vnpy_facade_conformance_v1(
    authority: VnpyFacadeConformanceAuthorityV2,
) -> None:
    _STATE.publish_conformance(authority)


def read_vnpy_facade_diagnostics_v1() -> VnpyFacadeDiagnosticsSnapshotV1:
    return _STATE.snapshot()


def _reset_vnpy_facade_diagnostics_for_tests_v1() -> None:
    _STATE.reset_for_tests()


__all__ = [
    "VnpyFacadeDiagnosticsSnapshotV1",
    "VnpyFacadeFailureSummaryV1",
    "VnpyFacadeMetricV1",
    "publish_vnpy_facade_characterization_v1",
    "publish_vnpy_facade_conformance_v1",
    "read_vnpy_facade_diagnostics_v1",
    "record_vnpy_facade_conformance_v1",
    "record_vnpy_facade_repository_read_v1",
    "record_vnpy_facade_runtime_invocation_v1",
    "record_vnpy_facade_source_execution_v1",
]
