"""Runtime resource admission/checkpoint contract for dataset stage children.

The control Worker never owns a data panel.  It uses this gate to admit an
attempt from host telemetry, then binds the task-owned Job/cgroup probe before
launching a data-bearing child.  Pressure only changes physical chunk rungs;
it never changes instruments, dates, fields, validators, or data scope.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import stat
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol

from .errors import DatasetReleaseError
from .profile import DatasetProfile
from .resource_budget import (
    HostMemorySnapshot,
    OwnedMemorySnapshot,
    PressureRung,
    ResourceAdmissionClass,
    ResourceBudget,
    ResourceDecision,
    ResourceSnapshot,
    ResourceTelemetryUnavailable,
    probe_host_memory,
)


RESOURCE_GATE_RECEIPT_SCHEMA = "dataset_release_resource_gate_receipt_v1"
RESOURCE_CHECKPOINT_SIGNAL_SCHEMA = "dataset_release_resource_checkpoint_signal_v1"
RESOURCE_CHECKPOINT_ENV = "DATASET_RESOURCE_CHECKPOINT_FILE"
_SIGNAL_LIMIT_BYTES = 64 * 1024
_IDENTITY = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")


class ResourceGateError(DatasetReleaseError):
    code = "BLOCKED_RESOURCE_GATE"


class ResourceCheckpointRequested(ResourceGateError):
    code = "WAITING_RESOURCE"
    retryable = True


class RequiredResourceTelemetryUnavailable(ResourceGateError):
    code = "BLOCKED_REQUIRED_TELEMETRY_UNAVAILABLE"


@dataclass(frozen=True, slots=True)
class DiskSpaceSnapshot:
    control_free_bytes: int
    candidate_free_bytes: int
    effective_free_bytes: int
    required_free_bytes: int
    predicted_remaining_new_bytes: int | None
    same_volume: bool


DiskProbe = Callable[[int | None], DiskSpaceSnapshot]


class DiskSpaceGuard:
    """Same-volume X-root reserve gate shared by parent and chunk children."""

    def __init__(
        self,
        profile: DatasetProfile,
        *,
        disk_usage: Callable[[Path], object] = shutil.disk_usage,
        volume_probe: Callable[[Path], object] = lambda path: (
            path.anchor.casefold(),
            path.stat().st_dev,
        ),
    ) -> None:
        self.policy = profile.resource_policy
        try:
            self.control_root = Path(profile.control_root).resolve(strict=True)
            self.candidate_root = Path(profile.candidate_root).resolve(strict=True)
            _assert_plain_existing_chain(self.control_root)
            _assert_plain_existing_chain(self.candidate_root)
        except (OSError, ResourceGateError) as exc:
            raise RequiredResourceTelemetryUnavailable("control/candidate storage roots are unavailable") from exc
        self._disk_usage = disk_usage
        self._volume_probe = volume_probe

    def sample(self, predicted_remaining_new_bytes: int | None = None) -> DiskSpaceSnapshot:
        if predicted_remaining_new_bytes is not None and (
            type(predicted_remaining_new_bytes) is not int or predicted_remaining_new_bytes < 0
        ):
            raise ResourceGateError("predicted remaining bytes are invalid")
        required = int(self.policy.candidate_free_space_floor_bytes)
        if predicted_remaining_new_bytes is not None:
            required = max(
                required,
                math.ceil(predicted_remaining_new_bytes * self.policy.predicted_new_bytes_multiplier),
            )
        try:
            control_usage = self._disk_usage(self.control_root)
            candidate_usage = self._disk_usage(self.candidate_root)
            control_free = int(getattr(control_usage, "free"))
            candidate_free = int(getattr(candidate_usage, "free"))
            same_volume = self._volume_probe(self.control_root) == self._volume_probe(self.candidate_root)
        except (AttributeError, OSError, TypeError, ValueError) as exc:
            raise RequiredResourceTelemetryUnavailable("control/candidate free-space telemetry is unavailable") from exc
        if control_free < 0 or candidate_free < 0:
            raise RequiredResourceTelemetryUnavailable("control/candidate free-space telemetry is invalid")
        return DiskSpaceSnapshot(
            control_free_bytes=control_free,
            candidate_free_bytes=candidate_free,
            effective_free_bytes=min(control_free, candidate_free),
            required_free_bytes=required,
            predicted_remaining_new_bytes=predicted_remaining_new_bytes,
            same_volume=bool(same_volume),
        )

    def checkpoint(self, predicted_remaining_new_bytes: int | None = None) -> DiskSpaceSnapshot:
        snapshot = self.sample(predicted_remaining_new_bytes)
        if not snapshot.same_volume:
            raise ResourceGateError(
                "control/candidate roots are not on one storage volume",
                code="BLOCKED_STORAGE_VOLUME_MISMATCH",
            )
        if snapshot.effective_free_bytes < snapshot.required_free_bytes:
            raise ResourceCheckpointRequested(
                "candidate storage reserve requires a safe chunk checkpoint",
                context={
                    "reason_code": "RESOURCE_DISK_RESERVE",
                    "disk_free_bytes": snapshot.effective_free_bytes,
                    "disk_required_free_bytes": snapshot.required_free_bytes,
                    "predicted_remaining_new_bytes": (snapshot.predicted_remaining_new_bytes),
                    "data_scope_changed": False,
                },
            )
        return snapshot


@dataclass(frozen=True, slots=True)
class WslRuntimeSnapshot:
    memory_current_bytes: int
    memory_peak_bytes: int
    memory_high_bytes: int
    memory_max_bytes: int
    swap_current_bytes: int
    swap_max_bytes: int
    available_bytes: int
    memory_events: Mapping[str, int]
    control_group: str = ""
    memory_oom_group: int = 1
    counter: int = 0
    observed_utc: str = ""
    wrapper_pid: int = 0

    def __post_init__(self) -> None:
        integers = (
            self.memory_current_bytes,
            self.memory_peak_bytes,
            self.memory_high_bytes,
            self.memory_max_bytes,
            self.swap_current_bytes,
            self.swap_max_bytes,
            self.available_bytes,
            self.memory_oom_group,
            self.counter,
            self.wrapper_pid,
        )
        if any(type(value) is not int or value < 0 for value in integers):
            raise ResourceGateError("WSL runtime telemetry contains invalid counters")
        if (
            self.memory_high_bytes <= 0
            or self.memory_max_bytes <= 0
            or self.memory_high_bytes >= self.memory_max_bytes
            or self.memory_current_bytes > self.memory_max_bytes
            or self.swap_current_bytes > self.swap_max_bytes
            or self.memory_oom_group != 1
            or len(self.memory_events) > 64
            or any(
                not str(key).strip() or len(str(key)) > 100 or type(value) is not int or value < 0
                for key, value in self.memory_events.items()
            )
        ):
            raise ResourceGateError("WSL runtime telemetry violates the cgroup contract")


@dataclass(frozen=True, slots=True)
class OwnedRuntimeSnapshot:
    windows_job_commit_bytes: int
    windows_job_peak_commit_bytes: int
    windows_tree_rss_bytes: int
    windows_tree_peak_rss_bytes: int
    active_processes: int
    wsl: WslRuntimeSnapshot | None = None

    def __post_init__(self) -> None:
        values = (
            self.windows_job_commit_bytes,
            self.windows_job_peak_commit_bytes,
            self.windows_tree_rss_bytes,
            self.windows_tree_peak_rss_bytes,
            self.active_processes,
        )
        if any(type(value) is not int or value < 0 for value in values):
            raise ResourceGateError("owned runtime telemetry contains invalid counters")


class OwnedRuntimeProbe(Protocol):
    def __call__(self, wsl_required: bool) -> OwnedRuntimeSnapshot: ...


HostProbe = Callable[[], HostMemorySnapshot]


@dataclass(frozen=True, slots=True)
class ResourceGateSample:
    decision: ResourceDecision
    snapshot: ResourceSnapshot
    owned_runtime: OwnedRuntimeSnapshot
    disk: DiskSpaceSnapshot
    admission_class: ResourceAdmissionClass
    effective_host_start_available_bytes: int
    effective_host_start_commit_headroom_bytes: int


class ResourceGate:
    """One attempt-scoped gate with bounded telemetry and deterministic rungs."""

    def __init__(
        self,
        profile: DatasetProfile,
        *,
        host_probe: HostProbe = probe_host_memory,
        disk_probe: DiskProbe | None = None,
        predicted_new_bytes: int | None = None,
        admission_class: ResourceAdmissionClass = ResourceAdmissionClass.FULL,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.profile = profile
        self.policy = profile.resource_policy
        self.admission_class = admission_class
        self._host_probe = host_probe
        if predicted_new_bytes is not None and (type(predicted_new_bytes) is not int or predicted_new_bytes < 0):
            raise ResourceGateError("predicted new bytes are invalid")
        self._predicted_new_bytes = predicted_new_bytes
        if disk_probe is None:
            disk_guard = DiskSpaceGuard(profile)
            self._disk_probe = disk_guard.sample
        else:
            self._disk_probe = disk_probe
        self._owned_probe: OwnedRuntimeProbe | None = None
        self._sleep = sleep
        self._budget = ResourceBudget(
            profile,
            self._probe,
            admission_class=admission_class,
            sleep=sleep,
            monotonic=monotonic,
        )
        self._samples: deque[ResourceGateSample] = deque(maxlen=7_200)
        self._sample_count = 0
        self._last_owned = OwnedRuntimeSnapshot(0, 0, 0, 0, 0, None)

    @property
    def pressure_ladder(self) -> tuple[PressureRung, ...]:
        return self._budget.pressure_ladder

    def bind_owned_probe(self, probe: OwnedRuntimeProbe) -> None:
        if self._owned_probe is not None:
            raise ResourceGateError("owned runtime probe is already bound")
        self._owned_probe = probe

    def admit(
        self,
        stage: str,
        *,
        wsl_required: bool,
        pressure_rung: int,
    ) -> ResourceGateSample:
        """Require two ready samples; a non-ready sample returns immediately."""

        first = self._sample(
            stage,
            wsl_required=wsl_required,
            pressure_rung=pressure_rung,
            predicted_remaining_new_bytes=self._predicted_new_bytes,
        )
        if first.decision.status != "READY":
            return first
        self._sleep(self.policy.enforcement_sample_seconds)
        return self._sample(
            stage,
            wsl_required=wsl_required,
            pressure_rung=pressure_rung,
            predicted_remaining_new_bytes=self._predicted_new_bytes,
        )

    def sample(
        self,
        stage: str,
        *,
        wsl_required: bool,
        pressure_rung: int,
    ) -> ResourceGateSample:
        return self._sample(
            stage,
            wsl_required=wsl_required,
            pressure_rung=pressure_rung,
            predicted_remaining_new_bytes=None,
        )

    def _sample(
        self,
        stage: str,
        *,
        wsl_required: bool,
        pressure_rung: int,
        predicted_remaining_new_bytes: int | None,
    ) -> ResourceGateSample:
        if not _IDENTITY.fullmatch(str(stage)):
            raise ResourceGateError("resource stage identity is invalid")
        snapshot = self._probe(str(stage), bool(wsl_required))
        decision = self._budget.classify(snapshot, pressure_rung=pressure_rung)
        try:
            disk = self._disk_probe(predicted_remaining_new_bytes)
        except (ResourceTelemetryUnavailable, OSError, ValueError) as exc:
            raise RequiredResourceTelemetryUnavailable("mandatory disk resource telemetry is unavailable") from exc
        if not isinstance(disk, DiskSpaceSnapshot):
            raise RequiredResourceTelemetryUnavailable("disk resource telemetry has an invalid type")
        if not disk.same_volume:
            decision = ResourceDecision(
                "BLOCKED",
                "BLOCKED_STORAGE_VOLUME_MISMATCH",
                pressure_rung,
                hard_failure=True,
            )
        elif disk.effective_free_bytes < disk.required_free_bytes:
            decision = ResourceDecision(
                "WAITING_RESOURCE",
                "RESOURCE_DISK_RESERVE",
                pressure_rung,
            )
        sample = ResourceGateSample(
            decision=decision,
            snapshot=snapshot,
            owned_runtime=self._last_owned,
            disk=disk,
            admission_class=self.admission_class,
            effective_host_start_available_bytes=(
                self._budget.admission_thresholds.host_start_available_bytes
            ),
            effective_host_start_commit_headroom_bytes=(
                self._budget.admission_thresholds.host_start_commit_headroom_bytes
            ),
        )
        self._samples.append(sample)
        self._sample_count += 1
        return sample

    def rung(self, index: int) -> PressureRung:
        try:
            return self.pressure_ladder[int(index)]
        except (IndexError, TypeError, ValueError) as exc:
            raise ResourceGateError("pressure rung is outside the profile contract") from exc

    def receipt(self) -> dict[str, object]:
        if not self._samples:
            raise ResourceGateError("resource receipt requires at least one sample")
        snapshots = [item.snapshot for item in self._samples]
        heads = [item.host.commit_headroom_bytes for item in snapshots]
        headrooms = [value for value in heads if value is not None]
        final = self._samples[-1]
        next_pressure_rung = max(item.decision.pressure_rung for item in self._samples)
        return {
            "schema_version": RESOURCE_GATE_RECEIPT_SCHEMA,
            "sample_count": self._sample_count,
            "retained_sample_count": len(self._samples),
            "final_status": final.decision.status,
            "final_reason_code": final.decision.reason_code,
            "admission_class": self.admission_class.value,
            "effective_host_start_available_bytes": (
                self._budget.admission_thresholds.host_start_available_bytes
            ),
            "effective_host_start_commit_headroom_bytes": (
                self._budget.admission_thresholds.host_start_commit_headroom_bytes
            ),
            "checkpoint_requested": any(item.decision.status not in {"READY"} for item in self._samples),
            "pressure_rung": final.decision.pressure_rung,
            "next_pressure_rung": next_pressure_rung,
            "pressure_settings": asdict(self.rung(next_pressure_rung)),
            "host_min_available_bytes": min(item.host.available_bytes for item in snapshots),
            "host_min_commit_headroom_bytes": min(headrooms) if headrooms else None,
            "host_low_memory_observed": any(item.host.low_memory_signaled is True for item in snapshots),
            "host_peak_page_reads_per_second": max(
                (
                    float(item.host.page_reads_per_second)
                    for item in snapshots
                    if item.host.page_reads_per_second is not None
                ),
                default=None,
            ),
            "pagefile_peak_used_bytes": max(item.host.pagefile_used_bytes for item in snapshots),
            "pagefile_limit_bytes": min(item.host.pagefile_limit_bytes for item in snapshots),
            "disk_min_effective_free_bytes": min(item.disk.effective_free_bytes for item in self._samples),
            "disk_max_required_free_bytes": max(item.disk.required_free_bytes for item in self._samples),
            "disk_same_volume": all(item.disk.same_volume for item in self._samples),
            "predicted_new_bytes": self._predicted_new_bytes,
            "windows_job_peak_commit_bytes": max(
                max(
                    item.owned.windows_job_commit_bytes,
                    item.owned.windows_job_peak_commit_bytes,
                )
                for item in snapshots
            ),
            "windows_tree_peak_rss_bytes": max(
                max(
                    item.owned.windows_tree_rss_bytes,
                    item.owned.windows_tree_peak_rss_bytes,
                )
                for item in snapshots
            ),
            "wsl_cgroup_peak_current_bytes": max(
                max(item.owned.wsl_cgroup_current_bytes, item.owned.wsl_cgroup_peak_bytes) for item in snapshots
            ),
            "aggregate_owned_peak_commit_bytes": max(item.owned.aggregate_commit_bytes for item in snapshots),
            "wsl_required": any(item.wsl_required for item in snapshots),
            "wsl_min_available_bytes": min(
                (
                    item.owned_runtime.wsl.available_bytes
                    for item in self._samples
                    if item.owned_runtime.wsl is not None
                ),
                default=None,
            ),
            "wsl_memory_high_bytes": _single_wsl_value(list(self._samples), "memory_high_bytes"),
            "wsl_memory_max_bytes": _single_wsl_value(list(self._samples), "memory_max_bytes"),
            "wsl_swap_max_bytes": _single_wsl_value(list(self._samples), "swap_max_bytes"),
            "wsl_peak_swap_current_bytes": max(
                (
                    item.owned_runtime.wsl.swap_current_bytes
                    for item in self._samples
                    if item.owned_runtime.wsl is not None
                ),
                default=0,
            ),
            "wsl_memory_events": _peak_wsl_events(list(self._samples)),
            "data_scope_changed": False,
        }

    def _probe(self, stage: str, wsl_required: bool) -> ResourceSnapshot:
        try:
            host = self._host_probe()
            owned = (
                self._owned_probe(wsl_required)
                if self._owned_probe is not None
                else OwnedRuntimeSnapshot(0, 0, 0, 0, 0, None)
            )
        except (ResourceTelemetryUnavailable, OSError, ValueError) as exc:
            raise RequiredResourceTelemetryUnavailable("mandatory resource telemetry is unavailable") from exc
        wsl = owned.wsl
        self._last_owned = owned
        return ResourceSnapshot(
            stage=stage,
            host=host,
            owned=OwnedMemorySnapshot(
                windows_job_commit_bytes=owned.windows_job_commit_bytes,
                wsl_cgroup_current_bytes=(wsl.memory_current_bytes if wsl else 0),
                windows_tree_rss_bytes=owned.windows_tree_rss_bytes,
                wsl_tree_rss_bytes=0,
                windows_job_peak_commit_bytes=owned.windows_job_peak_commit_bytes,
                wsl_cgroup_peak_bytes=(wsl.memory_peak_bytes if wsl else 0),
                windows_tree_peak_rss_bytes=owned.windows_tree_peak_rss_bytes,
                wsl_tree_peak_rss_bytes=0,
            ),
            wsl_available_bytes=(wsl.available_bytes if wsl else None),
            wsl_required=wsl_required,
        )


class ChildResourceCheckpoint:
    """Bounded read-only chunk boundary check used inside supervised stages."""

    def __init__(
        self,
        *,
        attempt_id: str,
        fence: int,
        execution_id: str,
        path: str | Path | None = None,
    ) -> None:
        if not _IDENTITY.fullmatch(str(attempt_id)) or not _IDENTITY.fullmatch(str(execution_id)) or int(fence) <= 0:
            raise ResourceGateError("child checkpoint identity is invalid")
        supplied = path or os.environ.get(RESOURCE_CHECKPOINT_ENV)
        if not supplied:
            raise ResourceGateError("child resource checkpoint path is missing")
        candidate = Path(supplied)
        if not candidate.is_absolute():
            raise ResourceGateError("child resource checkpoint path must be absolute")
        self.path = candidate
        self.attempt_id = str(attempt_id)
        self.fence = int(fence)
        self.execution_id = str(execution_id)

    def checkpoint(self) -> None:
        if not self.path.exists():
            return
        try:
            size = self.path.stat().st_size
            if not 0 < size <= _SIGNAL_LIMIT_BYTES:
                raise ResourceGateError("resource checkpoint signal size is invalid")
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ResourceGateError("resource checkpoint signal is unreadable") from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != RESOURCE_CHECKPOINT_SIGNAL_SCHEMA
            or payload.get("attempt_id") != self.attempt_id
            or int(payload.get("fence", 0)) != self.fence
            or payload.get("execution_id") != self.execution_id
        ):
            raise ResourceGateError("resource checkpoint signal identity mismatched")
        raise ResourceCheckpointRequested(
            "resource pressure requires a safe chunk checkpoint",
            context={
                "reason_code": str(payload.get("reason_code", "RESOURCE_PRESSURE")),
                "pressure_rung": int(payload.get("pressure_rung", 0)),
                "data_scope_changed": False,
            },
        )


def _single_wsl_value(samples: list[ResourceGateSample], field: str) -> int | None:
    values = {int(getattr(item.owned_runtime.wsl, field)) for item in samples if item.owned_runtime.wsl is not None}
    if len(values) > 1:
        raise ResourceGateError(f"WSL {field} changed during the supervised stage")
    return next(iter(values), None)


def _peak_wsl_events(samples: list[ResourceGateSample]) -> dict[str, int]:
    result: dict[str, int] = {}
    for sample in samples:
        if sample.owned_runtime.wsl is None:
            continue
        for key, value in sample.owned_runtime.wsl.memory_events.items():
            result[str(key)] = max(result.get(str(key), 0), int(value))
    return result


def _assert_plain_existing_chain(path: Path) -> None:
    if not path.is_absolute():
        raise ResourceGateError("storage root must be absolute")
    current = Path(path.parts[0])
    for part in path.parts[1:]:
        current = current / part
        metadata = current.lstat()
        attributes = int(getattr(metadata, "st_file_attributes", 0))
        reparse = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
        if stat.S_ISLNK(metadata.st_mode) or attributes & reparse:
            raise ResourceGateError("storage root contains a link/reparse point")


__all__ = [
    "ChildResourceCheckpoint",
    "DiskProbe",
    "DiskSpaceGuard",
    "DiskSpaceSnapshot",
    "OwnedRuntimeProbe",
    "OwnedRuntimeSnapshot",
    "RESOURCE_CHECKPOINT_ENV",
    "RESOURCE_CHECKPOINT_SIGNAL_SCHEMA",
    "RESOURCE_GATE_RECEIPT_SCHEMA",
    "ResourceCheckpointRequested",
    "ResourceGate",
    "ResourceGateError",
    "ResourceGateSample",
    "RequiredResourceTelemetryUnavailable",
    "WslRuntimeSnapshot",
]
