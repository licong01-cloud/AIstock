from __future__ import annotations

import ctypes
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Protocol

import psutil

from .errors import ProfileValidationError
from .profile import DatasetProfile, ResourcePolicy as ProfileResourcePolicy, validate_resource_policy


GIB = 1024**3
MIB = 1024**2


class ResourceTelemetryUnavailable(RuntimeError):
    """Raised when mandatory host telemetry cannot be collected."""


@dataclass(frozen=True)
class PressureRung:
    h5_batch: int
    minute_batch: int
    chunk_months: int
    row_group_rows: int
    dump_workers: int


_PRESSURE_RATIO = 0.85
_COMMIT_PRESSURE_SUSTAINED_SAMPLES = 2
_TELEMETRY_CAPACITY = 7_200


class ResourceAdmissionClass(str, Enum):
    """Approved workload classes for start-reserve admission only."""

    FULL = "full"
    SAMPLE = "sample"
    RESOLUTION_LIGHT = "resolution_light"


@dataclass(frozen=True, slots=True)
class ResourceAdmissionThresholds:
    host_start_available_bytes: int
    host_start_commit_headroom_bytes: int
    wsl_start_available_bytes: int


def _admission_thresholds(
    policy: ProfileResourcePolicy,
    admission_class: ResourceAdmissionClass,
) -> ResourceAdmissionThresholds:
    if not isinstance(admission_class, ResourceAdmissionClass):
        raise ProfileValidationError("resource admission class is not approved")

    if admission_class is ResourceAdmissionClass.FULL:
        available = policy.host_start_available_bytes
        headroom = policy.host_start_commit_headroom_bytes
    elif admission_class is ResourceAdmissionClass.SAMPLE:
        approved_floor = 12 * GIB
        available = min(
            policy.host_start_available_bytes,
            max(approved_floor, policy.host_emergency_available_bytes + GIB),
        )
        headroom = min(
            policy.host_start_commit_headroom_bytes,
            max(approved_floor, policy.host_emergency_commit_headroom_bytes + GIB),
        )
    elif admission_class is ResourceAdmissionClass.RESOLUTION_LIGHT:
        approved_floor = 10 * GIB
        available = min(
            policy.host_start_available_bytes,
            max(approved_floor, policy.host_emergency_available_bytes + GIB),
        )
        headroom = min(
            policy.host_start_commit_headroom_bytes,
            max(approved_floor, policy.host_emergency_commit_headroom_bytes + GIB),
        )
    else:  # pragma: no cover - enum exhaustiveness guard for future changes
        raise ProfileValidationError("resource admission class lacks user-approved thresholds")
    return ResourceAdmissionThresholds(
        host_start_available_bytes=available,
        host_start_commit_headroom_bytes=headroom,
        wsl_start_available_bytes=policy.wsl_start_available_bytes,
    )


def _project_pressure_ladder(profile: DatasetProfile) -> tuple[PressureRung, ...]:
    """Project the immutable profile ladder through its effective hard policy.

    The YAML profile remains the only ladder/configuration authority. Runtime
    overrides may only tighten a physical unit, so each rung is capped by the
    corresponding effective ResourcePolicy field and can never re-expand it.
    """

    sources = {
        "h5_batch": profile.pressure_ladder.get("h5_batch"),
        "minute_batch": profile.pressure_ladder.get("minute_batch"),
        "date_chunk_months": profile.pressure_ladder.get("date_chunk_months"),
        "row_group_rows": profile.pressure_ladder.get("row_group_rows"),
        "dump_workers": profile.pressure_ladder.get("dump_workers"),
    }
    for name, values in sources.items():
        if not values or any(int(value) <= 0 for value in values):
            raise ProfileValidationError(f"pressure ladder {name} must be non-empty and positive")
        if any(int(later) > int(earlier) for earlier, later in zip(values, values[1:])):
            raise ProfileValidationError(f"pressure ladder {name} must be non-increasing")

    policy = validate_resource_policy(profile.resource_policy)
    caps = {
        "h5_batch": policy.h5_load_batch_size,
        "minute_batch": policy.minute_code_batch_size,
        "date_chunk_months": policy.date_chunk_months,
        "row_group_rows": policy.parquet_row_group_rows,
        "dump_workers": policy.qlib_dump_workers,
    }

    def value_at(name: str, index: int) -> int:
        values = sources[name]
        assert values is not None
        return min(int(values[min(index, len(values) - 1)]), int(caps[name]))

    rung_count = max(len(values) for values in sources.values() if values is not None)
    return tuple(
        PressureRung(
            h5_batch=value_at("h5_batch", index),
            minute_batch=value_at("minute_batch", index),
            chunk_months=value_at("date_chunk_months", index),
            row_group_rows=value_at("row_group_rows", index),
            dump_workers=value_at("dump_workers", index),
        )
        for index in range(rung_count)
    )


@dataclass(frozen=True)
class HostMemorySnapshot:
    observed_monotonic: float
    available_bytes: int
    commit_total_bytes: int | None
    commit_limit_bytes: int | None
    pagefile_used_bytes: int
    pagefile_limit_bytes: int
    page_reads_per_second: float | None
    low_memory_signaled: bool | None

    @property
    def commit_headroom_bytes(self) -> int | None:
        if self.commit_total_bytes is None or self.commit_limit_bytes is None:
            return None
        return max(0, self.commit_limit_bytes - self.commit_total_bytes)


@dataclass(frozen=True)
class OwnedMemorySnapshot:
    windows_job_commit_bytes: int
    wsl_cgroup_current_bytes: int
    windows_tree_rss_bytes: int = 0
    wsl_tree_rss_bytes: int = 0
    windows_job_peak_commit_bytes: int = 0
    wsl_cgroup_peak_bytes: int = 0
    windows_tree_peak_rss_bytes: int = 0
    wsl_tree_peak_rss_bytes: int = 0

    @property
    def aggregate_commit_bytes(self) -> int:
        return self.windows_job_commit_bytes + self.wsl_cgroup_current_bytes


@dataclass(frozen=True)
class ResourceSnapshot:
    stage: str
    host: HostMemorySnapshot
    owned: OwnedMemorySnapshot
    wsl_available_bytes: int | None = None
    wsl_required: bool = False


@dataclass(frozen=True)
class ResourceDecision:
    status: str
    reason_code: str
    pressure_rung: int
    hard_failure: bool = False
    warning_codes: tuple[str, ...] = ()


class SnapshotProbe(Protocol):
    def __call__(self, stage: str, wsl_required: bool) -> ResourceSnapshot: ...


class _PerformanceInformation(ctypes.Structure):
    _fields_ = [
        ("cb", ctypes.c_ulong),
        ("CommitTotal", ctypes.c_size_t),
        ("CommitLimit", ctypes.c_size_t),
        ("CommitPeak", ctypes.c_size_t),
        ("PhysicalTotal", ctypes.c_size_t),
        ("PhysicalAvailable", ctypes.c_size_t),
        ("SystemCache", ctypes.c_size_t),
        ("KernelTotal", ctypes.c_size_t),
        ("KernelPaged", ctypes.c_size_t),
        ("KernelNonpaged", ctypes.c_size_t),
        ("PageSize", ctypes.c_size_t),
        ("HandleCount", ctypes.c_ulong),
        ("ProcessCount", ctypes.c_ulong),
        ("ThreadCount", ctypes.c_ulong),
    ]


class _PdhCounterValueUnion(ctypes.Union):
    _fields_ = [
        ("long_value", ctypes.c_long),
        ("double_value", ctypes.c_double),
        ("large_value", ctypes.c_longlong),
        ("ansi_string", ctypes.c_char_p),
        ("wide_string", ctypes.c_wchar_p),
    ]


class _PdhFormattedCounterValue(ctypes.Structure):
    _fields_ = [
        ("status", ctypes.c_ulong),
        ("value", _PdhCounterValueUnion),
    ]


class _WindowsPageReadsCounter:
    """Exact ``\\Memory\\Page Reads/sec`` PDH counter with a primed baseline."""

    _PDH_FMT_DOUBLE = 0x00000200
    _PDH_VALID_DATA = 0x00000000
    _PDH_NEW_DATA = 0x00000001

    def __init__(
        self,
        *,
        sleep: Callable[[float], None] = time.sleep,
        prime_seconds: float = 1.0,
    ) -> None:
        if os.name != "nt":
            raise ResourceTelemetryUnavailable("Windows PDH telemetry requires Windows")
        try:
            self._pdh = ctypes.WinDLL("pdh", use_last_error=True)
            self._query = ctypes.c_void_p()
            self._counter = ctypes.c_void_p()
            self._pdh.PdhOpenQueryW.argtypes = (
                ctypes.c_wchar_p,
                ctypes.c_size_t,
                ctypes.POINTER(ctypes.c_void_p),
            )
            self._pdh.PdhOpenQueryW.restype = ctypes.c_ulong
            add_counter = self._pdh.PdhAddEnglishCounterW
            add_counter.argtypes = (
                ctypes.c_void_p,
                ctypes.c_wchar_p,
                ctypes.c_size_t,
                ctypes.POINTER(ctypes.c_void_p),
            )
            add_counter.restype = ctypes.c_ulong
            self._pdh.PdhCollectQueryData.argtypes = (ctypes.c_void_p,)
            self._pdh.PdhCollectQueryData.restype = ctypes.c_ulong
            self._pdh.PdhGetFormattedCounterValue.argtypes = (
                ctypes.c_void_p,
                ctypes.c_ulong,
                ctypes.POINTER(ctypes.c_ulong),
                ctypes.POINTER(_PdhFormattedCounterValue),
            )
            self._pdh.PdhGetFormattedCounterValue.restype = ctypes.c_ulong
            self._pdh.PdhCloseQuery.argtypes = (ctypes.c_void_p,)
            self._pdh.PdhCloseQuery.restype = ctypes.c_ulong
            if self._pdh.PdhOpenQueryW(None, 0, ctypes.byref(self._query)) != 0:
                raise ResourceTelemetryUnavailable("PdhOpenQueryW failed")
            if (
                add_counter(
                    self._query,
                    r"\Memory\Page Reads/sec",
                    0,
                    ctypes.byref(self._counter),
                )
                != 0
            ):
                raise ResourceTelemetryUnavailable("Page Reads/sec counter is unavailable")
            if self._pdh.PdhCollectQueryData(self._query) != 0:
                raise ResourceTelemetryUnavailable("initial PDH collection failed")
            sleep(max(0.0, float(prime_seconds)))
        except (AttributeError, OSError) as exc:  # pragma: no cover - platform dependent
            self.close()
            raise ResourceTelemetryUnavailable("Windows PDH telemetry is unavailable") from exc

    def __call__(self) -> float:
        if not getattr(self, "_query", None):
            raise ResourceTelemetryUnavailable("PDH query is closed")
        if self._pdh.PdhCollectQueryData(self._query) != 0:
            raise ResourceTelemetryUnavailable("PDH collection failed")
        counter_type = ctypes.c_ulong()
        value = _PdhFormattedCounterValue()
        status = self._pdh.PdhGetFormattedCounterValue(
            self._counter,
            self._PDH_FMT_DOUBLE,
            ctypes.byref(counter_type),
            ctypes.byref(value),
        )
        if status != 0 or value.status not in {
            self._PDH_VALID_DATA,
            self._PDH_NEW_DATA,
        }:
            raise ResourceTelemetryUnavailable("Page Reads/sec PDH value is invalid")
        result = float(value.value.double_value)
        if result < 0:
            raise ResourceTelemetryUnavailable("Page Reads/sec PDH value is negative")
        return result

    def close(self) -> None:
        query = getattr(self, "_query", None)
        if query and getattr(query, "value", None):
            try:
                self._pdh.PdhCloseQuery(query)
            finally:
                query.value = None


class _WindowsLowMemoryProbe:
    """Own and query a Win32 LowMemoryResourceNotification handle."""

    _LOW_MEMORY_RESOURCE_NOTIFICATION = 0

    def __init__(self) -> None:
        if os.name != "nt":
            raise ResourceTelemetryUnavailable("low-memory notification requires Windows")
        try:
            self._kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            self._kernel32.CreateMemoryResourceNotification.argtypes = (ctypes.c_int,)
            self._kernel32.CreateMemoryResourceNotification.restype = ctypes.c_void_p
            self._kernel32.QueryMemoryResourceNotification.argtypes = (
                ctypes.c_void_p,
                ctypes.POINTER(ctypes.c_int),
            )
            self._kernel32.QueryMemoryResourceNotification.restype = ctypes.c_int
            self._kernel32.CloseHandle.argtypes = (ctypes.c_void_p,)
            self._kernel32.CloseHandle.restype = ctypes.c_int
            self._handle = self._kernel32.CreateMemoryResourceNotification(self._LOW_MEMORY_RESOURCE_NOTIFICATION)
        except (AttributeError, OSError) as exc:  # pragma: no cover - platform dependent
            raise ResourceTelemetryUnavailable("LowMemoryResourceNotification is unavailable") from exc
        if not self._handle:
            raise ResourceTelemetryUnavailable("CreateMemoryResourceNotification failed")

    def __call__(self) -> bool:
        state = ctypes.c_int()
        if not self._kernel32.QueryMemoryResourceNotification(self._handle, ctypes.byref(state)):
            raise ResourceTelemetryUnavailable("QueryMemoryResourceNotification failed")
        return bool(state.value)

    def close(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle:
            try:
                self._kernel32.CloseHandle(handle)
            finally:
                self._handle = None


class HostTelemetrySampler:
    """Production host sampler with explicit optional system telemetry.

    Dataset-release-owned counters are collected by the supervised Job/cgroup
    probe.  Host-wide commit, paging and low-memory counters are observational;
    an unavailable optional counter is represented as ``None`` and surfaced in
    the resource receipt instead of preventing a monthly release.
    """

    def __init__(
        self,
        *,
        virtual_memory: Callable[[], object] = psutil.virtual_memory,
        swap_memory: Callable[[], object] = psutil.swap_memory,
        commit_probe: Callable[[], tuple[int | None, int | None]] = lambda: _windows_commit_bytes(),
        page_reads_probe: Callable[[], float] | None = None,
        low_memory_probe: Callable[[], bool] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        prime_sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._virtual_memory = virtual_memory
        self._swap_memory = swap_memory
        self._commit_probe = commit_probe
        self._monotonic = monotonic
        self._owned_page_probe = None
        self._owned_low_probe = None
        try:
            if page_reads_probe is None:
                self._owned_page_probe = _WindowsPageReadsCounter(sleep=prime_sleep)
        except Exception:
            self._owned_page_probe = None
        try:
            if low_memory_probe is None:
                self._owned_low_probe = _WindowsLowMemoryProbe()
        except Exception:
            self._owned_low_probe = None
        self._page_reads_probe = page_reads_probe or self._owned_page_probe
        self._low_memory_probe = low_memory_probe or self._owned_low_probe
        self._lock = threading.Lock()

    def __call__(self) -> HostMemorySnapshot:
        with self._lock:
            try:
                virtual = self._virtual_memory()
                swap = self._swap_memory()
                available = int(getattr(virtual, "available"))
                pagefile_used = int(getattr(swap, "used"))
                pagefile_limit = int(getattr(swap, "total"))
            except ResourceTelemetryUnavailable:
                raise
            except (AttributeError, OSError, TypeError, ValueError) as exc:
                raise ResourceTelemetryUnavailable("mandatory host resource telemetry is unavailable") from exc
            try:
                commit_total, commit_limit = self._commit_probe()
            except (ResourceTelemetryUnavailable, AttributeError, OSError, TypeError, ValueError):
                commit_total, commit_limit = None, None
            try:
                page_reads = float(self._page_reads_probe()) if self._page_reads_probe is not None else None
            except (ResourceTelemetryUnavailable, AttributeError, OSError, TypeError, ValueError):
                page_reads = None
            try:
                raw_low_memory = self._low_memory_probe() if self._low_memory_probe is not None else None
                low_memory = raw_low_memory if type(raw_low_memory) is bool else None
            except (ResourceTelemetryUnavailable, AttributeError, OSError, TypeError, ValueError):
                low_memory = None
        if (
            available < 0
            or pagefile_used < 0
            or pagefile_limit < 0
            or pagefile_used > pagefile_limit
            or (commit_total is None) != (commit_limit is None)
            or (commit_total is not None and commit_total < 0)
            or (commit_limit is not None and commit_limit <= 0)
            or (
                commit_total is not None
                and commit_limit is not None
                and commit_total > commit_limit
            )
            or (page_reads is not None and page_reads < 0)
        ):
            raise ResourceTelemetryUnavailable("mandatory host resource telemetry violates its contract")
        return HostMemorySnapshot(
            observed_monotonic=self._monotonic(),
            available_bytes=available,
            commit_total_bytes=(int(commit_total) if commit_total is not None else None),
            commit_limit_bytes=(int(commit_limit) if commit_limit is not None else None),
            pagefile_used_bytes=pagefile_used,
            pagefile_limit_bytes=pagefile_limit,
            page_reads_per_second=page_reads,
            low_memory_signaled=low_memory,
        )

    def close(self) -> None:
        for owned in (self._owned_page_probe, self._owned_low_probe):
            if owned is not None:
                owned.close()


def _windows_commit_bytes() -> tuple[int | None, int | None]:
    if os.name != "nt":
        return None, None
    info = _PerformanceInformation()
    info.cb = ctypes.sizeof(info)
    try:
        ok = ctypes.windll.psapi.GetPerformanceInfo(ctypes.byref(info), info.cb)
    except (AttributeError, OSError) as exc:  # pragma: no cover - platform dependent
        raise ResourceTelemetryUnavailable("GetPerformanceInfo is unavailable") from exc
    if not ok or not info.PageSize:
        raise ResourceTelemetryUnavailable("GetPerformanceInfo failed")
    return int(info.CommitTotal * info.PageSize), int(info.CommitLimit * info.PageSize)


def probe_host_memory(
    *,
    monotonic: Callable[[], float] = time.monotonic,
    page_reads_per_second: float | None = None,
    low_memory_signaled: bool | None = None,
) -> HostMemorySnapshot:
    virtual = psutil.virtual_memory()
    swap = psutil.swap_memory()
    commit_total, commit_limit = _windows_commit_bytes()
    return HostMemorySnapshot(
        observed_monotonic=monotonic(),
        available_bytes=int(virtual.available),
        commit_total_bytes=commit_total,
        commit_limit_bytes=commit_limit,
        pagefile_used_bytes=int(swap.used),
        pagefile_limit_bytes=int(swap.total),
        page_reads_per_second=page_reads_per_second,
        low_memory_signaled=low_memory_signaled,
    )


class ResourceBudget:
    """Evaluate a frozen resource contract without weakening data scope.

    OS Job/cgroup objects enforce the continuous hard cap. This class supplies
    admission, emergency and deterministic pressure-ladder decisions.
    """

    def __init__(
        self,
        profile: DatasetProfile,
        probe: SnapshotProbe,
        *,
        admission_class: ResourceAdmissionClass = ResourceAdmissionClass.FULL,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.policy: ProfileResourcePolicy = validate_resource_policy(profile.resource_policy)
        self.admission_class = admission_class
        self.admission_thresholds = _admission_thresholds(self.policy, admission_class)
        self.pressure_ladder = _project_pressure_ladder(profile)
        self._probe = probe
        self._sleep = sleep
        self._monotonic = monotonic
        self._commit_pressure_samples = 0
        self._telemetry: deque[ResourceSnapshot] = deque(maxlen=_TELEMETRY_CAPACITY)

    @property
    def telemetry(self) -> tuple[ResourceSnapshot, ...]:
        return tuple(self._telemetry)

    def _record_pressure(self, snapshot: ResourceSnapshot) -> None:
        ratio = snapshot.owned.aggregate_commit_bytes / self.policy.aggregate_private_commit_bytes
        if ratio >= _PRESSURE_RATIO:
            self._commit_pressure_samples += 1
        else:
            self._commit_pressure_samples = 0

    def _system_warning_codes(self, snapshot: ResourceSnapshot) -> tuple[str, ...]:
        """Describe host-wide pressure without blocking a monthly release.

        These counters include unrelated databases, WSL workloads and model
        training.  They remain useful operational telemetry, but only the
        release-owned Job/cgroup counters may drive the pressure ladder.
        """

        warnings: list[str] = []
        headroom = snapshot.host.commit_headroom_bytes
        if snapshot.host.available_bytes < self.admission_thresholds.host_start_available_bytes:
            warnings.append("SYSTEM_AVAILABLE_MEMORY_LOW")
        if headroom is None:
            warnings.append("SYSTEM_COMMIT_TELEMETRY_UNAVAILABLE")
        elif headroom < self.admission_thresholds.host_start_commit_headroom_bytes:
            warnings.append("SYSTEM_COMMIT_HEADROOM_LOW")
        if snapshot.host.page_reads_per_second is None:
            warnings.append("SYSTEM_PAGE_READS_TELEMETRY_UNAVAILABLE")
        elif snapshot.host.page_reads_per_second >= 256.0:
            warnings.append("SYSTEM_PAGING_ACTIVITY_HIGH")
        if snapshot.host.pagefile_limit_bytes and (
            snapshot.host.pagefile_used_bytes / snapshot.host.pagefile_limit_bytes >= _PRESSURE_RATIO
        ):
            warnings.append("SYSTEM_PAGEFILE_USAGE_HIGH")
        if snapshot.host.low_memory_signaled is None:
            warnings.append("SYSTEM_LOW_MEMORY_SIGNAL_UNAVAILABLE")
        if snapshot.wsl_required:
            if snapshot.wsl_available_bytes is None:
                warnings.append("SYSTEM_WSL_AVAILABLE_TELEMETRY_UNAVAILABLE")
            elif snapshot.wsl_available_bytes < self.admission_thresholds.wsl_start_available_bytes:
                warnings.append("SYSTEM_WSL_AVAILABLE_MEMORY_LOW")
        return tuple(warnings)

    def _telemetry_complete(self, snapshot: ResourceSnapshot) -> bool:
        integer_counters = (
            snapshot.host.available_bytes,
            snapshot.host.pagefile_used_bytes,
            snapshot.host.pagefile_limit_bytes,
            snapshot.owned.windows_job_commit_bytes,
            snapshot.owned.wsl_cgroup_current_bytes,
            snapshot.owned.windows_tree_rss_bytes,
            snapshot.owned.wsl_tree_rss_bytes,
            snapshot.owned.windows_job_peak_commit_bytes,
            snapshot.owned.wsl_cgroup_peak_bytes,
            snapshot.owned.windows_tree_peak_rss_bytes,
            snapshot.owned.wsl_tree_peak_rss_bytes,
        )
        if any(type(value) is not int or value < 0 for value in integer_counters):
            return False
        commit_values = (
            snapshot.host.commit_total_bytes,
            snapshot.host.commit_limit_bytes,
        )
        if (commit_values[0] is None) != (commit_values[1] is None):
            return False
        if any(value is not None and (type(value) is not int or value < 0) for value in commit_values):
            return False
        if (
            commit_values[0] is not None
            and commit_values[1] is not None
            and (commit_values[1] <= 0 or commit_values[0] > commit_values[1])
        ):
            return False
        if snapshot.host.pagefile_used_bytes > snapshot.host.pagefile_limit_bytes:
            return False
        if snapshot.host.page_reads_per_second is not None and (
            isinstance(snapshot.host.page_reads_per_second, bool)
            or not isinstance(snapshot.host.page_reads_per_second, (int, float))
            or snapshot.host.page_reads_per_second < 0
        ):
            return False
        if snapshot.host.low_memory_signaled is not None and type(snapshot.host.low_memory_signaled) is not bool:
            return False
        if snapshot.wsl_available_bytes is not None and (
            type(snapshot.wsl_available_bytes) is not int or snapshot.wsl_available_bytes < 0
        ):
            return False
        return True

    def classify(self, snapshot: ResourceSnapshot, *, pressure_rung: int = 0) -> ResourceDecision:
        if not 0 <= pressure_rung < len(self.pressure_ladder):
            raise ValueError(f"pressure_rung is outside profile ladder: {pressure_rung}")
        self._telemetry.append(snapshot)
        if not self._telemetry_complete(snapshot):
            return ResourceDecision(
                "BLOCKED",
                "BLOCKED_REQUIRED_TELEMETRY_UNAVAILABLE",
                pressure_rung,
                hard_failure=True,
            )
        self._record_pressure(snapshot)
        warnings = self._system_warning_codes(snapshot)
        if snapshot.owned.aggregate_commit_bytes > self.policy.aggregate_private_commit_bytes:
            return ResourceDecision(
                "FAILED",
                "FAILED_RESOURCE_HARD_LIMIT",
                pressure_rung,
                hard_failure=True,
                warning_codes=warnings,
            )
        windows_limit = (
            self.policy.hybrid_job_commit_bytes if snapshot.wsl_required else self.policy.windows_job_commit_bytes
        )
        if snapshot.owned.windows_job_commit_bytes > windows_limit:
            return ResourceDecision(
                "FAILED",
                "FAILED_WINDOWS_JOB_COMMIT_LIMIT",
                pressure_rung,
                hard_failure=True,
                warning_codes=warnings,
            )
        if snapshot.owned.wsl_cgroup_current_bytes > self.policy.wsl_memory_max_bytes:
            return ResourceDecision(
                "FAILED",
                "FAILED_WSL_CGROUP_MEMORY_MAX",
                pressure_rung,
                hard_failure=True,
                warning_codes=warnings,
            )
        if snapshot.host.low_memory_signaled is True:
            return ResourceDecision(
                "WAITING_RESOURCE",
                "RESOURCE_OS_LOW_MEMORY_SIGNAL",
                pressure_rung,
                warning_codes=warnings,
            )

        commit_pressure = self._commit_pressure_samples >= _COMMIT_PRESSURE_SUSTAINED_SAMPLES
        if commit_pressure and pressure_rung < len(self.pressure_ladder) - 1:
            self._commit_pressure_samples = 0
            return ResourceDecision(
                "CHECKPOINT_PRESSURE",
                "RESOURCE_OWNED_COMMIT_PRESSURE_LADDER",
                pressure_rung + 1,
                warning_codes=warnings,
            )

        return ResourceDecision("READY", "RESOURCE_READY", pressure_rung, warning_codes=warnings)

    def wait_until_ready(self, stage: str, *, wsl_required: bool, pressure_rung: int = 0) -> ResourceDecision:
        started = self._monotonic()
        consecutive_ready = 0
        last: ResourceDecision | None = None
        while self._monotonic() - started <= self.policy.wait_deadline_seconds:
            snapshot = self._probe(stage, wsl_required)
            last = self.classify(snapshot, pressure_rung=pressure_rung)
            if last.hard_failure or last.status == "FAILED":
                return last
            if last.status == "CHECKPOINT_PRESSURE":
                return last
            if last.status == "READY":
                consecutive_ready += 1
                if consecutive_ready >= 2:
                    return last
            else:
                consecutive_ready = 0
            self._sleep(self.policy.enforcement_sample_seconds)
        if last is None:
            return ResourceDecision(
                "WAITING_RESOURCE",
                "RESOURCE_WAIT_DEADLINE_OBSERVED",
                pressure_rung,
                warning_codes=("RESOURCE_WAIT_DEADLINE_EXCEEDED",),
            )
        return ResourceDecision(
            last.status,
            last.reason_code,
            last.pressure_rung,
            hard_failure=False,
            warning_codes=tuple(dict.fromkeys((*last.warning_codes, "RESOURCE_WAIT_DEADLINE_EXCEEDED"))),
        )


__all__ = [
    "GIB",
    "MIB",
    "HostTelemetrySampler",
    "HostMemorySnapshot",
    "OwnedMemorySnapshot",
    "PressureRung",
    "ResourceAdmissionClass",
    "ResourceAdmissionThresholds",
    "ResourceBudget",
    "ResourceDecision",
    "ResourceSnapshot",
    "ResourceTelemetryUnavailable",
    "probe_host_memory",
]
