from __future__ import annotations

import hashlib
import errno
import json
import os
import re
import stat
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Sequence

from .cas_store import CASStore, CASStoreError
from .errors import DatasetReleaseError
from .profile import ResourcePolicy, validate_resource_policy
from .resource_budget import ResourceTelemetryUnavailable
from .resource_gate import (
    RESOURCE_CHECKPOINT_ENV,
    OwnedRuntimeSnapshot,
    ResourceGate,
    ResourceGateError,
    WslRuntimeSnapshot,
)
from .windows_job import JobAccounting, JobChild, WindowsJob
from .wsl_cgroup import WslCgroupReadback, WslCgroupService, WslUnitIdentity


class ResourceSupervisorError(RuntimeError):
    """Task-owned resource supervisor setup or lifecycle failure."""


class ControlRootCapacityExceeded(DatasetReleaseError):
    code = "CONTROL_ROOT_CAPACITY_EXCEEDED"


_EXECUTION_ID = re.compile(r"^[A-Za-z0-9_.-]{1,120}$")
_RUNNER_RESULT_SCHEMA = "dataset_supervised_runner_result_v1"
WSL_GUARDIAN_STATE_SCHEMA = "dataset_release_wsl_guardian_state_v1"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
_SEGMENT_LIMIT_BYTES = 16 * 1024**2
_RESULT_LIMIT_BYTES = 1024**2
_WSL_TELEMETRY_LIMIT_BYTES = 64 * 1024
SOURCE_CREDENTIAL_ENV_ALLOWLIST = frozenset(
    {
        "TUSHARE_TOKEN",
        "TDX_DB_HOST",
        "TDX_DB_PORT",
        "TDX_DB_USER",
        "TDX_DB_PASSWORD",
        "TDX_DB_NAME",
        "TDX_HTTP_PORT",
    }
)
_BASE_CHILD_ENV_KEYS = (
    "SystemRoot",
    "WINDIR",
    "ComSpec",
    "PATH",
    "PATHEXT",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "HOME",
    "LANG",
    "LC_ALL",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
)
_SAFE_CALLER_ENV_KEYS = frozenset(
    {
        "OMP_NUM_THREADS",
        "MKL_NUM_THREADS",
        "OPENBLAS_NUM_THREADS",
        "NUMEXPR_NUM_THREADS",
        "PYTHONIOENCODING",
        "PYTHONUTF8",
        "TZ",
    }
)


def build_supervised_environment(
    *,
    scope: Literal["source", "build", "validation"],
    credential_env_keys: Sequence[str] = (),
    overrides: Mapping[str, str] | None = None,
    source: Mapping[str, str] = os.environ,
) -> dict[str, str]:
    """Construct the only environment admitted to a data-bearing wrapper."""

    if scope not in {"source", "build", "validation"}:
        raise ResourceSupervisorError("supervised environment scope is invalid")
    credentials = tuple(str(key) for key in credential_env_keys)
    if len(set(credentials)) != len(credentials) or any(
        not re.fullmatch(r"[A-Z][A-Z0-9_]{0,99}", key) for key in credentials
    ):
        raise ResourceSupervisorError("credential environment key list is invalid")
    if scope != "source" and credentials:
        raise ResourceSupervisorError("non-source supervised child cannot receive business credentials")
    if set(credentials).difference(SOURCE_CREDENTIAL_ENV_ALLOWLIST):
        raise ResourceSupervisorError("source credential environment key is not allowlisted")
    result = {key: str(source[key]) for key in _BASE_CHILD_ENV_KEYS if key in source and "\x00" not in str(source[key])}
    for key in credentials:
        if key in source:
            value = str(source[key])
            if "\x00" in value:
                raise ResourceSupervisorError("source credential environment value is invalid")
            result[key] = value
    for raw_key, raw_value in (overrides or {}).items():
        key = str(raw_key)
        value = str(raw_value)
        if key not in _SAFE_CALLER_ENV_KEYS or "\x00" in value or len(value) > 32_767:
            raise ResourceSupervisorError("caller environment override is not allowlisted")
        result[key] = value
    result["PYTHONNOUSERSITE"] = "1"
    return result


def _atomic_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    temp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    handle = os.open(temp, flags, 0o600)
    try:
        with os.fdopen(handle, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.close(handle)
        handle = -1
        os.replace(temp, path)
        with path.open("rb") as stream:
            if stream.read() != data:
                raise ResourceSupervisorError("heartbeat readback mismatch")
    finally:
        if handle != -1:
            os.close(handle)
        if temp.exists():
            temp.unlink(missing_ok=True)


def _atomic_json_create(path: Path, payload: Mapping[str, object]) -> None:
    """Create one authority file without ever replacing an existing fence state."""

    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    temp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    handle = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(handle, "wb", closefd=False) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.close(handle)
        handle = -1
        try:
            os.link(temp, path)
        except FileExistsError as exc:
            raise ResourceSupervisorError("WSL guardian state appeared for attempt/fence") from exc
    finally:
        if handle != -1:
            os.close(handle)
        temp.unlink(missing_ok=True)


class HeartbeatWriter:
    def __init__(
        self,
        path: Path,
        *,
        attempt_id: str,
        fence: int,
        interval_seconds: float = 1.0,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        if not attempt_id or fence <= 0 or not 0 < interval_seconds <= 1:
            raise ResourceSupervisorError("heartbeat identity/interval is invalid")
        self.path = path
        self.attempt_id = attempt_id
        self.fence = int(fence)
        self.interval_seconds = float(interval_seconds)
        self._now = now
        self._counter = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    def write_once(self) -> None:
        self._counter += 1
        _atomic_json(
            self.path,
            {
                "schema_version": "dataset_resource_heartbeat_v1",
                "attempt_id": self.attempt_id,
                "fence": self.fence,
                "counter": self._counter,
                "host_utc": self._now().astimezone(timezone.utc).isoformat(),
            },
        )

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.write_once()
            except BaseException as exc:  # keep failure observable by owner
                self._error = exc
                self._stop.set()

    def start(self) -> None:
        if self._thread is not None:
            raise ResourceSupervisorError("heartbeat is already started")
        self.write_once()
        self._thread = threading.Thread(
            target=self._run,
            name=f"dataset-heartbeat-{self.attempt_id}",
            daemon=False,
        )
        self._thread.start()

    def assert_healthy(self) -> None:
        if self._error is not None:
            raise ResourceSupervisorError("resource heartbeat writer failed") from self._error
        if self._thread is None or not self._thread.is_alive():
            raise ResourceSupervisorError("resource heartbeat writer is not alive")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.interval_seconds * 3))
            if self._thread.is_alive():
                raise ResourceSupervisorError("resource heartbeat writer did not stop")


@dataclass(frozen=True)
class SupervisedChild:
    child: JobChild
    attempt_id: str
    fence: int
    wsl_unit: WslUnitIdentity | None = None
    wsl_readback: WslCgroupReadback | None = None


@dataclass(frozen=True)
class WslSupervisedOptions:
    distro: str
    guardian_python: str
    guardian_script_wsl: str
    heartbeat_path_wsl: str
    runner_python_wsl: str
    runner_script_wsl: str
    task_cwd_wsl: str
    execution_root_wsl: str


@dataclass(frozen=True)
class SupervisedExecutionReceipt:
    schema_version: str
    execution_id: str
    runtime: str
    environment_scope: str
    credential_env_keys: tuple[str, ...]
    command_sha256: str
    wrapper_pid: int
    child_pid: int
    returncode: int
    elapsed_seconds: float
    cooperative_reason: str | None
    log_segments: tuple[dict[str, object], ...]
    log_total_bytes: int
    segment_limit_bytes: int
    cancellation_requested: bool
    timeout_seconds: float | None
    job_current_commit_bytes: int
    job_peak_commit_bytes: int
    active_processes: int
    wsl_readback: WslCgroupReadback | None
    resource_gate_receipt: Mapping[str, object]
    result_path: str
    log_root: str

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "execution_id": self.execution_id,
            "runtime": self.runtime,
            "environment_scope": self.environment_scope,
            "credential_env_keys": list(self.credential_env_keys),
            "command_sha256": self.command_sha256,
            "wrapper_pid": self.wrapper_pid,
            "child_pid": self.child_pid,
            "returncode": self.returncode,
            "elapsed_seconds": self.elapsed_seconds,
            "cooperative_reason": self.cooperative_reason,
            "log_segments": list(self.log_segments),
            "log_total_bytes": self.log_total_bytes,
            "segment_limit_bytes": self.segment_limit_bytes,
            "cancellation_requested": self.cancellation_requested,
            "timeout_seconds": self.timeout_seconds,
            "job_current_commit_bytes": self.job_current_commit_bytes,
            "job_peak_commit_bytes": self.job_peak_commit_bytes,
            "active_processes": self.active_processes,
            "wsl_readback": (
                {
                    "main_pid": self.wsl_readback.main_pid,
                    "control_group": self.wsl_readback.control_group,
                    "memory_high_bytes": self.wsl_readback.memory_high_bytes,
                    "memory_max_bytes": self.wsl_readback.memory_max_bytes,
                    "memory_swap_max_bytes": self.wsl_readback.memory_swap_max_bytes,
                    "active_state": self.wsl_readback.active_state,
                }
                if self.wsl_readback is not None
                else None
            ),
            "resource_gate_receipt": dict(self.resource_gate_receipt),
            "result_path": self.result_path,
            "log_root": self.log_root,
        }


class ResourceSupervisor:
    """Hold the task Job handle and WSL guardian heartbeat for one attempt."""

    def __init__(
        self,
        *,
        attempt_id: str,
        fence: int,
        control_root: Path,
        policy: ResourcePolicy,
        hybrid_wsl: bool,
        resource_gate: ResourceGate | None = None,
        cas_store_factory: Callable[[Path], CASStore] = CASStore,
        job_factory: Callable[..., WindowsJob] = WindowsJob,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        child_poll: Callable[[JobChild], int | None] | None = None,
        child_close: Callable[[JobChild], None] | None = None,
    ) -> None:
        validate_resource_policy(policy)
        if not _EXECUTION_ID.fullmatch(str(attempt_id)) or fence <= 0:
            raise ResourceSupervisorError("attempt identity is invalid")
        if not isinstance(hybrid_wsl, bool):
            raise ResourceSupervisorError("hybrid_wsl must be a boolean")
        self.attempt_id = attempt_id
        self.fence = int(fence)
        self.policy = policy
        self.control_root = control_root.resolve(strict=True)
        _assert_existing_plain_chain(self.control_root)
        self.cas = cas_store_factory(self.control_root)
        self._hybrid_wsl = hybrid_wsl
        self._wsl_launch_count = 0
        self._current_wsl_execution_id: str | None = None
        self._wsl_distro: str | None = None
        self._wsl_guardian_state_path: Path | None = None
        if hybrid_wsl:
            guardian_state_root = self.control_root / "guardian_states"
            guardian_state_root.mkdir(parents=False, exist_ok=True)
            guardian_state_root = guardian_state_root.resolve(strict=True)
            _assert_existing_plain_chain(guardian_state_root)
            if not guardian_state_root.is_relative_to(self.control_root):
                raise ResourceSupervisorError("WSL guardian state path escapes control root")
            self._wsl_guardian_state_path = guardian_state_root / f"{self.attempt_id}-{self.fence}.json"
            if self._wsl_guardian_state_path.exists():
                raise ResourceSupervisorError("WSL guardian state already exists for attempt/fence")
            self._write_wsl_guardian_state(
                state="QUIESCENT",
                execution_id=None,
                unit=f"aistock-dataset-{self.attempt_id}-{self.fence}.service",
                distro=None,
                control_group=None,
                systemd_wait_completed=False,
                active_processes=0,
                create_only=True,
            )
        heartbeat_candidate = self.control_root / "heartbeats"
        if not heartbeat_candidate.parent.resolve(strict=True).is_relative_to(self.control_root):
            raise ResourceSupervisorError("heartbeat path escapes control root")
        heartbeat_candidate.mkdir(parents=False, exist_ok=True)
        heartbeat_root = heartbeat_candidate.resolve(strict=True)
        _assert_existing_plain_chain(heartbeat_root)
        self.heartbeat_path = heartbeat_root / f"{attempt_id}-{fence}.json"
        self.job = job_factory(
            f"aistock-dataset-{attempt_id}-{fence}",
            policy=policy,
            hybrid_wsl=hybrid_wsl,
        )
        self.heartbeat = HeartbeatWriter(
            self.heartbeat_path,
            attempt_id=attempt_id,
            fence=fence,
            interval_seconds=policy.enforcement_sample_seconds,
        )
        self.heartbeat.start()
        self._sleep = sleep
        self._monotonic = monotonic
        self._child_poll = child_poll or _poll_job_child
        self._child_close = child_close or _close_job_child
        self.resource_gate = resource_gate
        self._wsl_telemetry_path: Path | None = None
        self._wsl_expected_control_group: str | None = None
        if self.resource_gate is not None:
            self.resource_gate.bind_owned_probe(self._owned_runtime_snapshot)
        self._closed = False

    def _launch_windows(
        self,
        command: Sequence[str],
        *,
        cwd: Path,
        env: Mapping[str, str] | None = None,
    ) -> SupervisedChild:
        self.heartbeat.assert_healthy()
        child = self.job.launch(command, cwd=cwd, env=env)
        return SupervisedChild(child=child, attempt_id=self.attempt_id, fence=self.fence)

    def _launch_wsl(
        self,
        command: Sequence[str],
        *,
        distro: str,
        guardian_python: str,
        guardian_script_wsl: str,
        heartbeat_path_wsl: str,
        telemetry_path_wsl: str,
        resource_checkpoint_path_wsl: str,
        cwd: Path,
        env: Mapping[str, str] | None = None,
        service_factory: Callable[..., WslCgroupService] = WslCgroupService,
    ) -> SupervisedChild:
        self.heartbeat.assert_healthy()
        identity = WslUnitIdentity.create(distro, self.attempt_id, self.fence)
        self._wsl_distro = identity.distro
        self._wsl_launch_count += 1
        self._write_wsl_guardian_state(
            state="ACTIVE",
            execution_id=self._current_wsl_execution_id,
            unit=identity.unit,
            distro=identity.distro,
            control_group=None,
            systemd_wait_completed=False,
            active_processes=1,
        )
        service = service_factory(identity, self.policy)
        guardian = [
            guardian_python,
            guardian_script_wsl,
            "--attempt-id",
            self.attempt_id,
            "--fence",
            str(self.fence),
            "--heartbeat",
            heartbeat_path_wsl,
            "--ttl-seconds",
            str(max(3.0, self.policy.enforcement_sample_seconds * 5)),
            "--telemetry",
            telemetry_path_wsl,
            "--memory-high-bytes",
            "0",
            "--memory-max-bytes",
            "0",
            "--memory-swap-max-bytes",
            "0",
            "--wsl-start-available-bytes",
            "0",
            "--resource-checkpoint",
            resource_checkpoint_path_wsl,
            "--",
            *[str(part) for part in command],
        ]
        child = self.job.launch(service.launch_command(guardian), cwd=cwd, env=env)
        last_error: BaseException | None = None
        for _attempt in range(10):
            try:
                readback = service.readback(timeout_seconds=5)
                self._wsl_expected_control_group = readback.control_group
                self._write_wsl_guardian_state(
                    state="ACTIVE",
                    execution_id=self._current_wsl_execution_id,
                    unit=identity.unit,
                    distro=identity.distro,
                    control_group=readback.control_group,
                    systemd_wait_completed=False,
                    active_processes=max(1, self.job.accounting().active_processes),
                )
                self._owned_runtime_snapshot(True)
                return SupervisedChild(
                    child=child,
                    attempt_id=self.attempt_id,
                    fence=self.fence,
                    wsl_unit=identity,
                    wsl_readback=readback,
                )
            except BaseException as exc:
                last_error = exc
                self._sleep(0.25)
        # Closing the Job handle on abort only affects the task-owned launcher.
        self.abort()
        raise ResourceSupervisorError("WSL transient unit did not pass readback") from last_error

    def _write_wsl_guardian_state(
        self,
        *,
        state: Literal["ACTIVE", "QUIESCENT"],
        execution_id: str | None,
        unit: str,
        distro: str | None,
        control_group: str | None,
        systemd_wait_completed: bool,
        active_processes: int,
        create_only: bool = False,
    ) -> None:
        """Persist the sole fence-bound WSL liveness authority for this attempt."""

        path = self._wsl_guardian_state_path
        if path is None:
            raise ResourceSupervisorError("hybrid WSL attempt has no durable guardian state path")
        if state == "ACTIVE" and active_processes <= 0:
            raise ResourceSupervisorError("active WSL guardian state has no process")
        if state == "QUIESCENT" and active_processes != 0:
            raise ResourceSupervisorError("quiescent WSL guardian state has processes")
        payload = {
            "schema_version": WSL_GUARDIAN_STATE_SCHEMA,
            "attempt_id": self.attempt_id,
            "fence": self.fence,
            "state": state,
            "launch_count": self._wsl_launch_count,
            "execution_id": execution_id,
            "unit": unit,
            "distro": distro,
            "control_group": control_group,
            "systemd_wait_completed": systemd_wait_completed,
            "active_processes": active_processes,
            "observed_utc": datetime.now(timezone.utc).isoformat(),
        }
        if create_only:
            _atomic_json_create(path, payload)
        else:
            _atomic_json(path, payload)

    def accounting(self) -> JobAccounting:
        self.heartbeat.assert_healthy()
        return self.job.accounting()

    def _owned_runtime_snapshot(self, wsl_required: bool) -> OwnedRuntimeSnapshot:
        accounting = self.accounting()
        wsl_snapshot = None
        if wsl_required:
            if self._wsl_telemetry_path is None:
                raise ResourceTelemetryUnavailable("WSL resource telemetry path is not bound")
            wsl_snapshot = _read_wsl_runtime_telemetry(
                self._wsl_telemetry_path,
                attempt_id=self.attempt_id,
                fence=self.fence,
                expected_control_group=self._wsl_expected_control_group,
                policy=self.policy,
            )
        return OwnedRuntimeSnapshot(
            windows_job_commit_bytes=int(accounting.current_commit_bytes),
            windows_job_peak_commit_bytes=int(accounting.peak_commit_bytes),
            windows_tree_rss_bytes=int(accounting.current_rss_bytes),
            windows_tree_peak_rss_bytes=int(accounting.peak_rss_bytes),
            active_processes=int(accounting.active_processes),
            wsl=wsl_snapshot,
        )

    def run_supervised(
        self,
        command: Sequence[str],
        *,
        execution_id: str,
        cwd: Path,
        env: Mapping[str, str] | None = None,
        environment_scope: Literal["source", "build", "validation"] = "build",
        credential_env_keys: Sequence[str] = (),
        runtime: str = "windows",
        timeout_seconds: float | None = None,
        cooperative_grace_seconds: float = 30.0,
        pressure_rung: int = 0,
        cancel_requested: Callable[[], bool] = lambda: False,
        checkpoint: Callable[[], None] = lambda: None,
        wsl: WslSupervisedOptions | None = None,
        service_factory: Callable[..., WslCgroupService] = WslCgroupService,
    ) -> SupervisedExecutionReceipt:
        """Run one bounded-log helper with ownership and telemetry receipts."""

        if not _EXECUTION_ID.fullmatch(str(execution_id)):
            raise ResourceSupervisorError("supervised execution_id is invalid")
        if runtime not in {"windows", "wsl"}:
            raise ResourceSupervisorError("supervised runtime must be windows or wsl")
        if not command or any("\x00" in str(part) for part in command):
            raise ResourceSupervisorError("supervised command is invalid")
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ResourceSupervisorError("supervised timeout must be positive")
        if cooperative_grace_seconds <= 0:
            raise ResourceSupervisorError("cooperative grace must be positive")
        if type(pressure_rung) is not int or pressure_rung < 0:
            raise ResourceSupervisorError("supervised pressure rung is invalid")
        if self.resource_gate is None:
            raise ResourceSupervisorError("production resource gate is not configured")
        if runtime == "wsl" and wsl is None:
            raise ResourceSupervisorError("WSL supervised execution requires typed options")
        if runtime == "windows" and wsl is not None:
            raise ResourceSupervisorError("Windows execution cannot accept WSL options")
        child_environment = build_supervised_environment(
            scope=environment_scope,
            credential_env_keys=credential_env_keys,
            overrides=env,
        )
        admission = self.resource_gate.admit(
            str(execution_id),
            wsl_required=False,
            pressure_rung=pressure_rung,
        )
        if admission.decision.status != "READY":
            raise ResourceSupervisorError(f"resource admission rejected child: {admission.decision.reason_code}")
        resolved_cwd = Path(cwd).resolve(strict=True)
        attempt_runs = self.control_root / "attempt_runs"
        attempt_runs.mkdir(parents=False, exist_ok=True)
        attempt_runs = attempt_runs.resolve(strict=True)
        _assert_existing_plain_chain(attempt_runs)
        if not attempt_runs.is_relative_to(self.control_root):
            raise ResourceSupervisorError("supervised execution path escapes control root")
        attempt_root = attempt_runs / f"{self.attempt_id}-{self.fence}"
        attempt_root.mkdir(parents=False, exist_ok=True)
        attempt_root = attempt_root.resolve(strict=True)
        _assert_existing_plain_chain(attempt_root)
        execution_candidate = attempt_root / str(execution_id)
        execution_candidate.mkdir(parents=False, exist_ok=False)
        execution_root = execution_candidate.resolve(strict=True)
        if not execution_root.is_relative_to(self.control_root):
            raise ResourceSupervisorError("supervised execution path escapes control root")
        _assert_existing_plain_chain(execution_root)
        log_root = execution_root / "logs"
        result_path = execution_root / "result.json"
        cancel_path = execution_root / "cancel.requested.json"
        resource_checkpoint_path = execution_root / "resource_checkpoint.requested.json"
        wrapper = self._runner_command(
            command,
            runtime=runtime,
            cwd=resolved_cwd,
            log_root=log_root,
            result_path=result_path,
            cancel_path=cancel_path,
            timeout_seconds=timeout_seconds,
            cooperative_grace_seconds=cooperative_grace_seconds,
            wsl=wsl,
        )
        if runtime == "windows":
            merged_env = dict(child_environment)
            merged_env[RESOURCE_CHECKPOINT_ENV] = str(resource_checkpoint_path)
            existing_pythonpath = merged_env.get("PYTHONPATH")
            merged_env["PYTHONPATH"] = str(_REPOSITORY_ROOT) + (
                os.pathsep + existing_pythonpath if existing_pythonpath else ""
            )
            admitted = self._launch_windows(
                wrapper,
                cwd=_REPOSITORY_ROOT,
                env=merged_env,
            )
        else:
            assert wsl is not None
            self._current_wsl_execution_id = str(execution_id)
            self._wsl_telemetry_path = execution_root / "wsl_resource_telemetry.json"
            self._wsl_expected_control_group = None
            checkpoint_path_wsl = f"{wsl.execution_root_wsl.rstrip('/')}/resource_checkpoint.requested.json"
            merged_env = dict(child_environment)
            merged_env[RESOURCE_CHECKPOINT_ENV] = checkpoint_path_wsl
            merged_env["WSLENV"] = _append_wslenv(merged_env.get("WSLENV", ""), RESOURCE_CHECKPOINT_ENV)
            admitted = self._launch_wsl(
                wrapper,
                distro=wsl.distro,
                guardian_python=wsl.guardian_python,
                guardian_script_wsl=wsl.guardian_script_wsl,
                heartbeat_path_wsl=wsl.heartbeat_path_wsl,
                telemetry_path_wsl=(f"{wsl.execution_root_wsl.rstrip('/')}/wsl_resource_telemetry.json"),
                resource_checkpoint_path_wsl=checkpoint_path_wsl,
                cwd=_REPOSITORY_ROOT,
                env=merged_env,
                service_factory=service_factory,
            )
        started = self._monotonic()
        next_resource_sample = started
        cancel_written = False
        cancellation_at: float | None = None
        peak_current = 0
        peak_commit = 0
        wrapper_returncode: int | None = None
        while wrapper_returncode is None:
            self.heartbeat.assert_healthy()
            checkpoint()
            accounting = self.job.accounting()
            peak_current = max(peak_current, accounting.current_commit_bytes)
            peak_commit = max(peak_commit, accounting.peak_commit_bytes)
            wrapper_returncode = self._child_poll(admitted.child)
            if wrapper_returncode is not None:
                break
            sample_now = self._monotonic()
            if sample_now >= next_resource_sample:
                self.resource_gate.sample(
                    str(execution_id),
                    wsl_required=runtime == "wsl",
                    pressure_rung=pressure_rung,
                )
                next_resource_sample = sample_now + self.policy.enforcement_sample_seconds
            if cancel_requested() and not cancel_written:
                _atomic_json(
                    cancel_path,
                    {
                        "schema_version": "dataset_supervised_cancel_v1",
                        "attempt_id": self.attempt_id,
                        "fence": self.fence,
                        "execution_id": execution_id,
                        "requested_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
                cancel_written = True
                cancellation_at = self._monotonic()
            elapsed = self._monotonic() - started
            outer_deadline = timeout_seconds + cooperative_grace_seconds + 10 if timeout_seconds is not None else None
            cancellation_deadline = (
                cancellation_at + cooperative_grace_seconds + 10 if cancellation_at is not None else None
            )
            if (outer_deadline is not None and elapsed > outer_deadline) or (
                cancellation_deadline is not None and self._monotonic() > cancellation_deadline
            ):
                raise ResourceSupervisorError("supervised child did not quiesce after cooperative deadline")
            self._sleep(0.1)
        # The wrapper result is authoritative only after every inherited Job
        # child has exited; otherwise a crashed wrapper could hide a live child.
        quiescence_deadline = self._monotonic() + cooperative_grace_seconds
        accounting = self.job.accounting()
        while accounting.active_processes:
            self.heartbeat.assert_healthy()
            checkpoint()
            if self._monotonic() >= quiescence_deadline:
                raise ResourceSupervisorError("supervised Job/cgroup descendants did not become quiescent")
            self._sleep(0.1)
            accounting = self.job.accounting()
            peak_current = max(peak_current, accounting.current_commit_bytes)
            peak_commit = max(peak_commit, accounting.peak_commit_bytes)
        self._child_close(admitted.child)
        final_owned = self._owned_runtime_snapshot(runtime == "wsl")
        expected_wrapper_pid = admitted.child.pid
        if runtime == "wsl":
            if final_owned.wsl is None or final_owned.wsl.wrapper_pid <= 0:
                raise ResourceSupervisorError("WSL runner wrapper PID telemetry is unavailable")
            expected_wrapper_pid = final_owned.wsl.wrapper_pid
            assert admitted.wsl_unit is not None
            self._write_wsl_guardian_state(
                state="QUIESCENT",
                execution_id=str(execution_id),
                unit=admitted.wsl_unit.unit,
                distro=admitted.wsl_unit.distro,
                control_group=final_owned.wsl.control_group,
                # The allowlisted launcher is systemd-run --wait --collect;
                # its admitted Windows process tree has fully exited here.
                systemd_wait_completed=True,
                active_processes=0,
            )
        payload = _read_bounded_runner_result(result_path)
        receipt = _validate_runner_result(
            payload,
            execution_id=str(execution_id),
            runtime=runtime,
            attempt_id=self.attempt_id,
            fence=self.fence,
            admitted=admitted,
            wrapper_returncode=wrapper_returncode,
            command=command,
            log_root=log_root,
            result_path=result_path,
            cancel_written=cancel_written,
            timeout_seconds=timeout_seconds,
            peak_current=max(peak_current, accounting.current_commit_bytes),
            peak_commit=max(peak_commit, accounting.peak_commit_bytes),
            active_processes=accounting.active_processes,
            expected_wrapper_pid=expected_wrapper_pid,
            cas_store=self.cas,
            environment_scope=environment_scope,
            credential_env_keys=tuple(sorted(credential_env_keys)),
            resource_gate_receipt=self.resource_gate.receipt(),
        )
        if receipt.cooperative_reason == "log_capacity":
            raise ControlRootCapacityExceeded(
                "supervised log segment capacity was reached",
                context={"execution_id": str(execution_id)},
            )
        if receipt.cooperative_reason == "log_failure":
            raise ResourceSupervisorError("supervised log writer failed")
        if receipt.active_processes != 0:
            raise ResourceSupervisorError("supervised receipt retained active processes")
        return receipt

    def _runner_command(
        self,
        command: Sequence[str],
        *,
        runtime: str,
        cwd: Path,
        log_root: Path,
        result_path: Path,
        cancel_path: Path,
        timeout_seconds: float | None,
        cooperative_grace_seconds: float,
        wsl: WslSupervisedOptions | None,
    ) -> list[str]:
        if runtime == "windows":
            prefix = [
                sys.executable,
                "-m",
                "backend.services.dataset_release.subprocess_runner",
            ]
            runner_cwd = str(cwd)
            runner_logs = str(log_root)
            runner_result = str(result_path)
            runner_cancel = str(cancel_path)
        else:
            assert wsl is not None
            _validate_wsl_options(wsl)
            prefix = [wsl.runner_python_wsl, wsl.runner_script_wsl]
            runner_cwd = wsl.task_cwd_wsl
            runner_logs = f"{wsl.execution_root_wsl.rstrip('/')}/logs"
            runner_result = f"{wsl.execution_root_wsl.rstrip('/')}/result.json"
            runner_cancel = f"{wsl.execution_root_wsl.rstrip('/')}/cancel.requested.json"
        result = [
            *prefix,
            "--log-root",
            runner_logs,
            "--cwd",
            runner_cwd,
            "--result-path",
            runner_result,
            "--cancel-file",
            runner_cancel,
            "--attempt-id",
            self.attempt_id,
            "--fence",
            str(self.fence),
            "--cooperative-grace-seconds",
            str(cooperative_grace_seconds),
        ]
        if timeout_seconds is not None:
            result.extend(("--timeout-seconds", str(timeout_seconds)))
        result.extend(("--", *[str(part) for part in command]))
        return result

    def close(self) -> None:
        if self._closed:
            return
        self.job.close(require_quiescent=True)
        self.heartbeat.stop()
        self._closed = True

    def abort(self) -> None:
        if self._closed:
            return
        try:
            self.heartbeat.stop()
        finally:
            self.job.close(require_quiescent=False)
            self._closed = True

    def __enter__(self) -> "ResourceSupervisor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type is None:
            self.close()
        else:
            self.abort()


__all__ = [
    "ControlRootCapacityExceeded",
    "HeartbeatWriter",
    "ResourceSupervisor",
    "ResourceSupervisorError",
    "SOURCE_CREDENTIAL_ENV_ALLOWLIST",
    "SupervisedExecutionReceipt",
    "SupervisedChild",
    "WslSupervisedOptions",
    "build_supervised_environment",
]


def _poll_job_child(child: JobChild) -> int | None:
    handle = child.process_handle
    if isinstance(handle, subprocess.Popen):
        return handle.poll()
    if os.name != "nt":
        raise ResourceSupervisorError("native Job child polling requires Windows")
    try:
        import win32event
        import win32process

        status = win32event.WaitForSingleObject(handle, 0)
        if status == win32event.WAIT_TIMEOUT:
            return None
        return int(win32process.GetExitCodeProcess(handle))
    except Exception as exc:  # pragma: no cover - platform API failure
        raise ResourceSupervisorError("unable to poll admitted Job child") from exc


def _close_job_child(child: JobChild) -> None:
    handle = child.process_handle
    if isinstance(handle, subprocess.Popen):
        return
    if os.name != "nt":
        raise ResourceSupervisorError("native Job child close requires Windows")
    try:
        import win32api

        if child.owner_pipe_handle is not None:
            win32api.CloseHandle(child.owner_pipe_handle)
            child.owner_pipe_handle = None
        win32api.CloseHandle(handle)
    except Exception as exc:  # pragma: no cover - platform API failure
        raise ResourceSupervisorError("unable to close admitted child handle") from exc


def _read_bounded_runner_result(path: Path) -> Mapping[str, Any]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise ResourceSupervisorError("supervised runner result is missing") from exc
    if not 0 < size <= _RESULT_LIMIT_BYTES:
        raise ResourceSupervisorError("supervised runner result exceeds bounded limit")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ResourceSupervisorError) as exc:
        raise ResourceSupervisorError("supervised runner result is invalid") from exc
    if not isinstance(payload, dict):
        raise ResourceSupervisorError("supervised runner result must be an object")
    return payload


def _validate_runner_result(
    payload: Mapping[str, Any],
    *,
    execution_id: str,
    runtime: str,
    attempt_id: str,
    fence: int,
    admitted: SupervisedChild,
    wrapper_returncode: int,
    command: Sequence[str],
    log_root: Path,
    result_path: Path,
    cancel_written: bool,
    timeout_seconds: float | None,
    peak_current: int,
    peak_commit: int,
    active_processes: int,
    expected_wrapper_pid: int,
    cas_store: CASStore,
    environment_scope: str,
    credential_env_keys: tuple[str, ...],
    resource_gate_receipt: Mapping[str, object],
) -> SupervisedExecutionReceipt:
    if (
        payload.get("schema_version") != _RUNNER_RESULT_SCHEMA
        or payload.get("attempt_id") != attempt_id
        or int(payload.get("fence", 0)) != int(fence)
        or int(payload.get("wrapper_pid", 0)) != int(expected_wrapper_pid)
        or not _exit_codes_equivalent(int(payload.get("returncode", -999999)), int(wrapper_returncode))
    ):
        raise ResourceSupervisorError("supervised runner identity/exit readback mismatch")
    raw_segments = payload.get("log_segments")
    if not isinstance(raw_segments, list) or len(raw_segments) > 4096:
        raise ResourceSupervisorError("supervised log manifest is invalid or unbounded")
    segments: list[dict[str, object]] = []
    total = 0
    for raw in raw_segments:
        if not isinstance(raw, dict):
            raise ResourceSupervisorError("supervised log segment is invalid")
        stream = str(raw.get("stream"))
        size = int(raw.get("size_bytes", -1))
        path = str(raw.get("path", ""))
        digest = str(raw.get("sha256", ""))
        generation = int(raw.get("generation", 0))
        if (
            stream not in {"stdout", "stderr"}
            or not 0 <= size <= _SEGMENT_LIMIT_BYTES
            or generation <= 0
            or not re.fullmatch(r"(stdout|stderr)\.[0-9]{6}\.log", path)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise ResourceSupervisorError("supervised log segment contract mismatch")
        persisted = log_root / path
        if not persisted.is_file() or persisted.stat().st_size != size or _sha256_file(persisted) != digest:
            raise ResourceSupervisorError("supervised log segment readback mismatch")
        total += size
        try:
            reference = cas_store.put_stream(
                _file_chunks(persisted),
                max_chunk_bytes=1024 * 1024,
            )
            reference = cas_store.verify(reference)
        except OSError as exc:
            if exc.errno in {errno.ENOSPC, getattr(errno, "EDQUOT", 122)}:
                raise ControlRootCapacityExceeded("control-root CAS has insufficient capacity") from exc
            raise ResourceSupervisorError("supervised log segment CAS registration failed") from exc
        except CASStoreError as exc:
            raise ResourceSupervisorError("supervised log segment CAS registration failed") from exc
        if reference.sha256 != digest or reference.size != size:
            raise ResourceSupervisorError("supervised log segment CAS identity mismatch")
        segments.append(
            {
                "stream": stream,
                "generation": generation,
                "path": path,
                "size_bytes": size,
                "sha256": digest,
                "cas_ref": reference.as_dict(),
            }
        )
    elapsed = float(payload.get("elapsed_seconds", -1))
    child_pid = int(payload.get("child_pid", 0))
    reason = payload.get("cooperative_reason")
    if (
        elapsed < 0
        or child_pid <= 0
        or reason
        not in {
            None,
            "cancel",
            "timeout",
            "log_capacity",
            "log_failure",
        }
    ):
        raise ResourceSupervisorError("supervised runner timing/child result is invalid")
    command_digest = hashlib.sha256(
        json.dumps(
            [str(part) for part in command],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return SupervisedExecutionReceipt(
        schema_version="dataset_supervised_execution_receipt_v1",
        execution_id=execution_id,
        runtime=runtime,
        environment_scope=environment_scope,
        credential_env_keys=credential_env_keys,
        command_sha256=command_digest,
        wrapper_pid=int(payload["wrapper_pid"]),
        child_pid=child_pid,
        returncode=int(payload["returncode"]),
        elapsed_seconds=elapsed,
        cooperative_reason=reason,
        log_segments=tuple(segments),
        log_total_bytes=total,
        segment_limit_bytes=_SEGMENT_LIMIT_BYTES,
        cancellation_requested=cancel_written,
        timeout_seconds=timeout_seconds,
        job_current_commit_bytes=peak_current,
        job_peak_commit_bytes=peak_commit,
        active_processes=active_processes,
        wsl_readback=admitted.wsl_readback,
        resource_gate_receipt=dict(resource_gate_receipt),
        result_path=str(result_path.relative_to(result_path.parents[3])).replace("\\", "/"),
        log_root=str(log_root.relative_to(log_root.parents[3])).replace("\\", "/"),
    )


def _validate_wsl_options(value: WslSupervisedOptions) -> None:
    fields = (
        value.distro,
        value.guardian_python,
        value.guardian_script_wsl,
        value.heartbeat_path_wsl,
        value.runner_python_wsl,
        value.runner_script_wsl,
        value.task_cwd_wsl,
        value.execution_root_wsl,
    )
    if any(not str(item).strip() or "\x00" in str(item) for item in fields):
        raise ResourceSupervisorError("WSL supervised options are invalid")
    if not all(
        str(item).startswith("/")
        for item in (
            value.guardian_python,
            value.guardian_script_wsl,
            value.heartbeat_path_wsl,
            value.runner_python_wsl,
            value.runner_script_wsl,
            value.task_cwd_wsl,
            value.execution_root_wsl,
        )
    ):
        raise ResourceSupervisorError("WSL supervised paths must be absolute")


def _read_wsl_runtime_telemetry(
    path: Path,
    *,
    attempt_id: str,
    fence: int,
    expected_control_group: str | None,
    policy: ResourcePolicy,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> WslRuntimeSnapshot:
    try:
        _assert_plain_path_node(path)
        size = path.stat().st_size
        if not 0 < size <= _WSL_TELEMETRY_LIMIT_BYTES:
            raise ResourceTelemetryUnavailable("WSL resource telemetry exceeds its bounded contract")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except ResourceTelemetryUnavailable:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ResourceTelemetryUnavailable("WSL resource telemetry is unavailable") from exc
    if not isinstance(payload, dict):
        raise ResourceTelemetryUnavailable("WSL resource telemetry is not an object")
    try:
        observed = datetime.fromisoformat(str(payload["observed_utc"]).replace("Z", "+00:00"))
        events = payload["memory_events"]
        control_group = str(payload["control_group"])
        if observed.tzinfo is None or not isinstance(events, dict):
            raise ValueError("invalid timestamp/events")
        age = (now().astimezone(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds()
        snapshot = WslRuntimeSnapshot(
            memory_current_bytes=int(payload["memory_current_bytes"]),
            memory_peak_bytes=int(payload["memory_peak_bytes"]),
            memory_high_bytes=int(payload["memory_high_bytes"]),
            memory_max_bytes=int(payload["memory_max_bytes"]),
            swap_current_bytes=int(payload["swap_current_bytes"]),
            swap_max_bytes=int(payload["swap_max_bytes"]),
            available_bytes=int(payload["wsl_mem_available_bytes"]),
            memory_events={str(key): int(value) for key, value in events.items()},
            control_group=control_group,
            memory_oom_group=int(payload["memory_oom_group"]),
            counter=int(payload["counter"]),
            observed_utc=observed.astimezone(timezone.utc).isoformat(),
            wrapper_pid=int(payload["wrapper_pid"]),
        )
    except (KeyError, TypeError, ValueError, ResourceGateError) as exc:
        raise ResourceTelemetryUnavailable("WSL resource telemetry violates its schema") from exc
    if (
        payload.get("schema_version") != "dataset_release_wsl_resource_telemetry_v1"
        or payload.get("attempt_id") != attempt_id
        or int(payload.get("fence", 0)) != fence
        or not control_group.startswith("/")
        or (expected_control_group is not None and control_group != expected_control_group)
        or not -5 <= age <= max(5.0, policy.enforcement_sample_seconds * 5)
        or snapshot.counter <= 0
        or snapshot.memory_high_bytes != 0
        or snapshot.memory_max_bytes != 0
        or snapshot.swap_max_bytes != 0
    ):
        raise ResourceTelemetryUnavailable("WSL resource telemetry identity/unlimited/freshness mismatch")
    return snapshot


def _exit_codes_equivalent(child_code: int, wrapper_code: int) -> bool:
    if child_code == wrapper_code:
        return True
    if os.name == "nt":
        return child_code & 0xFFFFFFFF == wrapper_code & 0xFFFFFFFF
    return child_code & 0xFF == wrapper_code & 0xFF


def _append_wslenv(existing: str, variable: str) -> str:
    entries = [entry for entry in str(existing).split(":") if entry]
    if not any(entry.split("/", 1)[0].casefold() == variable.casefold() for entry in entries):
        entries.append(variable)
    return ":".join(entries)


def _sha256_file(path: Path, *, block_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(block_size):
            digest.update(chunk)
    return digest.hexdigest()


def _file_chunks(path: Path, *, block_size: int = 1024 * 1024):
    with path.open("rb") as handle:
        while chunk := handle.read(block_size):
            yield chunk


def _assert_existing_plain_chain(path: Path) -> None:
    """Reject links/reparse points in every existing component of ``path``.

    ``Path.resolve`` alone is insufficient for a control directory because a
    junction can redirect the write before the resolved-path containment check.
    The caller only passes existing paths, so every component is checked using
    ``lstat`` and, on Windows, the reparse-point file attribute.
    """

    candidate = Path(path)
    if not candidate.is_absolute():
        raise ResourceSupervisorError("supervisor control path must be absolute")
    parts = candidate.parts
    current = Path(parts[0])
    _assert_plain_path_node(current)
    for part in parts[1:]:
        current = current / part
        _assert_plain_path_node(current)


def _assert_plain_path_node(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ResourceSupervisorError("supervisor control path is not readable") from exc
    file_attributes = int(getattr(metadata, "st_file_attributes", 0))
    reparse_attribute = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    if stat.S_ISLNK(metadata.st_mode) or file_attributes & reparse_attribute:
        raise ResourceSupervisorError("supervisor control path contains a link/reparse point")
