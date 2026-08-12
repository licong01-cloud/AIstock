"""Independent, durable dataset-release Worker orchestration core.

This module owns control-plane polling, lease/fence claims and failure
transitions.  Domain processors are injected through strict protocols.  A
processor cannot report success by returning a truthy value: success is
accepted only after the Worker reads back an already committed terminal state.
"""

from __future__ import annotations

import json
import re
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, Protocol, Sequence, cast

from .cas_store import CASStore
from .control_store import ControlStore, StateConflict, append_event
from .errors import DatasetReleaseError
from .lease import ClaimedAttempt, LeaseConflict, LeaseManager, LeaseToken
from .profile import ResourcePolicy, validate_resource_policy
from .publisher import PublishConflict
from .resource_supervisor import (
    SOURCE_CREDENTIAL_ENV_ALLOWLIST,
    ResourceSupervisor,
    ResourceSupervisorError,
    SupervisedExecutionReceipt,
    WslSupervisedOptions,
)
from .resource_gate import (
    ResourceCheckpointRequested,
    ResourceGate,
    ResourceGateError,
    ResourceGateSample,
)
from .state_machine import RUN_TRANSITIONS, TERMINAL_RUN_STATES
from .worker_commands import WorkerCommandCoordinator
from .worker_identity import WorkerHeartbeatStore, WorkerIdentity


WORKER_ERROR_RECEIPT_SCHEMA = "dataset_release_worker_error_v1"
DEFAULT_LEASE_TTL_SECONDS = 30.0
DEFAULT_POLL_SECONDS = 5.0
DEFAULT_IDLE_MAX_POLL_SECONDS = 15.0
DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 15.0
DEFAULT_RETRY_BACKOFF_SECONDS = 30.0
DEFAULT_MAX_ATTEMPTS = 3
MAX_DRAIN_JOBS = 1_000
SOURCE_RECHECK_CREDENTIAL_ENV_ALLOWLIST = (
    "TDX_DB_HOST",
    "TDX_DB_NAME",
    "TDX_DB_PASSWORD",
    "TDX_DB_PORT",
    "TDX_DB_USER",
)
SOURCE_RECHECK_EXECUTION_ID = "prepublish-source-recheck"
SOURCE_RECHECK_SCRIPT = "dataset_release_source_recheck.py"


class WorkerError(DatasetReleaseError):
    code = "DATASET_RELEASE_WORKER_ERROR"


class ProcessorUnavailable(WorkerError):
    code = "BLOCKED_DATASET_PROCESSOR_UNAVAILABLE"


class ProcessorContractViolation(WorkerError):
    code = "BLOCKED_DATASET_PROCESSOR_CONTRACT"


class PublishRecoveryConflict(WorkerError):
    """Immutable publish identity/marker differs from the prepared record."""

    code = "PUBLISH_FINAL_PATH_CONFLICT"


class CooperativeShutdown(WorkerError):
    code = "DATASET_WORKER_COOPERATIVE_SHUTDOWN"
    retryable = True


class CancellationRequested(WorkerError):
    code = "DATASET_RELEASE_CANCEL_REQUESTED"


class ProcessorDisposition(str, Enum):
    DURABLE_SUCCESS = "DURABLE_SUCCESS"
    RETRYABLE = "RETRYABLE"
    WAITING = "WAITING"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


@dataclass(frozen=True, slots=True)
class ProcessorResult:
    disposition: ProcessorDisposition
    error_code: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)
    retry_after_seconds: float | None = None
    terminal_state: str | None = None

    def __post_init__(self) -> None:
        if self.disposition is ProcessorDisposition.DURABLE_SUCCESS:
            if self.error_code or self.retry_after_seconds is not None or self.terminal_state:
                raise ValueError("durable success cannot carry failure metadata")
            return
        if not str(self.error_code or "").strip():
            raise ValueError("non-success processor result requires an error code")
        if self.retry_after_seconds is not None and self.retry_after_seconds < 0:
            raise ValueError("retry delay cannot be negative")

    @classmethod
    def durable_success(cls) -> "ProcessorResult":
        return cls(ProcessorDisposition.DURABLE_SUCCESS)

    @classmethod
    def retryable(
        cls,
        error_code: str,
        *,
        retry_after_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        context: Mapping[str, Any] | None = None,
    ) -> "ProcessorResult":
        return cls(
            ProcessorDisposition.RETRYABLE,
            error_code,
            dict(context or {}),
            retry_after_seconds,
        )

    @classmethod
    def waiting(
        cls,
        error_code: str,
        *,
        retry_after_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        context: Mapping[str, Any] | None = None,
    ) -> "ProcessorResult":
        return cls(
            ProcessorDisposition.WAITING,
            error_code,
            dict(context or {}),
            retry_after_seconds,
        )

    @classmethod
    def blocked(
        cls,
        error_code: str,
        *,
        terminal_state: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> "ProcessorResult":
        return cls(
            ProcessorDisposition.BLOCKED,
            error_code,
            dict(context or {}),
            None,
            terminal_state,
        )

    @classmethod
    def cancelled(cls) -> "ProcessorResult":
        return cls(ProcessorDisposition.CANCELLED, "DATASET_RELEASE_CANCELLED")


@dataclass(frozen=True, slots=True)
class WorkResourceSpec:
    policy: ResourcePolicy
    hybrid_wsl: bool
    release_id: str | None = None
    acquire_host: bool = True
    requested_ram: int | None = None
    db_connections: int | None = None
    io_class: str = "dataset-release"
    staging_ref: str | None = None
    pressure_rung: int = 0
    predicted_new_bytes: int | None = None
    credential_env_allowlist: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        validate_resource_policy(self.policy)
        if self.acquire_host is not True:
            raise ProcessorContractViolation("data-bearing Worker processors must acquire the host resource lease")
        if not isinstance(self.hybrid_wsl, bool) or not self.io_class.strip():
            raise ProcessorContractViolation("processor resource spec is invalid")
        if self.requested_ram is None:
            object.__setattr__(
                self,
                "requested_ram",
                self.policy.aggregate_private_commit_bytes,
            )
        if self.db_connections is None:
            object.__setattr__(self, "db_connections", self.policy.db_pool_size)
        if int(self.requested_ram) <= 0 or not 0 < int(self.db_connections) <= self.policy.db_pool_size:
            raise ProcessorContractViolation("processor resource claim is outside the profile")
        if type(self.pressure_rung) is not int or self.pressure_rung < 0:
            raise ProcessorContractViolation("processor pressure rung is invalid")
        if self.predicted_new_bytes is not None and (
            type(self.predicted_new_bytes) is not int or self.predicted_new_bytes < 0
        ):
            raise ProcessorContractViolation("processor predicted new bytes are invalid")
        keys = tuple(str(key) for key in self.credential_env_allowlist)
        if len(set(keys)) != len(keys) or set(keys).difference(SOURCE_CREDENTIAL_ENV_ALLOWLIST):
            raise ProcessorContractViolation("processor credential environment allowlist is invalid")
        object.__setattr__(self, "credential_env_allowlist", tuple(sorted(keys)))


@dataclass(frozen=True, slots=True)
class SupervisorRequest:
    attempt_id: str
    fence: int
    control_root: Path
    policy: ResourcePolicy
    hybrid_wsl: bool
    resource_gate: ResourceGate | None = None


class AttemptSupervisor(Protocol):
    heartbeat_path: Path

    def __enter__(self) -> "AttemptSupervisor": ...

    def __exit__(self, exc_type, exc, traceback) -> None: ...

    def run_supervised(
        self,
        command: Sequence[str],
        **kwargs: Any,
    ) -> SupervisedExecutionReceipt: ...


class ResolutionProcessor(Protocol):
    def resource_spec(self, submission: Mapping[str, Any]) -> WorkResourceSpec: ...

    def process(self, context: "WorkerAttemptContext") -> ProcessorResult: ...


class BuildProcessor(Protocol):
    def resource_spec(self, run: Mapping[str, Any]) -> WorkResourceSpec: ...

    def process(self, context: "WorkerAttemptContext") -> ProcessorResult: ...


class PublishRecoveryProvider(Protocol):
    """Parent-only adapter around ``DatasetPublisher.recover_and_finalize``."""

    def recover_and_finalize(
        self,
        *,
        run: Mapping[str, Any],
        claim: ClaimedAttempt,
    ) -> Mapping[str, Any]: ...


class WslQuiescenceProvider(Protocol):
    """Read durable, attempt/fence-bound WSL guardian evidence only."""

    def __call__(self, owner: "LeaseOwnerSnapshot") -> Literal["active", "quiescent", "unknown"]: ...


@dataclass(frozen=True, slots=True)
class ProcessorRegistry:
    resolution: ResolutionProcessor | None = None
    build: BuildProcessor | None = None
    dependency_paths: tuple[str, ...] = ()
    publish_recovery: PublishRecoveryProvider | None = None
    wsl_quiescence: WslQuiescenceProvider | None = None

    @property
    def missing(self) -> tuple[str, ...]:
        return tuple(
            name
            for name, processor in (
                ("resolution", self.resolution),
                ("build", self.build),
                ("publish_recovery", self.publish_recovery),
                ("wsl_quiescence", self.wsl_quiescence),
            )
            if processor is None
        )

    def assert_production_ready(self) -> None:
        if self.missing:
            raise ProcessorUnavailable(
                "dataset release processors are not registered",
                context={"missing_processors": list(self.missing)},
            )
        if not self.dependency_paths:
            raise ProcessorUnavailable(
                "dataset release processor dependency manifest is not registered",
                context={"missing": "dependency_paths"},
            )
        for value in self.dependency_paths:
            path = Path(value)
            if (
                path.is_absolute()
                or not path.parts
                or any(part in {"", ".", ".."} for part in path.parts)
                or any(character in str(value) for character in ("*", "?", "[", "]"))
            ):
                raise ProcessorUnavailable("dataset release processor dependency manifest is invalid")


@dataclass(frozen=True, slots=True)
class LeaseOwnerSnapshot:
    attempt_id: str
    attempt_kind: str
    owner_identity: str
    host: str | None
    owner_pid: int | None
    owner_create_time: str | None
    worker_instance_id: str | None
    code_sha: str | None
    capability_digest: str | None
    hybrid_wsl: bool
    expires_at: str
    lease_state: str


class LivenessProbe(Protocol):
    def __call__(self, owner: LeaseOwnerSnapshot) -> Literal["alive", "dead", "unknown"]: ...


def unknown_liveness(_owner: LeaseOwnerSnapshot) -> Literal["unknown"]:
    """Safe production default until platform process-tree probing is wired."""

    return "unknown"


@dataclass(frozen=True, slots=True)
class CycleReport:
    claimed: bool
    kind: str
    identity: str | None
    state: str
    detail: str | None = None


class WorkerAttemptContext:
    """Only processor gateway to task-owned data-bearing helper launches."""

    def __init__(
        self,
        *,
        kind: Literal["resolution", "build"],
        target_id: str,
        record: Mapping[str, Any],
        store: ControlStore,
        claim: ClaimedAttempt,
        identity: WorkerIdentity,
        supervisor: AttemptSupervisor,
        leases: LeaseManager,
        commands: WorkerCommandCoordinator,
        lease_ttl_seconds: float,
        pressure_rung: int,
        credential_env_allowlist: tuple[str, ...],
        stop_event: threading.Event,
        now: Callable[[], datetime],
    ) -> None:
        self.kind = kind
        self.target_id = target_id
        self.record = dict(record)
        self.store = store
        self.claim = claim
        self.identity = identity
        self._supervisor = supervisor
        self._leases = leases
        self._commands = commands
        self._lease_ttl_seconds = lease_ttl_seconds
        self._pressure_rung = int(pressure_rung)
        self._credential_env_allowlist = tuple(credential_env_allowlist)
        self._stop_event = stop_event
        self._now = now

    @property
    def tokens(self) -> tuple[LeaseToken, ...]:
        return tuple(
            token for token in (self.claim.resolution, self.claim.host, self.claim.release) if token is not None
        )

    @property
    def pressure_rung(self) -> int:
        return int(self._pressure_rung)

    @property
    def supervised_heartbeat_path(self) -> Path:
        """Return the attempt/fence heartbeat consumed by the WSL guardian."""

        raw = getattr(self._supervisor, "heartbeat_path", None)
        if raw is None:
            raise ProcessorContractViolation("attempt supervisor does not expose its heartbeat path")
        try:
            path = Path(raw).resolve(strict=False)
            control_root = self.store.root.resolve(strict=True)
        except OSError as exc:
            raise ProcessorContractViolation("attempt supervisor heartbeat path is unavailable") from exc
        if control_root not in path.parents:
            raise ProcessorContractViolation("attempt supervisor heartbeat path escapes control root")
        return path

    def cancellation_requested(self) -> bool:
        return self._commands.cancellation_requested(
            target_type="submission" if self.kind == "resolution" else "run",
            target_id=self.target_id,
        )

    def checkpoint(self) -> None:
        self._refresh_lease()
        if self.cancellation_requested():
            raise CancellationRequested("durable cancellation is pending")
        if self._stop_event.is_set():
            raise CooperativeShutdown("Worker cooperative shutdown requested")

    def _refresh_lease(self) -> None:
        self._leases.heartbeat(
            self.tokens,
            ttl_seconds=self._lease_ttl_seconds,
            now=self._now(),
        )

    def run_supervised(
        self,
        command: Sequence[str],
        *,
        execution_id: str,
        cwd: Path,
        env: Mapping[str, str] | None = None,
        runtime: str = "windows",
        timeout_seconds: float | None = None,
        cooperative_grace_seconds: float = 30.0,
        wsl: WslSupervisedOptions | None = None,
    ) -> SupervisedExecutionReceipt:
        self.checkpoint()
        self._register_run_log_execution(execution_id)
        receipt = self._supervisor.run_supervised(
            command,
            execution_id=execution_id,
            cwd=cwd,
            env=env,
            environment_scope="source" if self.kind == "resolution" else "build",
            credential_env_keys=self._credential_env_allowlist,
            runtime=runtime,
            timeout_seconds=timeout_seconds,
            cooperative_grace_seconds=cooperative_grace_seconds,
            pressure_rung=self._pressure_rung,
            cancel_requested=lambda: self.cancellation_requested() or self._stop_event.is_set(),
            checkpoint=self._refresh_lease,
            wsl=wsl,
        )
        gate_receipt = getattr(receipt, "resource_gate_receipt", None)
        if not isinstance(gate_receipt, Mapping):
            raise ProcessorContractViolation("supervised child omitted its resource gate receipt")
        if str(gate_receipt.get("final_status")) != "READY" or gate_receipt.get("checkpoint_requested") is True:
            raise ResourceCheckpointRequested(
                "supervised child stopped at a resource checkpoint",
                context={
                    "reason_code": str(gate_receipt.get("final_reason_code", "RESOURCE_PRESSURE")),
                    "pressure_rung": int(gate_receipt.get("next_pressure_rung", self._pressure_rung)),
                    "data_scope_changed": False,
                },
            )
        return receipt

    def run_source_recheck_supervised(
        self,
        command: Sequence[str],
        *,
        execution_id: str,
        cwd: Path,
        timeout_seconds: float,
        cooperative_grace_seconds: float = 30.0,
    ) -> SupervisedExecutionReceipt:
        """Launch the sole DB-only prepublish source recheck from a build."""

        self.checkpoint()
        if self.kind != "build" or execution_id != SOURCE_RECHECK_EXECUTION_ID:
            raise ProcessorContractViolation("source recheck is limited to the typed prepublish build stage")
        try:
            resolved_cwd = Path(cwd).resolve(strict=True)
            expected_script = (resolved_cwd / "scripts" / SOURCE_RECHECK_SCRIPT).resolve(strict=True)
            supplied_script = Path(str(command[1])).resolve(strict=True)
            supplied_python = Path(str(command[0])).resolve(strict=True)
            expected_python = Path(sys.executable).resolve(strict=True)
        except (IndexError, OSError) as exc:
            raise ProcessorContractViolation("source recheck command is incomplete") from exc
        flag_order = (
            "--profile",
            "--control-root",
            "--cutoff",
            "--artifact-ready-contract-ref",
            "--run-id",
            "--attempt-id",
            "--attempt-fence",
            "--execution-id",
            "--result-path",
            "--stage-timeout-seconds",
            "--pressure-rung",
        )
        if len(command) != 2 + 2 * len(flag_order):
            raise ProcessorContractViolation("source recheck command flag set differs")
        supplied_flags = tuple(str(command[index]) for index in range(2, len(command), 2))
        values = {str(command[index]): str(command[index + 1]) for index in range(2, len(command), 2)}
        try:
            profile_path = Path(values["--profile"]).resolve(strict=True)
            control_root = Path(values["--control-root"]).resolve(strict=True)
            result_path = Path(values["--result-path"]).resolve(strict=False)
            date.fromisoformat(values["--cutoff"])
            fence = int(values["--attempt-fence"])
            stage_timeout = float(values["--stage-timeout-seconds"])
            pressure_rung = int(values["--pressure-rung"])
        except (KeyError, OSError, ValueError) as exc:
            raise ProcessorContractViolation("source recheck command values are invalid") from exc
        expected_result = (
            self.store.root
            / "attempt_runs"
            / f"{self.claim.attempt_id}-{self.claim.attempt_fence}"
            / SOURCE_RECHECK_EXECUTION_ID
            / "semantic_result.json"
        ).resolve(strict=False)
        profile_root = (resolved_cwd / "configs" / "datasets").resolve(strict=True)
        if (
            supplied_python != expected_python
            or supplied_script != expected_script
            or supplied_flags != flag_order
            or timeout_seconds <= 0
            or stage_timeout != float(timeout_seconds)
            or control_root != self.store.root.resolve(strict=True)
            or profile_path.parent != profile_root
            or values["--run-id"] != self.target_id
            or values["--attempt-id"] != self.claim.attempt_id
            or fence != self.claim.attempt_fence
            or values["--execution-id"] != SOURCE_RECHECK_EXECUTION_ID
            or result_path != expected_result
            or pressure_rung != self._pressure_rung
            or re.fullmatch(r"[0-9a-f]{64}", values["--artifact-ready-contract-ref"]) is None
        ):
            raise ProcessorContractViolation("source recheck command/script identity differs")
        self._register_run_log_execution(execution_id)
        receipt = self._supervisor.run_supervised(
            command,
            execution_id=execution_id,
            cwd=resolved_cwd,
            env=None,
            environment_scope="source",
            credential_env_keys=SOURCE_RECHECK_CREDENTIAL_ENV_ALLOWLIST,
            runtime="windows",
            timeout_seconds=timeout_seconds,
            cooperative_grace_seconds=cooperative_grace_seconds,
            pressure_rung=self._pressure_rung,
            cancel_requested=lambda: self.cancellation_requested() or self._stop_event.is_set(),
            checkpoint=self._refresh_lease,
            wsl=None,
        )
        gate_receipt = getattr(receipt, "resource_gate_receipt", None)
        if not isinstance(gate_receipt, Mapping):
            raise ProcessorContractViolation("source recheck child omitted its resource gate receipt")
        if str(gate_receipt.get("final_status")) != "READY" or gate_receipt.get("checkpoint_requested") is True:
            raise ResourceCheckpointRequested(
                "source recheck stopped at a resource checkpoint",
                context={
                    "reason_code": str(gate_receipt.get("final_reason_code", "RESOURCE_PRESSURE")),
                    "pressure_rung": int(gate_receipt.get("next_pressure_rung", self._pressure_rung)),
                    "data_scope_changed": False,
                    "production_writes": 0,
                    "production_deletes": 0,
                    "production_pointer_changes": 0,
                    "service_process_controls": 0,
                },
            )
        return receipt

    def _register_run_log_execution(self, execution_id: str) -> None:
        if self.kind != "build":
            return
        self.store.register_run_log_execution(
            run_id=self.target_id,
            attempt_id=self.claim.attempt_id,
            attempt_fence=self.claim.attempt_fence,
            execution_id=execution_id,
        )


class _LeaseHeartbeatLoop:
    """Refresh SQLite lease TTL independently of processor chunk callbacks."""

    def __init__(
        self,
        *,
        leases: LeaseManager,
        tokens: tuple[LeaseToken, ...],
        ttl_seconds: float,
        interval_seconds: float,
        now: Callable[[], datetime],
    ) -> None:
        if not tokens or interval_seconds <= 0 or ttl_seconds < 3 * interval_seconds:
            raise ProcessorContractViolation("lease heartbeat timing/identity is invalid")
        self.leases = leases
        self.tokens = tokens
        self.ttl_seconds = ttl_seconds
        self.interval_seconds = interval_seconds
        self.now = now
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    def _beat(self) -> None:
        self.leases.heartbeat(
            self.tokens,
            ttl_seconds=self.ttl_seconds,
            now=self.now(),
        )

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self._beat()
            except BaseException as exc:  # preserve stale-owner evidence for parent
                self._error = exc
                self._stop.set()

    def start(self) -> None:
        self._beat()
        self._thread = threading.Thread(
            target=self._run,
            name=f"dataset-lease-heartbeat-{self.tokens[0].attempt_id}",
            daemon=False,
        )
        self._thread.start()

    def stop(self) -> BaseException | None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.interval_seconds * 3))
            if self._thread.is_alive() and self._error is None:
                self._error = WorkerError("lease heartbeat thread did not stop")
        return self._error


class _WorkerHealthHeartbeatLoop:
    """Refresh operator health while one synchronous processor is running."""

    def __init__(
        self,
        *,
        heartbeats: WorkerHeartbeatStore,
        identity: WorkerIdentity,
        interval_seconds: float,
        claim_kind: str,
        claim_id: str,
        now: Callable[[], datetime],
    ) -> None:
        if interval_seconds <= 0 or not claim_kind or not claim_id:
            raise WorkerError("worker health heartbeat timing/identity is invalid")
        self.heartbeats = heartbeats
        self.identity = identity
        self.interval_seconds = float(interval_seconds)
        self.claim_kind = claim_kind
        self.claim_id = claim_id
        self.now = now
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    def _beat(self) -> None:
        self.heartbeats.write(
            self.identity,
            status=f"{self.claim_kind.upper()}_RUNNING",
            observed_at=self.now(),
            claim_kind=self.claim_kind,
            claim_id=self.claim_id,
        )

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self._beat()
            except BaseException as exc:  # health is evidence, never lease authority
                self._error = exc
                self._stop.set()

    def start(self) -> None:
        try:
            self._beat()
        except BaseException as exc:
            self._error = exc
            raise
        self._thread = threading.Thread(
            target=self._run,
            name=f"dataset-worker-health-{self.identity.instance_id}",
            daemon=False,
        )
        self._thread.start()

    def stop(self) -> BaseException | None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self.interval_seconds * 3))
            if self._thread.is_alive() and self._error is None:
                self._error = WorkerError("worker health heartbeat thread did not stop")
        return self._error


SupervisorFactory = Callable[[SupervisorRequest], AttemptSupervisor]
ResourceGateFactory = Callable[[WorkResourceSpec, str], ResourceGate]


def _default_supervisor(request: SupervisorRequest) -> AttemptSupervisor:
    return cast(
        AttemptSupervisor,
        ResourceSupervisor(
            attempt_id=request.attempt_id,
            fence=request.fence,
            control_root=request.control_root,
            policy=request.policy,
            hybrid_wsl=request.hybrid_wsl,
            resource_gate=request.resource_gate,
        ),
    )


class DatasetReleaseWorker:
    """Bounded poller independent of FastAPI/backend process lifetime."""

    def __init__(
        self,
        *,
        store: ControlStore,
        identity: WorkerIdentity,
        registry: ProcessorRegistry,
        supervisor_factory: SupervisorFactory = _default_supervisor,
        resource_gate_factory: ResourceGateFactory | None = None,
        liveness_probe: LivenessProbe = unknown_liveness,
        lease_ttl_seconds: float = DEFAULT_LEASE_TTL_SECONDS,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        idle_max_poll_seconds: float = DEFAULT_IDLE_MAX_POLL_SECONDS,
        heartbeat_interval_seconds: float = DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = time.sleep,
        stop_event: threading.Event | None = None,
    ) -> None:
        if (
            lease_ttl_seconds < 3
            or poll_seconds <= 0
            or idle_max_poll_seconds < poll_seconds
            or not 0 < heartbeat_interval_seconds <= idle_max_poll_seconds
            or retry_backoff_seconds < 0
        ):
            raise WorkerError("Worker lease/poll/retry timing is invalid")
        if max_attempts <= 0:
            raise WorkerError("Worker max_attempts must be positive")
        self.store = store
        self.identity = identity
        self.registry = registry
        self.supervisor_factory = supervisor_factory
        self.resource_gate_factory = resource_gate_factory
        self.liveness_probe = liveness_probe
        self.lease_ttl_seconds = float(lease_ttl_seconds)
        self.poll_seconds = float(poll_seconds)
        self.idle_max_poll_seconds = float(idle_max_poll_seconds)
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self.retry_backoff_seconds = float(retry_backoff_seconds)
        self.max_attempts = int(max_attempts)
        self.now = now
        self.sleep = sleep
        self.stop_event = stop_event or threading.Event()
        self.leases = LeaseManager(store)
        self.commands = WorkerCommandCoordinator(store)
        self.cas = CASStore(store.root)
        self.heartbeats = WorkerHeartbeatStore(store)
        self._closed = False
        started_at = self._now()
        self.heartbeats.write(identity, status="STARTED", observed_at=started_at)
        self._last_heartbeat_at = started_at
        self._last_heartbeat_state = "STARTED"

    def request_stop(self) -> None:
        self.stop_event.set()

    def run_once(self) -> CycleReport:
        """Claim at most one command, orphan, resolution, or build item."""

        if self._closed:
            raise WorkerError("Worker instance is already closed")
        if self.stop_event.is_set():
            report = CycleReport(False, "worker", None, "STOP_REQUESTED")
            self._heartbeat(report)
            return report
        command = self.commands.claim_and_apply_one(
            identity=self.identity,
            ttl_seconds=self.lease_ttl_seconds,
            now=self._now(),
        )
        if command is not None:
            report = CycleReport(
                True,
                "command",
                command.command_id,
                command.state,
                command.target_state,
            )
            self._heartbeat(report)
            return report
        orphan = self._reconcile_one_orphan()
        if orphan is not None:
            self._heartbeat(orphan)
            return orphan
        self._requeue_one_due_retry()
        submission = self._next_submission()
        if submission is not None:
            try:
                report = self._process_resolution(submission)
            except LeaseConflict as exc:
                report = CycleReport(
                    False,
                    "resolution",
                    str(submission["submission_id"]),
                    "CLAIM_RACE",
                    type(exc).__name__,
                )
            self._heartbeat(report)
            return report
        run = self._next_run()
        if run is not None:
            try:
                report = self._process_build(run)
            except LeaseConflict as exc:
                report = CycleReport(
                    False,
                    "build",
                    str(run["run_id"]),
                    "CLAIM_RACE",
                    type(exc).__name__,
                )
            self._heartbeat(report)
            return report
        report = CycleReport(False, "worker", None, "IDLE")
        self._heartbeat(report)
        return report

    def run_drain(self, *, max_jobs: int) -> tuple[CycleReport, ...]:
        if not 0 < max_jobs <= MAX_DRAIN_JOBS:
            raise ValueError(f"max_jobs must be between 1 and {MAX_DRAIN_JOBS}")
        reports: list[CycleReport] = []
        while len(reports) < max_jobs and not self.stop_event.is_set():
            report = self.run_once()
            if not report.claimed:
                break
            reports.append(report)
        return tuple(reports)

    def run_serve(self, *, max_polls: int | None = None) -> tuple[CycleReport, ...]:
        """Poll until cooperative stop and retain only a bounded status tail.

        ``max_polls`` exists only for bounded tests; production ``--serve``
        never accumulates an unbounded in-memory history.
        """

        if max_polls is not None and max_polls <= 0:
            raise ValueError("max_polls must be positive")
        reports: deque[CycleReport] = deque(maxlen=200)
        polls = 0
        idle_delay = self.poll_seconds
        while not self.stop_event.is_set() and (max_polls is None or polls < max_polls):
            report = self.run_once()
            reports.append(report)
            polls += 1
            if report.claimed:
                idle_delay = self.poll_seconds
            elif not self.stop_event.is_set():
                self.sleep(idle_delay)
                idle_delay = min(self.idle_max_poll_seconds, idle_delay * 2.0)
        self.close()
        return tuple(reports)

    def close(self) -> None:
        if self._closed:
            return
        self.heartbeats.write(
            self.identity,
            status="STOPPED",
            observed_at=self._now(),
            stop_requested=self.stop_event.is_set(),
        )
        self._closed = True

    def _process_resolution(self, submission: Mapping[str, Any]) -> CycleReport:
        submission_id = str(submission["submission_id"])
        processor = self.registry.resolution
        if processor is None:
            return self._block_unclaimed(
                kind="resolution",
                target_id=submission_id,
                error=ProcessorUnavailable("resolution processor is not registered"),
            )
        try:
            resources = processor.resource_spec(submission)
            self._validate_ttl(resources.policy)
            self._validate_credential_authority("resolution", resources)
        except DatasetReleaseError as exc:
            return self._block_unclaimed(kind="resolution", target_id=submission_id, error=exc)
        resource_gate, deferred = self._admit_resources(
            kind="resolution",
            target_id=submission_id,
            record=submission,
            resources=resources,
        )
        if deferred is not None:
            return deferred
        claim = self.leases.claim_resolution(
            submission_id=submission_id,
            owner_identity=self.identity.owner_identity,
            ttl_seconds=self.lease_ttl_seconds,
            acquire_host=resources.acquire_host,
            host=self.identity.host,
            owner_pid=self.identity.pid,
            owner_create_time=self.identity.process_create_time,
            worker_instance_id=self.identity.instance_id,
            code_sha=self.identity.code_sha,
            capability_digest=self.identity.capability_digest,
            requested_ram=resources.requested_ram,
            db_connections=resources.db_connections,
            io_class=resources.io_class,
            hybrid_wsl=resources.hybrid_wsl,
            now=self._now(),
        )
        return self._run_claimed(
            kind="resolution",
            target_id=submission_id,
            record=submission,
            claim=claim,
            resources=resources,
            processor=processor,
            resource_gate=resource_gate,
        )

    def _process_build(self, run: Mapping[str, Any]) -> CycleReport:
        run_id = str(run["run_id"])
        processor = self.registry.build
        if processor is None:
            return self._block_unclaimed(
                kind="build",
                target_id=run_id,
                error=ProcessorUnavailable("build processor is not registered"),
            )
        try:
            resources = processor.resource_spec(run)
            self._validate_ttl(resources.policy)
            self._validate_credential_authority("build", resources)
            if not str(resources.release_id or "").strip():
                raise ProcessorContractViolation("build processor did not freeze a release_id")
        except DatasetReleaseError as exc:
            return self._block_unclaimed(kind="build", target_id=run_id, error=exc)
        resource_gate, deferred = self._admit_resources(
            kind="build",
            target_id=run_id,
            record=run,
            resources=resources,
        )
        if deferred is not None:
            return deferred
        if str(run["state"]) == "WAITING_RESOURCE":
            run = self._requeue_resource_ready(run_id)
        claim = self.leases.claim_build(
            run_id=run_id,
            release_id=str(resources.release_id),
            owner_identity=self.identity.owner_identity,
            ttl_seconds=self.lease_ttl_seconds,
            attempt_kind=("REATTEST" if run["operation_kind"] == "REATTEST" else "BUILD"),
            host=self.identity.host,
            owner_pid=self.identity.pid,
            owner_create_time=self.identity.process_create_time,
            worker_instance_id=self.identity.instance_id,
            code_sha=self.identity.code_sha,
            capability_digest=self.identity.capability_digest,
            requested_ram=resources.requested_ram,
            db_connections=resources.db_connections,
            io_class=resources.io_class,
            hybrid_wsl=resources.hybrid_wsl,
            staging_ref=resources.staging_ref,
            now=self._now(),
        )
        return self._run_claimed(
            kind="build",
            target_id=run_id,
            record=run,
            claim=claim,
            resources=resources,
            processor=processor,
            resource_gate=resource_gate,
        )

    def _admit_resources(
        self,
        *,
        kind: Literal["resolution", "build"],
        target_id: str,
        record: Mapping[str, Any],
        resources: WorkResourceSpec,
    ) -> tuple[ResourceGate | None, CycleReport | None]:
        if self.resource_gate_factory is None:
            if self.supervisor_factory is _default_supervisor:
                return None, self._defer_unclaimed_resource(
                    kind=kind,
                    target_id=target_id,
                    record=record,
                    reason_code="BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE",
                    hard_failure=True,
                    pressure_rung=resources.pressure_rung,
                    wait_deadline_seconds=resources.policy.wait_deadline_seconds,
                )
            # Explicit injected fixture supervisors remain usable without
            # pretending to be production resource evidence.
            return None, None
        try:
            gate = self.resource_gate_factory(resources, f"{kind}-admission")
            sample = gate.admit(
                f"{kind}-admission",
                wsl_required=False,
                pressure_rung=resources.pressure_rung,
            )
        except ResourceGateError as exc:
            return None, self._defer_unclaimed_resource(
                kind=kind,
                target_id=target_id,
                record=record,
                reason_code=exc.code,
                hard_failure=True,
                pressure_rung=resources.pressure_rung,
                wait_deadline_seconds=resources.policy.wait_deadline_seconds,
            )
        if sample.decision.status == "READY":
            return gate, None
        return None, self._defer_unclaimed_resource(
            kind=kind,
            target_id=target_id,
            record=record,
            reason_code=sample.decision.reason_code,
            hard_failure=sample.decision.hard_failure,
            pressure_rung=sample.decision.pressure_rung,
            wait_deadline_seconds=resources.policy.wait_deadline_seconds,
            gate_sample=sample,
        )

    def _defer_unclaimed_resource(
        self,
        *,
        kind: Literal["resolution", "build"],
        target_id: str,
        record: Mapping[str, Any],
        reason_code: str,
        hard_failure: bool,
        pressure_rung: int,
        wait_deadline_seconds: int,
        gate_sample: ResourceGateSample | None = None,
    ) -> CycleReport:
        observed = self._now()
        first_wait = self._resource_wait_started(kind, target_id) or observed
        waited_seconds = max(0.0, (observed - first_wait).total_seconds())
        timed_out = waited_seconds >= int(wait_deadline_seconds)
        terminal = hard_failure or timed_out
        effective_reason = "BLOCKED_RESOURCE_TIMEOUT" if timed_out and not hard_failure else reason_code
        context = {
            "resource_reason_code": reason_code,
            "pressure_rung": int(pressure_rung),
            "data_scope_changed": False,
            "waited_seconds": waited_seconds,
        }
        if gate_sample is not None:
            context.update(
                {
                    "host_available_bytes": gate_sample.snapshot.host.available_bytes,
                    "host_commit_headroom_bytes": (gate_sample.snapshot.host.commit_headroom_bytes),
                    "aggregate_owned_commit_bytes": (gate_sample.snapshot.owned.aggregate_commit_bytes),
                }
            )
        result = (
            ProcessorResult.blocked(effective_reason, context=context)
            if terminal
            else ProcessorResult.waiting(
                effective_reason,
                retry_after_seconds=self.retry_backoff_seconds,
                context=context,
            )
        )
        reference = self._error_ref(f"{kind}_resource_admission", target_id, result)
        stamp = _iso(observed)
        with self.store.transaction() as connection:
            if kind == "resolution":
                next_state = "BLOCKED_CONTRACT" if terminal else "WAITING_SOURCE"
                updated = connection.execute(
                    """
                    UPDATE submissions SET state=?,terminal_receipt_ref=?,next_retry_at=?,
                        row_version=row_version+1,updated_at=?
                    WHERE submission_id=? AND state='QUEUED_RESOLUTION'
                      AND resolution_attempt_id IS NULL
                    """,
                    (
                        next_state,
                        reference if terminal else None,
                        (None if terminal else _iso(observed + timedelta(seconds=self.retry_backoff_seconds))),
                        stamp,
                        target_id,
                    ),
                )
                event_kwargs = {"submission_id": target_id}
            else:
                current_state = str(record["state"])
                next_state = "BLOCKED_RESOURCE_TIMEOUT" if terminal else "WAITING_RESOURCE"
                if current_state == "QUEUED":
                    updated = connection.execute(
                        """
                        UPDATE runs SET state=?,outcome=?,terminal_receipt_ref=?,
                            row_version=row_version+1,updated_at=?
                        WHERE run_id=? AND state='QUEUED' AND active_attempt_id IS NULL
                        """,
                        (
                            next_state,
                            "BLOCKED" if terminal else None,
                            reference if terminal else None,
                            stamp,
                            target_id,
                        ),
                    )
                elif current_state == "WAITING_RESOURCE" and terminal:
                    updated = connection.execute(
                        """
                        UPDATE runs SET state='BLOCKED_RESOURCE_TIMEOUT',outcome='BLOCKED',
                            terminal_receipt_ref=?,row_version=row_version+1,updated_at=?
                        WHERE run_id=? AND state='WAITING_RESOURCE'
                          AND active_attempt_id IS NULL
                        """,
                        (reference, stamp, target_id),
                    )
                elif current_state == "WAITING_RESOURCE":
                    updated = None
                else:
                    raise StateConflict("resource admission record state changed")
                event_kwargs = {"run_id": target_id}
            if updated is not None and updated.rowcount != 1:
                raise LeaseConflict("resource admission transition lost its CAS")
            append_event(
                connection,
                event_type=f"RESOURCE_{next_state}",
                payload_ref=reference,
                created_at=stamp,
                **event_kwargs,
            )
        return CycleReport(True, kind, target_id, next_state, effective_reason)

    def _resource_wait_started(self, kind: Literal["resolution", "build"], target_id: str) -> datetime | None:
        key = "submission_id" if kind == "resolution" else "run_id"
        event_type = "RESOURCE_WAITING_SOURCE" if kind == "resolution" else "RESOURCE_WAITING_RESOURCE"
        rows = self.store._many(
            f"SELECT created_at FROM events WHERE {key}=? AND type=? ORDER BY event_id LIMIT 1",
            (target_id, event_type),
        )
        return _parse_time(str(rows[0]["created_at"])) if rows else None

    def _requeue_resource_ready(self, run_id: str) -> Mapping[str, Any]:
        stamp = _iso(self._now())
        with self.store.transaction() as connection:
            updated = connection.execute(
                """
                UPDATE runs SET state='QUEUED',row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='WAITING_RESOURCE' AND active_attempt_id IS NULL
                """,
                (stamp, run_id),
            )
            if updated.rowcount != 1:
                raise LeaseConflict("resource-ready requeue lost its CAS")
            append_event(
                connection,
                event_type="RESOURCE_READY_REQUEUED",
                run_id=run_id,
                created_at=stamp,
            )
            return dict(connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone())

    def _run_claimed(
        self,
        *,
        kind: Literal["resolution", "build"],
        target_id: str,
        record: Mapping[str, Any],
        claim: ClaimedAttempt,
        resources: WorkResourceSpec,
        processor: ResolutionProcessor | BuildProcessor,
        resource_gate: ResourceGate | None,
    ) -> CycleReport:
        heartbeat = _LeaseHeartbeatLoop(
            leases=self.leases,
            tokens=self._tokens(claim),
            ttl_seconds=self.lease_ttl_seconds,
            interval_seconds=resources.policy.enforcement_sample_seconds,
            now=self.now,
        )
        health_heartbeat = _WorkerHealthHeartbeatLoop(
            heartbeats=self.heartbeats,
            identity=self.identity,
            interval_seconds=self.heartbeat_interval_seconds,
            claim_kind=kind,
            claim_id=target_id,
            now=self.now,
        )
        ownership_uncertain: str | None = None
        heartbeat.start()
        try:
            health_heartbeat.start()
            supervisor = self.supervisor_factory(
                SupervisorRequest(
                    attempt_id=claim.attempt_id,
                    fence=claim.attempt_fence,
                    control_root=self.store.root,
                    policy=resources.policy,
                    hybrid_wsl=resources.hybrid_wsl,
                    resource_gate=resource_gate,
                )
            )
            with supervisor:
                context = WorkerAttemptContext(
                    kind=kind,
                    target_id=target_id,
                    record=record,
                    store=self.store,
                    claim=claim,
                    identity=self.identity,
                    supervisor=supervisor,
                    leases=self.leases,
                    commands=self.commands,
                    lease_ttl_seconds=self.lease_ttl_seconds,
                    pressure_rung=resources.pressure_rung,
                    credential_env_allowlist=resources.credential_env_allowlist,
                    stop_event=self.stop_event,
                    now=self.now,
                )
                context.checkpoint()
                result = processor.process(context)
                if not isinstance(result, ProcessorResult):
                    raise ProcessorContractViolation("processor must return a typed ProcessorResult")
                if result.disposition is not ProcessorDisposition.DURABLE_SUCCESS:
                    context.checkpoint()
        except CancellationRequested:
            result = ProcessorResult.cancelled()
        except CooperativeShutdown as exc:
            result = ProcessorResult.retryable(exc.code, retry_after_seconds=0)
        except ResourceCheckpointRequested as exc:
            result = ProcessorResult.waiting(
                exc.code,
                retry_after_seconds=self.retry_backoff_seconds,
                context=exc.context,
            )
        except ResourceSupervisorError as exc:
            ownership_uncertain = f"BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE:{type(exc).__name__}"
            result = ProcessorResult.blocked("BLOCKED_RESOURCE_ENFORCEMENT_UNAVAILABLE")
        except DatasetReleaseError as exc:
            result = (
                ProcessorResult.retryable(
                    exc.code,
                    retry_after_seconds=self.retry_backoff_seconds,
                    context=exc.context,
                )
                if exc.retryable
                else ProcessorResult.blocked(exc.code, context=exc.context)
            )
        except Exception as exc:
            result = ProcessorResult.blocked(
                "DATASET_RELEASE_PROCESSOR_UNHANDLED",
                context={"exception_type": type(exc).__name__},
            )
        health_heartbeat_error = health_heartbeat.stop()
        heartbeat_error = heartbeat.stop()
        # A processor may atomically commit its terminal transaction and then
        # fail while returning/closing its supervisor.  The terminal commit
        # also releases leases, so the lease heartbeat can legitimately lose
        # its CAS in that narrow window.  Resolve that ambiguity only through
        # an exact same-attempt/fence snapshot before considering an orphan
        # hold; the processor's in-memory result is never success authority.
        if kind == "resolution":
            durable_terminal_state = self._durable_resolution_success_readback(
                submission_id=target_id,
                claim=claim,
            )
        else:
            durable_terminal_state = (
                "SUCCEEDED" if self._durable_build_success_readback(run_id=target_id, claim=claim) else None
            )
        if durable_terminal_state is not None:
            result = ProcessorResult.durable_success()
        elif ownership_uncertain is not None or (
            heartbeat_error is not None and result.disposition is not ProcessorDisposition.DURABLE_SUCCESS
        ):
            # Setup/close/heartbeat errors cannot prove that every task-owned
            # child is quiescent. Retain ownership for a liveness readback.
            state = self._hold_claim_unknown(kind, target_id, claim)
            detail = ownership_uncertain or (f"BLOCKED_LEASE_HEARTBEAT_UNCERTAIN:{type(heartbeat_error).__name__}")
            return CycleReport(True, kind, target_id, state, detail)
        if durable_terminal_state is not None:
            state = durable_terminal_state
        elif kind == "resolution":
            state, result = self._finish_resolution(target_id, claim, result)
        else:
            state, result = self._finish_build(target_id, claim, result)
        detail = result.error_code
        if detail is None and health_heartbeat_error is not None:
            detail = f"WORKER_HEALTH_HEARTBEAT_UNAVAILABLE:{type(health_heartbeat_error).__name__}"
        return CycleReport(True, kind, target_id, state, detail)

    def _hold_claim_unknown(
        self,
        kind: Literal["resolution", "build"],
        target_id: str,
        claim: ClaimedAttempt,
    ) -> str:
        if kind == "resolution":
            self.leases.mark_resolution_orphan_hold(
                submission_id=target_id,
                resolution_attempt_id=claim.attempt_id,
                tree_status="unknown",
                now=self._now(),
            )
            return "WAITING_ORPHAN_QUIESCENCE"
        self.leases.mark_orphan_hold(
            run_id=target_id,
            attempt_id=claim.attempt_id,
            tree_status="unknown",
            now=self._now(),
        )
        run = self.store.get_run(target_id)
        return str(run["state"]) if run is not None else "WAITING_ORPHAN_QUIESCENCE"

    def _finish_resolution(
        self,
        submission_id: str,
        claim: ClaimedAttempt,
        result: ProcessorResult,
    ) -> tuple[str, ProcessorResult]:
        terminal_state = self._durable_resolution_success_readback(
            submission_id=submission_id,
            claim=claim,
        )
        if terminal_state is not None:
            return terminal_state, ProcessorResult.durable_success()
        if result.disposition is ProcessorDisposition.DURABLE_SUCCESS:
            result = ProcessorResult.blocked(
                ProcessorContractViolation.code,
                context={"reason": "resolution success lacked durable terminal readback"},
            )
        error_ref = self._error_ref("resolution", submission_id, result)
        observed = self._now()
        stamp = _iso(observed)
        if result.disposition is ProcessorDisposition.RETRYABLE:
            next_state = "FAILED_RETRYABLE"
            attempt_state = "RELEASED_RETRYABLE"
            next_retry_at = _iso(observed + timedelta(seconds=result.retry_after_seconds or 0))
        elif result.disposition is ProcessorDisposition.WAITING:
            next_state = "WAITING_SOURCE"
            attempt_state = "RELEASED_WAITING"
            next_retry_at = _iso(observed + timedelta(seconds=result.retry_after_seconds or 0))
        elif result.disposition is ProcessorDisposition.CANCELLED:
            next_state = "CANCELLED"
            attempt_state = "RELEASED_CANCELLED"
            next_retry_at = None
        else:
            next_state = result.terminal_state or "BLOCKED_CONTRACT"
            attempt_state = "FAILED_TERMINAL"
            next_retry_at = None
        tokens = self._tokens(claim)
        with self.store.transaction() as connection:
            submission = connection.execute(
                "SELECT * FROM submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
            attempt = connection.execute(
                "SELECT * FROM resolution_attempts WHERE resolution_attempt_id=?",
                (claim.attempt_id,),
            ).fetchone()
            if (
                submission is None
                or attempt is None
                or submission["state"] != "RESOLVING_SOURCE"
                or submission["resolution_attempt_id"] != claim.attempt_id
                or int(attempt["fence"]) != claim.attempt_fence
            ):
                raise ProcessorContractViolation("resolution ownership changed before completion")
            self.leases._release_exact(connection, tokens, observed=observed)
            connection.execute(
                """
                UPDATE resolution_attempts SET state=?,error_ref=?,updated_at=?
                WHERE resolution_attempt_id=? AND state IN ('CLAIMED','RUNNING')
                """,
                (attempt_state, error_ref, stamp, claim.attempt_id),
            )
            updated = connection.execute(
                """
                UPDATE submissions SET state=?,resolution_attempt_id=NULL,
                    terminal_receipt_ref=?,next_retry_at=?,row_version=row_version+1,updated_at=?
                WHERE submission_id=? AND state='RESOLVING_SOURCE'
                  AND resolution_attempt_id=?
                """,
                (
                    next_state,
                    error_ref if next_retry_at is None else None,
                    next_retry_at,
                    stamp,
                    submission_id,
                    claim.attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("resolution completion CAS failed")
            if result.disposition is ProcessorDisposition.CANCELLED:
                self.commands.complete_pending_cancel(
                    connection,
                    target_type="submission",
                    target_id=submission_id,
                    stamp=stamp,
                )
            append_event(
                connection,
                event_type=f"SUBMISSION_{next_state}",
                submission_id=submission_id,
                resolution_attempt_id=claim.attempt_id,
                payload_ref=error_ref,
                created_at=stamp,
            )
        return next_state, result

    def _durable_resolution_success_readback(
        self,
        *,
        submission_id: str,
        claim: ClaimedAttempt,
    ) -> str | None:
        terminal_states = {
            "RESOLVED_TO_EXISTING",
            "RESOLVED_NO_OP",
            "RESOLVED_NEW_RUN",
        }
        with self.store.transaction(immediate=False) as connection:
            submission = connection.execute(
                "SELECT * FROM submissions WHERE submission_id=?", (submission_id,)
            ).fetchone()
            attempt = connection.execute(
                """
                SELECT * FROM resolution_attempts
                WHERE resolution_attempt_id=? AND submission_id=?
                """,
                (claim.attempt_id, submission_id),
            ).fetchone()
            if submission is None or attempt is None:
                return None
            state = str(submission["state"])
            if state not in terminal_states:
                if attempt["state"] == "RELEASED_SUCCEEDED":
                    raise ProcessorContractViolation("released resolution attempt lacks its terminal submission")
                return None
            run = (
                connection.execute("SELECT * FROM runs WHERE run_id=?", (submission["run_id"],)).fetchone()
                if submission["run_id"]
                else None
            )
            residual = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM leases
                    WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')
                    """,
                    (claim.attempt_id,),
                ).fetchone()[0]
            )
            release_events = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM events
                    WHERE submission_id=? AND resolution_attempt_id=?
                      AND type='RESOLUTION_RELEASED_SUCCEEDED'
                    """,
                    (submission_id, claim.attempt_id),
                ).fetchone()[0]
            )
            terminal_events = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM events
                    WHERE submission_id=? AND run_id=? AND type=?
                    """,
                    (submission_id, submission["run_id"], state),
                ).fetchone()[0]
            )
            valid = (
                submission["resolution_attempt_id"] is None
                and submission["intent_id"] is not None
                and submission["run_id"] is not None
                and attempt["state"] == "RELEASED_SUCCEEDED"
                and int(attempt["fence"]) == int(claim.attempt_fence)
                and claim.resolution is not None
                and claim.resolution.attempt_id == claim.attempt_id
                and int(claim.resolution.fence) == int(claim.attempt_fence)
                and attempt["owner"] == claim.resolution.owner_identity
                and attempt["logical_request_key"] == submission["logical_request_key"]
                and run is not None
                and run["intent_id"] == submission["intent_id"]
                and residual == 0
                and release_events == 1
                and terminal_events == 1
            )
            if state == "RESOLVED_NO_OP":
                valid = valid and (
                    run is not None
                    and run["operation_kind"] == "NO_OP"
                    and run["state"] == "SUCCEEDED"
                    and run["outcome"] == "NO_OP_VERIFIED"
                    and bool(run["terminal_receipt_ref"])
                    and submission["terminal_receipt_ref"] == run["terminal_receipt_ref"]
                )
            if not valid:
                raise ProcessorContractViolation("resolution terminal readback is incomplete or mismatched")
            return state

    def _finish_build(
        self,
        run_id: str,
        claim: ClaimedAttempt,
        result: ProcessorResult,
    ) -> tuple[str, ProcessorResult]:
        if self._durable_build_success_readback(run_id=run_id, claim=claim):
            return "SUCCEEDED", ProcessorResult.durable_success()
        if result.disposition is ProcessorDisposition.DURABLE_SUCCESS:
            result = ProcessorResult.blocked(
                ProcessorContractViolation.code,
                context={"reason": "build success lacked durable publish/signoff readback"},
            )
        run = self.store.get_run(run_id)
        if run is None:
            raise ProcessorContractViolation("claimed build run disappeared")
        current_state = str(run["state"])
        if current_state in {"PUBLISHING", "WAITING_PUBLISH_RECOVERY"}:
            self.leases.mark_orphan_hold(
                run_id=run_id,
                attempt_id=claim.attempt_id,
                tree_status="unknown",
                now=self._now(),
            )
            return "WAITING_PUBLISH_RECOVERY", result
        if result.disposition is ProcessorDisposition.RETRYABLE:
            next_state = "FAILED_RETRYABLE"
            attempt_state = "RELEASED_RETRYABLE"
            outcome = None
        elif result.disposition is ProcessorDisposition.WAITING:
            next_state = "WAITING_RESOURCE"
            attempt_state = "RELEASED_WAITING"
            outcome = None
        elif result.disposition is ProcessorDisposition.CANCELLED:
            next_state = "CANCELLED"
            attempt_state = "RELEASED_CANCELLED"
            outcome = "CANCELLED"
        else:
            next_state = result.terminal_state or "FAILED_TERMINAL"
            attempt_state = "FAILED_TERMINAL"
            outcome = "BLOCKED"
        if result.disposition is ProcessorDisposition.CANCELLED:
            if "CANCEL_REQUESTED" not in RUN_TRANSITIONS.get(current_state, set()):
                raise ProcessorContractViolation(f"run cannot accept cancellation from {current_state}")
        elif next_state not in RUN_TRANSITIONS.get(current_state, set()):
            if "FAILED_TERMINAL" in RUN_TRANSITIONS.get(current_state, set()):
                next_state = "FAILED_TERMINAL"
                attempt_state = "FAILED_TERMINAL"
                outcome = "BLOCKED"
            else:
                self.leases.mark_orphan_hold(
                    run_id=run_id,
                    attempt_id=claim.attempt_id,
                    tree_status="unknown",
                    now=self._now(),
                )
                return "WAITING_ORPHAN_QUIESCENCE", result
        error_ref = self._error_ref("build", run_id, result)
        observed = self._now()
        stamp = _iso(observed)
        tokens = self._tokens(claim)
        with self.store.transaction() as connection:
            durable_run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (claim.attempt_id, run_id),
            ).fetchone()
            if (
                durable_run is None
                or attempt is None
                or durable_run["state"] != current_state
                or durable_run["active_attempt_id"] != claim.attempt_id
                or int(attempt["attempt_fence"]) != claim.attempt_fence
            ):
                raise ProcessorContractViolation("build ownership changed before completion")
            self.leases._release_exact(connection, tokens, observed=observed)
            connection.execute(
                "UPDATE attempts SET state=?,error_ref=?,updated_at=? WHERE attempt_id=?",
                (attempt_state, error_ref, stamp, claim.attempt_id),
            )
            if result.disposition is ProcessorDisposition.CANCELLED:
                append_event(
                    connection,
                    event_type="RUN_CANCEL_REQUESTED",
                    run_id=run_id,
                    attempt_id=claim.attempt_id,
                    payload_ref=error_ref,
                    created_at=stamp,
                )
            updated = connection.execute(
                """
                UPDATE runs SET state=?,outcome=?,active_attempt_id=NULL,
                    terminal_receipt_ref=?,row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state=? AND active_attempt_id=?
                """,
                (
                    next_state,
                    outcome,
                    error_ref if next_state in TERMINAL_RUN_STATES else None,
                    stamp,
                    run_id,
                    current_state,
                    claim.attempt_id,
                ),
            )
            if updated.rowcount != 1:
                raise StateConflict("build completion CAS failed")
            if result.disposition is ProcessorDisposition.CANCELLED:
                self.commands.complete_pending_cancel(
                    connection,
                    target_type="run",
                    target_id=run_id,
                    stamp=stamp,
                )
            append_event(
                connection,
                event_type=f"RUN_{next_state}",
                run_id=run_id,
                attempt_id=claim.attempt_id,
                payload_ref=error_ref,
                created_at=stamp,
            )
            residual = connection.execute(
                "SELECT COUNT(*) FROM leases WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')",
                (claim.attempt_id,),
            ).fetchone()[0]
            if int(residual):
                raise StateConflict("completed build retained active leases")
        return next_state, result

    def _durable_build_success_readback(
        self,
        *,
        run_id: str,
        claim: ClaimedAttempt,
    ) -> bool:
        with self.store.transaction(immediate=False) as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (claim.attempt_id, run_id),
            ).fetchone()
            if run is None or attempt is None:
                return False
            if run["state"] != "SUCCEEDED":
                if attempt["state"] == "RELEASED_SUCCEEDED":
                    raise ProcessorContractViolation("released build attempt lacks its terminal run")
                return False
            residual = int(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM leases
                    WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')
                    """,
                    (claim.attempt_id,),
                ).fetchone()[0]
            )
            common = (
                run["active_attempt_id"] is None
                and attempt["state"] == "RELEASED_SUCCEEDED"
                and int(attempt["attempt_fence"]) == int(claim.attempt_fence)
                and claim.host is not None
                and claim.release is not None
                and claim.host.attempt_id == claim.attempt_id
                and claim.release.attempt_id == claim.attempt_id
                and claim.host.owner_identity == claim.release.owner_identity
                and attempt["owner"] == claim.host.owner_identity
                and int(attempt["host_fence"] or 0) == int(claim.host.fence)
                and int(attempt["release_fence"] or 0) == int(claim.release.fence)
                and bool(run["terminal_receipt_ref"])
                and bool(run["candidate_identity"])
                and bool(run["artifact_root"])
                and residual == 0
            )
            if not common:
                raise ProcessorContractViolation("build terminal readback is incomplete or mismatched")
            if run["operation_kind"] == "REATTEST":
                intent = connection.execute("SELECT * FROM intents WHERE intent_id=?", (run["intent_id"],)).fetchone()
                attestation = connection.execute(
                    """
                    SELECT * FROM attestations
                    WHERE receipt_ref=? AND committed=1
                      AND candidate_identity=? AND candidate_artifact_root=?
                    """,
                    (
                        run["terminal_receipt_ref"],
                        run["candidate_identity"],
                        run["artifact_root"],
                    ),
                ).fetchall()
                event_count = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM events
                        WHERE run_id=? AND attempt_id=? AND type='RUN_REATTESTED'
                          AND payload_ref=?
                        """,
                        (run_id, claim.attempt_id, run["terminal_receipt_ref"]),
                    ).fetchone()[0]
                )
                valid = (
                    attempt["attempt_kind"] == "REATTEST"
                    and run["outcome"] == "REATTESTED"
                    and len(attestation) == 1
                    and intent is not None
                    and attestation[0]["subject_type"] == "candidate"
                    and attestation[0]["subject_digest"] == run["candidate_identity"]
                    and attestation[0]["current_source_content_root"] == intent["source_content_root"]
                    and attestation[0]["pit_snapshot_digest"] == intent["pit_snapshot_digest"]
                    and attestation[0]["outcome"]
                    in {
                        "CURRENT_SOURCE_EQUIVALENT",
                        "CURRENT_SOURCE_EQUIVALENT_RECONSTRUCTED",
                    }
                    and event_count == 1
                )
            else:
                release = connection.execute(
                    "SELECT * FROM releases WHERE run_id=? AND state='COMMITTED'",
                    (run_id,),
                ).fetchone()
                publish = connection.execute(
                    "SELECT * FROM publish_records WHERE run_id=? AND state='COMMITTED'",
                    (run_id,),
                ).fetchone()
                registration = (
                    connection.execute(
                        """
                        SELECT * FROM candidate_registrations
                        WHERE registration_id=? AND state IN ('RELEASED','ATTESTED')
                        """,
                        (publish["registration_id"],),
                    ).fetchone()
                    if publish is not None
                    else None
                )
                attestation = (
                    connection.execute(
                        """
                        SELECT * FROM attestations
                        WHERE attestation_key=? AND receipt_ref=? AND committed=1
                        """,
                        (publish["attestation_key"], publish["attestation_ref"]),
                    ).fetchone()
                    if publish is not None
                    else None
                )
                event_count = int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM events
                        WHERE run_id=? AND attempt_id=? AND type='CANDIDATE_VALIDATED'
                          AND payload_ref=?
                        """,
                        (run_id, claim.attempt_id, run["terminal_receipt_ref"]),
                    ).fetchone()[0]
                )
                valid = (
                    run["operation_kind"] in {"BUILD", "RESUME_BUILD"}
                    and attempt["attempt_kind"] == "BUILD"
                    and run["outcome"] == "CANDIDATE_VALIDATED"
                    and release is not None
                    and publish is not None
                    and registration is not None
                    and attestation is not None
                    and publish["attempt_id"] == claim.attempt_id
                    and int(publish["attempt_fence"]) == int(claim.attempt_fence)
                    and int(publish["host_fence"]) == int(claim.host.fence)
                    and int(publish["release_fence"]) == int(claim.release.fence)
                    and publish["published_by_attempt_id"] == claim.attempt_id
                    and int(publish["published_by_fence"]) == int(claim.attempt_fence)
                    and publish["finalized_by_attempt_id"] == claim.attempt_id
                    and int(publish["finalized_by_fence"] or 0) == int(claim.attempt_fence)
                    and release["release_id"] == publish["release_id"]
                    and release["candidate_identity"] == run["candidate_identity"]
                    and release["artifact_root"] == run["artifact_root"]
                    and publish["candidate_identity"] == run["candidate_identity"]
                    and publish["artifact_root"] == run["artifact_root"]
                    and publish["build_receipt_ref"] == run["terminal_receipt_ref"]
                    and registration["candidate_identity"] == run["candidate_identity"]
                    and registration["artifact_root"] == run["artifact_root"]
                    and attestation["candidate_identity"] == run["candidate_identity"]
                    and attestation["candidate_artifact_root"] == run["artifact_root"]
                    and release["attestation_id"] == attestation["attestation_id"]
                    and event_count == 1
                )
            if not valid:
                raise ProcessorContractViolation("build terminal receipt/publish readback is incomplete or mismatched")
            return True

    def _block_unclaimed(
        self,
        *,
        kind: Literal["resolution", "build"],
        target_id: str,
        error: DatasetReleaseError,
    ) -> CycleReport:
        result = ProcessorResult.blocked(error.code, context=error.context)
        reference = self._error_ref(kind, target_id, result)
        stamp = _iso(self._now())
        with self.store.transaction() as connection:
            if kind == "resolution":
                updated = connection.execute(
                    """
                    UPDATE submissions SET state='BLOCKED_CONTRACT',terminal_receipt_ref=?,
                        row_version=row_version+1,updated_at=?
                    WHERE submission_id=? AND state='QUEUED_RESOLUTION'
                      AND resolution_attempt_id IS NULL
                    """,
                    (reference, stamp, target_id),
                )
                event_kwargs = {"submission_id": target_id}
                state = "BLOCKED_CONTRACT"
            else:
                updated = connection.execute(
                    """
                    UPDATE runs SET state='BLOCKED_VERSION_MISMATCH',outcome='BLOCKED',
                        terminal_receipt_ref=?,row_version=row_version+1,updated_at=?
                    WHERE run_id=? AND state='QUEUED' AND active_attempt_id IS NULL
                    """,
                    (reference, stamp, target_id),
                )
                event_kwargs = {"run_id": target_id}
                state = "BLOCKED_VERSION_MISMATCH"
            if updated.rowcount != 1:
                raise LeaseConflict("unclaimed processor block lost its CAS")
            append_event(
                connection,
                event_type="BLOCKED_PROCESSOR_UNAVAILABLE",
                payload_ref=reference,
                created_at=stamp,
                **event_kwargs,
            )
        return CycleReport(True, kind, target_id, state, error.code)

    def _requeue_one_due_retry(self) -> bool:
        now = self._now()
        stamp = _iso(now)
        with self.store.transaction() as connection:
            submission = connection.execute(
                """
                SELECT * FROM submissions
                WHERE state IN ('FAILED_RETRYABLE','WAITING_SOURCE')
                  AND next_retry_at IS NOT NULL AND next_retry_at<=?
                ORDER BY next_retry_at,submission_id LIMIT 1
                """,
                (stamp,),
            ).fetchone()
            if submission is not None:
                resolution_rows = connection.execute(
                    """
                    SELECT * FROM resolution_attempts
                    WHERE submission_id=? ORDER BY ordinal DESC
                    """,
                    (submission["submission_id"],),
                ).fetchall()
                attempts = len(resolution_rows)
                next_state = "BLOCKED_RETRY_EXHAUSTED" if attempts >= self.max_attempts else "QUEUED_RESOLUTION"
                terminal_ref = None
                if next_state == "BLOCKED_RETRY_EXHAUSTED":
                    terminal_ref = self._verified_retry_exhaustion_ref(
                        kind="resolution",
                        target_id=str(submission["submission_id"]),
                        row=resolution_rows[0] if resolution_rows else None,
                        expected_attempt_state=(
                            "RELEASED_WAITING" if submission["state"] == "WAITING_SOURCE" else "RELEASED_RETRYABLE"
                        ),
                    )
                connection.execute(
                    """
                    UPDATE submissions SET state=?,terminal_receipt_ref=?,next_retry_at=NULL,
                        row_version=row_version+1,updated_at=? WHERE submission_id=?
                    """,
                    (next_state, terminal_ref, stamp, submission["submission_id"]),
                )
                append_event(
                    connection,
                    event_type=f"SUBMISSION_{next_state}",
                    submission_id=submission["submission_id"],
                    created_at=stamp,
                )
                return True
            run = connection.execute(
                """
                SELECT * FROM runs WHERE state='FAILED_RETRYABLE'
                  AND active_attempt_id IS NULL ORDER BY updated_at,run_id LIMIT 1
                """
            ).fetchone()
            if run is None:
                return False
            updated_at = _parse_time(str(run["updated_at"]))
            if now < updated_at + timedelta(seconds=self.retry_backoff_seconds):
                return False
            attempt_rows = connection.execute(
                "SELECT * FROM attempts WHERE run_id=? ORDER BY ordinal DESC",
                (run["run_id"],),
            ).fetchall()
            attempts = len(attempt_rows)
            next_state = "BLOCKED_RETRY_EXHAUSTED" if attempts >= self.max_attempts else "QUEUED"
            terminal_ref = None
            if next_state == "BLOCKED_RETRY_EXHAUSTED":
                terminal_ref = self._verified_retry_exhaustion_ref(
                    kind="build",
                    target_id=str(run["run_id"]),
                    row=attempt_rows[0] if attempt_rows else None,
                    expected_attempt_state="RELEASED_RETRYABLE",
                )
            connection.execute(
                """
                UPDATE runs SET state=?,outcome=?,terminal_receipt_ref=?,
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='FAILED_RETRYABLE' AND active_attempt_id IS NULL
                """,
                (
                    next_state,
                    "BLOCKED" if next_state.startswith("BLOCKED_") else None,
                    terminal_ref,
                    stamp,
                    run["run_id"],
                ),
            )
            append_event(
                connection,
                event_type=f"RUN_{next_state}",
                run_id=run["run_id"],
                created_at=stamp,
            )
            return True

    def _verified_retry_exhaustion_ref(
        self,
        *,
        kind: Literal["resolution", "build"],
        target_id: str,
        row: Mapping[str, Any] | None,
        expected_attempt_state: str,
    ) -> str:
        if row is None or row["state"] != expected_attempt_state or not str(row["error_ref"] or "").strip():
            raise WorkerError(
                "retry exhaustion lacks the latest owned attempt error receipt",
                code="BLOCKED_RETRY_EXHAUSTION_EVIDENCE_INVALID",
            )
        try:
            reference = self.cas.verify(str(row["error_ref"]))
            receipt = self.cas.get_json_bounded(reference, max_bytes=1024 * 1024)
        except Exception as exc:
            raise WorkerError(
                "retry exhaustion attempt error receipt is unavailable",
                code="BLOCKED_RETRY_EXHAUSTION_EVIDENCE_INVALID",
            ) from exc
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("schema_version") != WORKER_ERROR_RECEIPT_SCHEMA
            or receipt.get("kind") != kind
            or receipt.get("target_id") != target_id
            or receipt.get("disposition")
            not in {ProcessorDisposition.RETRYABLE.value, ProcessorDisposition.WAITING.value}
            or not str(receipt.get("error_code") or "").strip()
        ):
            raise WorkerError(
                "retry exhaustion attempt error receipt identity differs",
                code="BLOCKED_RETRY_EXHAUSTION_EVIDENCE_INVALID",
            )
        return reference.sha256

    def _reconcile_one_orphan(self) -> CycleReport | None:
        now = self._now()
        stamp = _iso(now)
        rows = self.store._many(
            """
            SELECT * FROM leases
            WHERE state IN ('ACTIVE','ORPHAN_HOLD') AND expires_at IS NOT NULL AND expires_at<=?
              AND attempt_kind!='COMMAND'
            ORDER BY expires_at,resource_key LIMIT 1
            """,
            (stamp,),
        )
        if not rows:
            return None
        lease = rows[0]
        owner = LeaseOwnerSnapshot(
            attempt_id=str(lease["attempt_id"]),
            attempt_kind=str(lease["attempt_kind"]),
            owner_identity=str(lease["owner_identity"]),
            host=str(lease["host"]) if lease["host"] else None,
            owner_pid=int(lease["owner_pid"]) if lease["owner_pid"] is not None else None,
            owner_create_time=(str(lease["owner_create_time"]) if lease["owner_create_time"] else None),
            worker_instance_id=(str(lease["worker_instance_id"]) if lease["worker_instance_id"] else None),
            code_sha=str(lease["code_sha"]) if lease["code_sha"] else None,
            capability_digest=(str(lease["capability_digest"]) if lease["capability_digest"] else None),
            hybrid_wsl=bool(lease["hybrid_wsl"]),
            expires_at=str(lease["expires_at"]),
            lease_state=str(lease["state"]),
        )
        status = self.liveness_probe(owner)
        if status not in {"alive", "dead", "unknown"}:
            status = "unknown"
        resolution = self.store.get_resolution_attempt(owner.attempt_id)
        if resolution is not None:
            submission_id = str(resolution["submission_id"])
            if owner.lease_state == "ACTIVE":
                self.leases.mark_resolution_orphan_hold(
                    submission_id=submission_id,
                    resolution_attempt_id=owner.attempt_id,
                    tree_status="alive" if status == "alive" else "unknown",
                    now=now,
                )
            if status == "dead":
                self.leases.release_resolution_orphan_after_quiescence(
                    submission_id=submission_id,
                    resolution_attempt_id=owner.attempt_id,
                    tree_quiescent=True,
                    now=now,
                )
                state = "QUEUED_RESOLUTION"
            else:
                state = "WAITING_ORPHAN_QUIESCENCE"
            return CycleReport(True, "orphan_resolution", owner.attempt_id, state, status)
        attempt = self.store.get_attempt(owner.attempt_id)
        if attempt is None:
            raise WorkerError(
                "expired lease references no durable attempt",
                code="BLOCKED_ORPHAN_IDENTITY_MISSING",
            )
        run_id = str(attempt["run_id"])
        run = self.store.get_run(run_id)
        if run is None:
            raise WorkerError(
                "orphan attempt references no run",
                code="BLOCKED_ORPHAN_IDENTITY_MISSING",
            )
        if owner.lease_state == "ACTIVE":
            self.leases.mark_orphan_hold(
                run_id=run_id,
                attempt_id=owner.attempt_id,
                tree_status="alive" if status == "alive" else "unknown",
                now=now,
            )
            run = self.store.get_run(run_id)
        assert run is not None
        if run["state"] == "WAITING_PUBLISH_RECOVERY":
            if status != "dead":
                return CycleReport(
                    True,
                    "orphan_publish",
                    owner.attempt_id,
                    "WAITING_PUBLISH_RECOVERY",
                    status,
                )
            return self._recover_publish_orphan(
                run=run,
                old_attempt_id=owner.attempt_id,
                observed=now,
            )
        if status == "dead":
            self.leases.release_orphan_after_quiescence(
                run_id=run_id,
                attempt_id=owner.attempt_id,
                tree_quiescent=True,
                now=now,
            )
            state = "QUEUED"
        else:
            state = "WAITING_ORPHAN_QUIESCENCE"
        return CycleReport(True, "orphan_build", owner.attempt_id, state, status)

    def _recover_publish_orphan(
        self,
        *,
        run: Mapping[str, Any],
        old_attempt_id: str,
        observed: datetime,
    ) -> CycleReport:
        """Adopt a quiescent publish attempt without exposing a FREE lease window."""

        run_id = str(run["run_id"])
        provider = self.registry.publish_recovery
        if provider is None:
            return CycleReport(
                True,
                "orphan_publish",
                old_attempt_id,
                "WAITING_PUBLISH_RECOVERY",
                "BLOCKED_PUBLISH_RECOVERY_PROVIDER_UNAVAILABLE",
            )
        try:
            claim = self.leases.handoff_publish_finalizer(
                run_id=run_id,
                old_attempt_id=old_attempt_id,
                new_owner_identity=self.identity.owner_identity,
                ttl_seconds=self.lease_ttl_seconds,
                tree_quiescent=True,
                host=self.identity.host,
                owner_pid=self.identity.pid,
                owner_create_time=self.identity.process_create_time,
                worker_instance_id=self.identity.instance_id,
                code_sha=self.identity.code_sha,
                capability_digest=self.identity.capability_digest,
                now=observed,
            )
        except LeaseConflict as exc:
            return CycleReport(
                False,
                "orphan_publish",
                old_attempt_id,
                "CLAIM_RACE",
                type(exc).__name__,
            )

        heartbeat = _LeaseHeartbeatLoop(
            leases=self.leases,
            tokens=self._tokens(claim),
            ttl_seconds=self.lease_ttl_seconds,
            interval_seconds=max(1.0, self.lease_ttl_seconds / 4),
            now=self.now,
        )
        health_heartbeat = _WorkerHealthHeartbeatLoop(
            heartbeats=self.heartbeats,
            identity=self.identity,
            interval_seconds=self.heartbeat_interval_seconds,
            claim_kind="orphan_publish",
            claim_id=run_id,
            now=self.now,
        )

        def stop_heartbeats() -> tuple[BaseException | None, BaseException | None]:
            health_error = health_heartbeat.stop()
            lease_error = heartbeat.stop()
            return lease_error, health_error

        try:
            heartbeat.start()
            health_heartbeat.start()
            provider.recover_and_finalize(run=self.store.get_run(run_id) or run, claim=claim)
        except PublishRecoveryConflict as exc:
            heartbeat_error, _health_error = stop_heartbeats()
            if heartbeat_error is not None:
                return self._hold_publish_recovery(
                    run_id=run_id,
                    claim=claim,
                    detail="BLOCKED_PUBLISH_RECOVERY_HEARTBEAT_UNCERTAIN",
                )
            return self._block_publish_recovery_conflict(
                run_id=run_id,
                claim=claim,
                error=exc,
            )
        except PublishConflict as exc:
            heartbeat_error, _health_error = stop_heartbeats()
            if heartbeat_error is not None:
                return self._hold_publish_recovery(
                    run_id=run_id,
                    claim=claim,
                    detail="BLOCKED_PUBLISH_RECOVERY_HEARTBEAT_UNCERTAIN",
                )
            return self._block_publish_recovery_conflict(
                run_id=run_id,
                claim=claim,
                error=PublishRecoveryConflict(
                    "immutable publish identity or marker mismatched",
                    context={"exception_type": type(exc).__name__},
                ),
            )
        except Exception as exc:
            stop_heartbeats()
            durable = self.store.get_run(run_id)
            if durable is not None and durable["state"] == "SUCCEEDED":
                return CycleReport(
                    True,
                    "orphan_publish",
                    claim.attempt_id,
                    "SUCCEEDED",
                    None,
                )
            return self._hold_publish_recovery(
                run_id=run_id,
                claim=claim,
                detail=f"PUBLISH_RECOVERY_UNCERTAIN:{type(exc).__name__}",
            )

        heartbeat_error, health_heartbeat_error = stop_heartbeats()
        if heartbeat_error is not None:
            return self._hold_publish_recovery(
                run_id=run_id,
                claim=claim,
                detail="BLOCKED_PUBLISH_RECOVERY_HEARTBEAT_UNCERTAIN",
            )
        durable = self.store.get_run(run_id)
        attempt = self.store.get_attempt(claim.attempt_id)
        if (
            durable is None
            or attempt is None
            or durable["state"] != "SUCCEEDED"
            or durable["active_attempt_id"] is not None
            or attempt["state"] != "RELEASED_SUCCEEDED"
            or self._active_lease_count(claim.attempt_id)
        ):
            return self._hold_publish_recovery(
                run_id=run_id,
                claim=claim,
                detail="BLOCKED_PUBLISH_RECOVERY_DURABLE_READBACK",
            )
        detail = None
        if health_heartbeat_error is not None:
            detail = f"WORKER_HEALTH_HEARTBEAT_UNAVAILABLE:{type(health_heartbeat_error).__name__}"
        return CycleReport(
            True,
            "orphan_publish",
            claim.attempt_id,
            "SUCCEEDED",
            detail,
        )

    def _hold_publish_recovery(
        self,
        *,
        run_id: str,
        claim: ClaimedAttempt,
        detail: str,
    ) -> CycleReport:
        durable = self.store.get_run(run_id)
        if (
            durable is not None
            and durable["state"] == "PUBLISHING"
            and durable["active_attempt_id"] == claim.attempt_id
        ):
            self.leases.mark_orphan_hold(
                run_id=run_id,
                attempt_id=claim.attempt_id,
                tree_status="unknown",
                now=self._now(),
            )
        current = self.store.get_run(run_id)
        return CycleReport(
            True,
            "orphan_publish",
            claim.attempt_id,
            str(current["state"]) if current is not None else "WAITING_PUBLISH_RECOVERY",
            detail,
        )

    def _block_publish_recovery_conflict(
        self,
        *,
        run_id: str,
        claim: ClaimedAttempt,
        error: PublishRecoveryConflict,
    ) -> CycleReport:
        result = ProcessorResult.blocked(error.code, context=error.context)
        error_ref = self._error_ref("publish_recovery", run_id, result)
        observed = self._now()
        stamp = _iso(observed)
        tokens = self._tokens(claim)
        with self.store.transaction() as connection:
            run = connection.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
            if run is not None and run["state"] == "BLOCKED_PUBLISH_CONFLICT":
                return CycleReport(
                    True,
                    "orphan_publish",
                    claim.attempt_id,
                    "BLOCKED_PUBLISH_CONFLICT",
                    error.code,
                )
            attempt = connection.execute(
                "SELECT * FROM attempts WHERE attempt_id=? AND run_id=?",
                (claim.attempt_id, run_id),
            ).fetchone()
            if (
                run is None
                or attempt is None
                or run["state"] != "PUBLISHING"
                or run["active_attempt_id"] != claim.attempt_id
                or attempt["state"] not in {"CLAIMED", "RUNNING"}
            ):
                raise StateConflict("publish recovery ownership changed before conflict block")
            self.leases._release_exact(connection, tokens, observed=observed)
            connection.execute(
                """
                UPDATE publish_records SET state='CONFLICT',updated_at=?
                WHERE run_id=? AND state IN ('PREPARED','FILES_COMMITTED')
                """,
                (stamp, run_id),
            )
            connection.execute(
                "UPDATE attempts SET state='FAILED_TERMINAL',error_ref=?,updated_at=? WHERE attempt_id=?",
                (error_ref, stamp, claim.attempt_id),
            )
            updated = connection.execute(
                """
                UPDATE runs SET state='BLOCKED_PUBLISH_CONFLICT',outcome='BLOCKED',
                    terminal_receipt_ref=?,active_attempt_id=NULL,
                    row_version=row_version+1,updated_at=?
                WHERE run_id=? AND state='PUBLISHING' AND active_attempt_id=?
                """,
                (error_ref, stamp, run_id, claim.attempt_id),
            )
            if updated.rowcount != 1:
                raise StateConflict("publish recovery conflict CAS failed")
            append_event(
                connection,
                event_type="BLOCKED_PUBLISH_CONFLICT",
                run_id=run_id,
                attempt_id=claim.attempt_id,
                payload_ref=error_ref,
                created_at=stamp,
            )
        return CycleReport(
            True,
            "orphan_publish",
            claim.attempt_id,
            "BLOCKED_PUBLISH_CONFLICT",
            error.code,
        )

    def _next_submission(self) -> Mapping[str, Any] | None:
        rows = self.store._many(
            """
            SELECT * FROM submissions WHERE state='QUEUED_RESOLUTION'
              AND resolution_attempt_id IS NULL ORDER BY created_at,submission_id LIMIT 20
            """,
            (),
        )
        for row in rows:
            return row
        return None

    def _next_run(self) -> Mapping[str, Any] | None:
        rows = self.store._many(
            """
            SELECT * FROM runs WHERE state IN ('QUEUED','WAITING_RESOURCE')
              AND active_attempt_id IS NULL
              ORDER BY created_at,run_id LIMIT 20
            """,
            (),
        )
        for row in rows:
            return row
        return None

    def _error_ref(self, kind: str, target_id: str, result: ProcessorResult) -> str:
        reference = self.cas.put_json(
            {
                "schema_version": WORKER_ERROR_RECEIPT_SCHEMA,
                "kind": kind,
                "target_id": target_id,
                "worker_instance_id": self.identity.instance_id,
                "capability_digest": self.identity.capability_digest,
                "disposition": result.disposition.value,
                "error_code": result.error_code,
                "retry_after_seconds": result.retry_after_seconds,
                "terminal_state": result.terminal_state,
                "context": _bounded_context(result.context),
                "observed_at": _iso(self._now()),
                "safety": {
                    "database_writes": 0,
                    "production_writes": 0,
                    "production_deletes": 0,
                    "production_pointer_changes": 0,
                    "service_process_controls": 0,
                },
            }
        )
        self.cas.verify(reference)
        return reference.sha256

    def _heartbeat(self, report: CycleReport) -> None:
        observed_at = self._now()
        if (
            not report.claimed
            and report.state == self._last_heartbeat_state
            and (observed_at - self._last_heartbeat_at).total_seconds() < self.heartbeat_interval_seconds
        ):
            return
        self.heartbeats.write(
            self.identity,
            status=report.state,
            observed_at=observed_at,
            claim_kind=report.kind if report.identity is not None else None,
            claim_id=report.identity,
            stop_requested=self.stop_event.is_set(),
        )
        self._last_heartbeat_at = observed_at
        self._last_heartbeat_state = report.state

    def _validate_ttl(self, policy: ResourcePolicy) -> None:
        if self.lease_ttl_seconds < 3 * policy.enforcement_sample_seconds:
            raise ProcessorContractViolation("lease TTL must be at least three resource heartbeat intervals")

    @staticmethod
    def _validate_credential_authority(kind: Literal["resolution", "build"], resources: WorkResourceSpec) -> None:
        if kind == "build" and resources.credential_env_allowlist:
            raise ProcessorContractViolation("build processor cannot request source credentials")

    def _active_lease_count(self, attempt_id: str) -> int:
        rows = self.store._many(
            "SELECT resource_key FROM leases WHERE attempt_id=? AND state IN ('ACTIVE','ORPHAN_HOLD')",
            (attempt_id,),
        )
        return len(rows)

    @staticmethod
    def _tokens(claim: ClaimedAttempt) -> tuple[LeaseToken, ...]:
        return tuple(token for token in (claim.resolution, claim.host, claim.release) if token is not None)

    def _now(self) -> datetime:
        return _utc(self.now())


def _bounded_context(value: Mapping[str, Any]) -> dict[str, Any]:
    bounded: dict[str, Any] = {}
    for raw_key, raw_value in sorted(value.items(), key=lambda item: str(item[0]))[:32]:
        key = str(raw_key)[:100]
        if any(marker in key.casefold() for marker in ("password", "token", "secret", "private_key", "dsn")):
            bounded[key] = "<redacted>"
        elif isinstance(raw_value, (bool, int, float)) or raw_value is None:
            bounded[key] = raw_value
        elif isinstance(raw_value, str):
            bounded[key] = raw_value[:500]
        else:
            bounded[key] = json.dumps(raw_value, default=str, sort_keys=True)[:500]
    return bounded


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise WorkerError("Worker clock must return timezone-aware timestamps")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _parse_time(value: str) -> datetime:
    return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


__all__ = [
    "AttemptSupervisor",
    "BuildProcessor",
    "CancellationRequested",
    "CooperativeShutdown",
    "CycleReport",
    "DatasetReleaseWorker",
    "LeaseOwnerSnapshot",
    "MAX_DRAIN_JOBS",
    "ProcessorContractViolation",
    "ProcessorDisposition",
    "ProcessorRegistry",
    "ProcessorResult",
    "ProcessorUnavailable",
    "ResourceGateFactory",
    "PublishRecoveryConflict",
    "PublishRecoveryProvider",
    "ResolutionProcessor",
    "SupervisorRequest",
    "WorkResourceSpec",
    "WslQuiescenceProvider",
    "WorkerAttemptContext",
    "WorkerError",
    "unknown_liveness",
]
