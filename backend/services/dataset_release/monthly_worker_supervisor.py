"""FastAPI-owned supervisor for the durable monthly release worker process.

The HTTP process never performs a dataset build in a request thread.  When
explicitly enabled, one code-owned child process drains the same durable
operation store used by API, CLI, and MCP.  The child accepts no producer
commands or caller-selected paths; all wiring still comes from the validated
monthly runtime settings.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Callable, Mapping, Protocol, Sequence

from .monthly_worker_runtime import MonthlyWorkerRuntime, build_monthly_worker_runtime


LOGGER = logging.getLogger("aistock.dataset_release.monthly_worker_supervisor")
MONTHLY_WORKER_ENABLED_ENV = "AISTOCK_MONTHLY_RELEASE_WORKER_ENABLED"
MONTHLY_WORKER_POLL_SECONDS_ENV = "AISTOCK_MONTHLY_RELEASE_WORKER_POLL_SECONDS"
MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV = (
    "AISTOCK_MONTHLY_RELEASE_WORKER_SHUTDOWN_SECONDS"
)


class MonthlyWorkerSupervisorError(RuntimeError):
    """The explicitly enabled monthly worker cannot be supervised safely."""


class WorkerProcess(Protocol):
    pid: int
    returncode: int | None

    def poll(self) -> int | None: ...

    def terminate(self) -> None: ...

    def kill(self) -> None: ...

    def wait(self, timeout: float | None = None) -> int: ...


def _truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _bounded_float(
    raw: str | None,
    *,
    default: float,
    minimum: float,
    maximum: float,
    label: str,
) -> float:
    try:
        value = default if raw is None or not raw.strip() else float(raw)
    except ValueError as exc:
        raise MonthlyWorkerSupervisorError(f"{label} must be numeric") from exc
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise MonthlyWorkerSupervisorError(
            f"{label} must be finite and within {minimum}..{maximum}"
        )
    return value


@dataclass(frozen=True, slots=True)
class MonthlyWorkerSupervisorConfig:
    enabled: bool = False
    poll_seconds: float = 5.0
    shutdown_seconds: float = 30.0

    def __post_init__(self) -> None:
        for value, minimum, maximum, label in (
            (self.poll_seconds, 0.1, 300.0, "poll_seconds"),
            (self.shutdown_seconds, 1.0, 300.0, "shutdown_seconds"),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or not minimum <= float(value) <= maximum
            ):
                raise MonthlyWorkerSupervisorError(
                    f"{label} must be finite and within {minimum}..{maximum}"
                )

    @classmethod
    def from_env(cls) -> "MonthlyWorkerSupervisorConfig":
        enabled = _truthy(os.getenv(MONTHLY_WORKER_ENABLED_ENV))
        if not enabled:
            return cls(enabled=False)
        return cls(
            enabled=True,
            poll_seconds=_bounded_float(
                os.getenv(MONTHLY_WORKER_POLL_SECONDS_ENV),
                default=5.0,
                minimum=0.1,
                maximum=300.0,
                label=MONTHLY_WORKER_POLL_SECONDS_ENV,
            ),
            shutdown_seconds=_bounded_float(
                os.getenv(MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV),
                default=30.0,
                minimum=1.0,
                maximum=300.0,
                label=MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV,
            ),
        )


class MonthlyWorkerProcessSupervisor:
    """Start and stop the fixed monthly worker child exactly once per backend."""

    def __init__(
        self,
        *,
        project_root: Path,
        config: MonthlyWorkerSupervisorConfig,
        runtime_loader: Callable[..., MonthlyWorkerRuntime] = (
            build_monthly_worker_runtime
        ),
        popen_factory: Callable[..., WorkerProcess] = subprocess.Popen,
    ) -> None:
        self._project_root = project_root.resolve(strict=True)
        self._config = config
        self._runtime_loader = runtime_loader
        self._popen_factory = popen_factory
        self._process: WorkerProcess | None = None
        self._preflight: Mapping[str, Any] | None = None

    @property
    def process(self) -> WorkerProcess | None:
        return self._process

    def start(self) -> Mapping[str, Any]:
        if not self._config.enabled:
            return {
                "status": "DISABLED",
                "enabled": False,
                "pid": None,
            }
        if self._process is not None:
            previous_returncode = self._process.poll()
            if previous_returncode is None:
                raise MonthlyWorkerSupervisorError("monthly worker is already running")
            self._process.wait(timeout=0)
            self._process = None

        runtime = self._runtime_loader(project_root=self._project_root)
        preflight = runtime.preflight_receipt()
        if preflight.get("status") != "PASS":
            raise MonthlyWorkerSupervisorError("monthly worker preflight did not pass")
        script = (
            self._project_root
            / "scripts"
            / "monthly_unified_dataset_release_worker.py"
        ).resolve(strict=True)
        if not script.is_file() or not script.is_relative_to(self._project_root):
            raise MonthlyWorkerSupervisorError("monthly worker script is unavailable")
        command = (
            sys.executable,
            str(script),
            "--serve",
            "--poll-seconds",
            str(self._config.poll_seconds),
        )
        kwargs: dict[str, Any] = {
            "cwd": str(self._project_root),
            "env": os.environ.copy(),
            "stdin": subprocess.DEVNULL,
            "close_fds": True,
        }
        if os.name == "nt":
            kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        process = self._popen_factory(command, **kwargs)
        returncode = process.poll()
        if returncode is not None:
            process.wait(timeout=0)
            raise MonthlyWorkerSupervisorError(
                f"monthly worker exited during startup with code {returncode}"
            )
        self._process = process
        self._preflight = dict(preflight)
        LOGGER.info(
            "Monthly release worker started pid=%s poll_seconds=%s",
            process.pid,
            self._config.poll_seconds,
        )
        return {
            "status": "RUNNING",
            "enabled": True,
            "pid": process.pid,
            "preflight": dict(preflight),
        }

    def stop(self) -> Mapping[str, Any]:
        process = self._process
        if process is None:
            return {"status": "NOT_STARTED", "pid": None, "forced": False}
        self._process = None
        returncode = process.poll()
        if returncode is not None:
            process.wait(timeout=0)
            return {
                "status": "ALREADY_STOPPED",
                "pid": process.pid,
                "returncode": returncode,
                "forced": False,
            }
        process.terminate()
        forced = False
        try:
            returncode = process.wait(timeout=self._config.shutdown_seconds)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            returncode = process.wait(timeout=self._config.shutdown_seconds)
        LOGGER.info(
            "Monthly release worker stopped pid=%s returncode=%s forced=%s",
            process.pid,
            returncode,
            forced,
        )
        return {
            "status": "STOPPED",
            "pid": process.pid,
            "returncode": returncode,
            "forced": forced,
        }


def build_monthly_worker_supervisor_from_env(
    *,
    project_root: Path,
) -> MonthlyWorkerProcessSupervisor:
    return MonthlyWorkerProcessSupervisor(
        project_root=project_root,
        config=MonthlyWorkerSupervisorConfig.from_env(),
    )


__all__: Sequence[str] = (
    "MONTHLY_WORKER_ENABLED_ENV",
    "MONTHLY_WORKER_POLL_SECONDS_ENV",
    "MONTHLY_WORKER_SHUTDOWN_SECONDS_ENV",
    "MonthlyWorkerProcessSupervisor",
    "MonthlyWorkerSupervisorConfig",
    "MonthlyWorkerSupervisorError",
    "build_monthly_worker_supervisor_from_env",
)
