"""Supervised WSL execution adapters for the unified monthly BUILD stage.

The monthly materializer owns data preparation, while this module owns the
only boundary allowed to launch Qlib writers and the public-reader smoke.  It
accepts an attempt-scoped supervisor supplied by the production composition
root; no database/provider handle or caller supplied command crosses this
boundary.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import date
import json
from pathlib import Path
import shlex
import stat
from typing import Any, Mapping, Protocol, Sequence

from .candidate_consumer_smoke import (
    CandidateConsumerSmokeError,
    validate_candidate_consumer_smoke_receipt,
)
from .canonical import digest_named_fields
from .daily_minute_materializer import QlibDumpToolchain, build_qlib_dump_command
from .monthly_mature_build_runner import MonthlyConsumerSmokeResult
from .monthly_worker import ProducerContext
from .profile import DatasetProfile
from .resource_supervisor import WslSupervisedOptions


MAX_CONSUMER_RESULT_BYTES = 16 * 1024 * 1024
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x0400)
_OPERATION_FIELDS = {
    "operation_id",
    "dataset",
    "mode",
    "component_action",
    "csv_relative_path",
    "qlib_relative_path",
    "writer_targets_digest",
    "batch_manifest_identity",
    "batch_manifest_sha256",
}


class MonthlySupervisedBuildError(RuntimeError):
    """A supervised monthly Qlib child violated its frozen contract."""


class MonthlyAttemptSupervisor(Protocol):
    """Attempt-scoped execution capability supplied by repository code."""

    attempt_id: str
    fence: int
    control_root: Path
    heartbeat_path: Path

    def run_supervised(
        self,
        command: Sequence[str],
        **kwargs: Any,
    ) -> object: ...


def _is_link(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return stat.S_ISLNK(metadata.st_mode) or bool(
        int(getattr(metadata, "st_file_attributes", 0)) & _REPARSE_POINT
    )


def _future_path(root: Path, relative_value: object, *, label: str) -> Path:
    relative = Path(str(relative_value or ""))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise MonthlySupervisedBuildError(f"{label} path is invalid")
    resolved_root = root.resolve(strict=True)
    current = resolved_root
    for part in relative.parts:
        current /= part
        if _is_link(current):
            raise MonthlySupervisedBuildError(f"{label} path traverses a link")
    requested = Path(current.absolute())
    if not requested.is_relative_to(resolved_root):
        raise MonthlySupervisedBuildError(f"{label} path escapes staging")
    return requested


def _windows_to_wsl(path: Path) -> str:
    resolved = Path(path).absolute()
    drive = resolved.drive.rstrip(":").lower()
    if not drive or len(drive) != 1:
        raise MonthlySupervisedBuildError("WSL path requires a Windows drive")
    tail = resolved.as_posix().split(":", 1)[1]
    return f"/mnt/{drive}{tail}"


def _portable_receipt(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        payload = dict(value)
    elif is_dataclass(value):
        payload = asdict(value)
    elif hasattr(value, "as_dict"):
        payload = dict(value.as_dict())
    else:
        raise MonthlySupervisedBuildError("supervised child receipt is not typed")
    segments = payload.get("log_segments") or []
    if not isinstance(segments, list) or any(not isinstance(item, Mapping) for item in segments):
        raise MonthlySupervisedBuildError("supervised child log receipt is invalid")
    payload["log_segments"] = [
        {
            field: item[field]
            for field in ("stream", "generation", "size_bytes", "sha256", "cas_ref")
            if field in item
        }
        for item in segments
    ]
    payload.pop("result_path", None)
    payload.pop("log_root", None)
    return payload


def _validate_child(
    value: Mapping[str, Any],
    *,
    execution_id: str,
    timeout_seconds: int,
) -> None:
    gate = value.get("resource_gate_receipt")
    if (
        value.get("schema_version") != "dataset_supervised_execution_receipt_v1"
        or value.get("execution_id") != execution_id
        or value.get("runtime") != "wsl"
        or float(value.get("timeout_seconds", -1)) != float(timeout_seconds)
        or int(value.get("returncode", -1)) != 0
        or int(value.get("active_processes", -1)) != 0
        or not isinstance(value.get("wsl_readback"), Mapping)
        or not isinstance(gate, Mapping)
        or gate.get("final_status") != "READY"
        or gate.get("checkpoint_requested") is not False
        or gate.get("system_admission_thresholds_blocking") is not False
        or gate.get("data_scope_changed") is not False
    ):
        raise MonthlySupervisedBuildError(
            f"supervised WSL receipt differs: {execution_id}"
        )


def _attempt_execution_root(
    supervisor: MonthlyAttemptSupervisor,
    *,
    execution_id: str,
) -> Path:
    control = supervisor.control_root.resolve(strict=True)
    heartbeat = supervisor.heartbeat_path.resolve(strict=True)
    if not heartbeat.is_relative_to(control):
        raise MonthlySupervisedBuildError("supervisor heartbeat escapes control root")
    return (
        control
        / "attempt_runs"
        / f"{supervisor.attempt_id}-{supervisor.fence}"
        / execution_id
    )


@dataclass(frozen=True, slots=True)
class SupervisedMonthlyQlibWriter:
    profile: DatasetProfile
    project_root: Path
    toolchain: QlibDumpToolchain
    supervisor: MonthlyAttemptSupervisor

    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        operation: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        if context.stage != "BUILD" or set(operation) != _OPERATION_FIELDS:
            raise MonthlySupervisedBuildError("Qlib operation contract differs")
        operation_id = str(operation.get("operation_id") or "")
        dataset = str(operation.get("dataset") or "")
        if operation_id not in {"daily", "minute"} or dataset != f"{operation_id}_bin":
            raise MonthlySupervisedBuildError("Qlib operation identity differs")
        csv_root = _future_path(staging_root, operation["csv_relative_path"], label="Qlib CSV")
        qlib_root = _future_path(staging_root, operation["qlib_relative_path"], label="Qlib output")
        expected_target = digest_named_fields(
            "dataset_release_qlib_dump_writer_targets_v1",
            {
                "dataset": dataset,
                "mode": operation["mode"],
                "target": Path(str(operation["qlib_relative_path"])).as_posix(),
            },
        )
        if operation.get("writer_targets_digest") != expected_target:
            raise MonthlySupervisedBuildError("Qlib writer target identity differs")
        workers = int(self.profile.pressure_ladder["dump_workers"][0])
        command = build_qlib_dump_command(
            dataset=dataset,
            csv_root=csv_root,
            working_root=qlib_root,
            dump_workers=workers,
            toolchain=self.toolchain,
            mode=str(operation["mode"]),
        )
        execution_id = f"build-dump-{operation_id}"
        execution_root = _attempt_execution_root(self.supervisor, execution_id=execution_id)
        timeout = int(self.profile.stage_timeouts_seconds["qlib_dump"])
        child = self.supervisor.run_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root.resolve(strict=True),
            environment_scope="build",
            credential_env_keys=(),
            runtime="wsl",
            timeout_seconds=float(timeout),
            cooperative_grace_seconds=30.0,
            pressure_rung=0,
            wsl=WslSupervisedOptions(
                distro=self.toolchain.distro,
                guardian_python=self.toolchain.guardian_python,
                guardian_script_wsl=self.toolchain.guardian_script_wsl,
                heartbeat_path_wsl=_windows_to_wsl(self.supervisor.heartbeat_path),
                runner_python_wsl=self.toolchain.runner_python_wsl,
                runner_script_wsl=self.toolchain.runner_script_wsl,
                task_cwd_wsl=_windows_to_wsl(self.project_root),
                execution_root_wsl=_windows_to_wsl(execution_root),
            ),
        )
        receipt = _portable_receipt(child)
        _validate_child(receipt, execution_id=execution_id, timeout_seconds=timeout)
        return receipt


@dataclass(frozen=True, slots=True)
class SupervisedMonthlyConsumerSmoke:
    profile: DatasetProfile
    project_root: Path
    toolchain: QlibDumpToolchain
    supervisor: MonthlyAttemptSupervisor

    def execute(
        self,
        *,
        context: ProducerContext,
        staging_root: Path,
        prepare_result: Mapping[str, Any],
        release_digest: str,
    ) -> MonthlyConsumerSmokeResult:
        instrument = str(prepare_result.get("consumer_smoke_instrument") or "")
        try:
            cutoff = date.fromisoformat(str(context.plan.get("target_cutoff") or ""))
        except ValueError as exc:
            raise MonthlySupervisedBuildError("consumer smoke cutoff is invalid") from exc
        if context.stage != "BUILD" or not instrument:
            raise MonthlySupervisedBuildError("consumer smoke input identity is incomplete")
        release_id = str(context.plan.get("release_id") or "")
        if not release_id:
            raise MonthlySupervisedBuildError("consumer smoke release id is missing")
        execution_id = "build-consumer-smoke"
        candidate_root = Path(self.profile.candidate_root).resolve(strict=True)
        try:
            staging_relative_path = staging_root.resolve(strict=True).relative_to(
                candidate_root
            ).as_posix()
        except (OSError, ValueError) as exc:
            raise MonthlySupervisedBuildError(
                "consumer smoke staging root escapes candidate root"
            ) from exc
        if not staging_relative_path.startswith(".staging/"):
            raise MonthlySupervisedBuildError(
                "consumer smoke staging identity is non-canonical"
            )
        execution_root = _attempt_execution_root(self.supervisor, execution_id=execution_id)
        result_path = execution_root / "semantic_result.json"
        timeout = int(self.profile.stage_timeouts_seconds["consumer"])
        script = _windows_to_wsl(
            self.project_root.resolve(strict=True)
            / "scripts"
            / "dataset_release_candidate_consumer_smoke.py"
        )
        values = (
            "python",
            script,
            "--daily-provider-uri",
            _windows_to_wsl(staging_root / "daily_bin" / "qlib"),
            "--minute-provider-uri",
            _windows_to_wsl(staging_root / "minute_bin" / "qlib"),
            "--index-h5-path",
            _windows_to_wsl(staging_root / "index_context" / "index_daily.h5"),
            "--cutoff",
            cutoff.isoformat(),
            "--stock-instrument",
            instrument,
            "--profile",
            self.profile.profile,
            "--run-id",
            context.operation_id,
            "--attempt-id",
            self.supervisor.attempt_id,
            "--attempt-fence",
            str(self.supervisor.fence),
            "--release-id",
            release_id,
            "--release-digest",
            release_digest,
            "--staging-relative-path",
            staging_relative_path,
            "--execution-id",
            execution_id,
            "--max-h5-rows",
            str(self.profile.resource_policy.validation_read_chunk_rows),
            "--stage-timeout-seconds",
            str(timeout),
            "--result-path",
            _windows_to_wsl(result_path),
            "--control-root",
            _windows_to_wsl(self.supervisor.control_root),
            "--candidate-root",
            _windows_to_wsl(candidate_root),
        )
        command = [
            "bash",
            "-lc",
            " && ".join(
                (
                    f"source {shlex.quote(self.toolchain.conda_sh)}",
                    f"conda activate {shlex.quote(self.toolchain.conda_env)}",
                    " ".join(shlex.quote(value) for value in values),
                )
            ),
        ]
        child = self.supervisor.run_supervised(
            command,
            execution_id=execution_id,
            cwd=self.project_root.resolve(strict=True),
            environment_scope="validation",
            credential_env_keys=(),
            runtime="wsl",
            timeout_seconds=float(timeout),
            cooperative_grace_seconds=30.0,
            pressure_rung=0,
            wsl=WslSupervisedOptions(
                distro=self.toolchain.distro,
                guardian_python=self.toolchain.guardian_python,
                guardian_script_wsl=self.toolchain.guardian_script_wsl,
                heartbeat_path_wsl=_windows_to_wsl(self.supervisor.heartbeat_path),
                runner_python_wsl=self.toolchain.runner_python_wsl,
                runner_script_wsl=self.toolchain.runner_script_wsl,
                task_cwd_wsl=_windows_to_wsl(self.project_root),
                execution_root_wsl=_windows_to_wsl(execution_root),
            ),
        )
        resource_receipt = _portable_receipt(child)
        _validate_child(
            resource_receipt,
            execution_id=execution_id,
            timeout_seconds=timeout,
        )
        semantic = _read_json(result_path, max_bytes=MAX_CONSUMER_RESULT_BYTES)
        try:
            validated = validate_candidate_consumer_smoke_receipt(
                semantic,
                profile=self.profile.profile,
                cutoff=cutoff,
                expected_index_codes=self.profile.index_codes,
                expected_identity={
                    "run_id": context.operation_id,
                    "attempt_id": self.supervisor.attempt_id,
                    "attempt_fence": self.supervisor.fence,
                    "release_id": release_id,
                    "release_digest": release_digest,
                    "staging_relative_path": staging_relative_path,
                },
                expected_stage_timeout_seconds=timeout,
            )
        except (CandidateConsumerSmokeError, TypeError, ValueError) as exc:
            raise MonthlySupervisedBuildError(
                f"consumer smoke receipt is invalid: {exc}"
            ) from exc
        return MonthlyConsumerSmokeResult(
            semantic_receipt=validated,
            resource_receipt=resource_receipt,
        )


def _read_json(path: Path, *, max_bytes: int) -> Mapping[str, Any]:
    try:
        resolved = path.resolve(strict=True)
        if _is_link(path) or not resolved.is_file() or resolved.stat().st_size > max_bytes:
            raise MonthlySupervisedBuildError("consumer smoke result is unavailable or oversized")
        raw = resolved.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MonthlySupervisedBuildError("consumer smoke result is unreadable") from exc
    if not isinstance(value, Mapping):
        raise MonthlySupervisedBuildError("consumer smoke result must be an object")
    return value


__all__ = (
    "MonthlyAttemptSupervisor",
    "MonthlySupervisedBuildError",
    "SupervisedMonthlyConsumerSmoke",
    "SupervisedMonthlyQlibWriter",
)
