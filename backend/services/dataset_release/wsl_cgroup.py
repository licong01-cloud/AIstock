from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from .profile import ResourcePolicy, validate_resource_policy


class WslCgroupError(RuntimeError):
    """Fail-closed WSL systemd/cgroup enforcement error."""


_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")


@dataclass(frozen=True)
class WslUnitIdentity:
    unit: str
    distro: str
    attempt_id: str
    fence: int

    @classmethod
    def create(cls, distro: str, attempt_id: str, fence: int) -> "WslUnitIdentity":
        if not _SAFE_NAME.fullmatch(distro):
            raise WslCgroupError("invalid WSL distro name")
        if not _SAFE_NAME.fullmatch(attempt_id):
            raise WslCgroupError("invalid attempt identity")
        if int(fence) <= 0:
            raise WslCgroupError("fence must be positive")
        unit = f"aistock-dataset-{attempt_id}-{int(fence)}.service"
        if len(unit) > 128:
            raise WslCgroupError("transient unit name is too long")
        return cls(unit=unit, distro=distro, attempt_id=attempt_id, fence=int(fence))


@dataclass(frozen=True)
class WslCgroupReadback:
    main_pid: int
    control_group: str
    memory_high_bytes: int
    memory_max_bytes: int
    memory_swap_max_bytes: int
    active_state: str


Runner = Callable[..., subprocess.CompletedProcess[str]]


class WslCgroupService:
    def __init__(
        self,
        identity: WslUnitIdentity,
        policy: ResourcePolicy,
        *,
        runner: Runner = subprocess.run,
    ) -> None:
        validate_resource_policy(policy)
        self.identity = identity
        self.policy = policy
        self._runner = runner

    def launch_command(self, guardian_command: Sequence[str]) -> list[str]:
        if not guardian_command or any("\x00" in str(part) for part in guardian_command):
            raise WslCgroupError("guardian command is invalid")
        properties = [
            f"MemoryHigh={self.policy.wsl_memory_high_bytes}",
            f"MemoryMax={self.policy.wsl_memory_max_bytes}",
            f"MemorySwapMax={self.policy.wsl_swap_max_bytes}",
            "KillMode=control-group",
            "SendSIGKILL=yes",
            "CollectMode=inactive-or-failed",
            # systemd maps OOMPolicy=kill to cgroup v2 memory.oom.group=1.
            "OOMPolicy=kill",
        ]
        command = [
            "wsl.exe",
            "-d",
            self.identity.distro,
            "--",
            "systemd-run",
            "--user",
            "--wait",
            "--pipe",
            "--collect",
            "--service-type=exec",
            f"--unit={self.identity.unit}",
        ]
        command.extend(f"--property={item}" for item in properties)
        command.extend(str(part) for part in guardian_command)
        return command

    def show_command(self) -> list[str]:
        fields = "MainPID,ControlGroup,MemoryHigh,MemoryMax,MemorySwapMax,ActiveState"
        return [
            "wsl.exe",
            "-d",
            self.identity.distro,
            "--",
            "systemctl",
            "--user",
            "show",
            self.identity.unit,
            f"--property={fields}",
            "--no-pager",
        ]

    @staticmethod
    def _parse_show(output: str) -> Mapping[str, str]:
        values: dict[str, str] = {}
        for raw_line in output.splitlines():
            if "=" not in raw_line:
                continue
            key, value = raw_line.split("=", 1)
            values[key.strip()] = value.strip()
        return values

    def readback(self, *, timeout_seconds: float = 10.0) -> WslCgroupReadback:
        completed = self._runner(
            self.show_command(),
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
        if completed.returncode != 0:
            raise WslCgroupError(
                f"systemd unit readback failed: rc={completed.returncode} stderr={completed.stderr[-500:]}"
            )
        values = self._parse_show(completed.stdout)
        required = {
            "MainPID",
            "ControlGroup",
            "MemoryHigh",
            "MemoryMax",
            "MemorySwapMax",
            "ActiveState",
        }
        missing = sorted(required.difference(values))
        if missing:
            raise WslCgroupError(f"systemd unit readback missing fields: {missing}")
        try:
            readback = WslCgroupReadback(
                main_pid=int(values["MainPID"]),
                control_group=values["ControlGroup"],
                memory_high_bytes=int(values["MemoryHigh"]),
                memory_max_bytes=int(values["MemoryMax"]),
                memory_swap_max_bytes=int(values["MemorySwapMax"]),
                active_state=values["ActiveState"],
            )
        except ValueError as exc:
            raise WslCgroupError("systemd unit readback contains non-integer limits") from exc
        if readback.main_pid <= 0 or not readback.control_group.startswith("/"):
            raise WslCgroupError("systemd unit has no identity-bound MainPID/control group")
        expected = (
            self.policy.wsl_memory_high_bytes,
            self.policy.wsl_memory_max_bytes,
            self.policy.wsl_swap_max_bytes,
        )
        actual = (
            readback.memory_high_bytes,
            readback.memory_max_bytes,
            readback.memory_swap_max_bytes,
        )
        if actual != expected:
            raise WslCgroupError(f"cgroup memory limit readback mismatch: expected={expected} actual={actual}")
        if readback.active_state not in {"activating", "active"}:
            raise WslCgroupError(f"systemd unit is not active: {readback.active_state}")
        return readback

    def read_memory_files_command(
        self,
        control_group: str,
        *,
        python_wsl: str,
        guardian_script_wsl: str,
        cwd_wsl: str,
    ) -> list[str]:
        if not control_group.startswith("/") or ".." in Path(control_group).parts:
            raise WslCgroupError("invalid cgroup readback path")
        helper_paths = (python_wsl, guardian_script_wsl, cwd_wsl)
        if any(
            not str(value).startswith("/") or "\x00" in str(value) or ".." in Path(str(value)).parts
            for value in helper_paths
        ):
            raise WslCgroupError("cgroup readback requires explicit absolute helper paths/cwd")
        # No shell is used. The helper receives the verified cgroup path as one argv item.
        return [
            "wsl.exe",
            "-d",
            self.identity.distro,
            "--cd",
            cwd_wsl,
            "--",
            python_wsl,
            guardian_script_wsl,
            "--read-cgroup",
            control_group,
        ]


__all__ = [
    "WslCgroupError",
    "WslCgroupReadback",
    "WslCgroupService",
    "WslUnitIdentity",
]
