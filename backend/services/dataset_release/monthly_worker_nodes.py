"""Code-owned WSL/node1 capability composition for the monthly worker."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path, PurePosixPath
import re
import shlex
from types import MappingProxyType
from typing import Mapping, Sequence

from .monthly_consumer_validation import RegisteredMonthlyConsumerValidationExecutor
from .monthly_immutable_deploy import (
    ExistingTreeNodeTransport,
    ImmutableStreamingNodeTransport,
    MonthlyNodeReleaseTransport,
)
from .monthly_node_probe_runner import (
    MonthlyNodeProbeRunner,
    build_node_attested_consumer_validation_executor,
    build_ssh_node_probe_runner,
    build_wsl_node_probe_runner,
)
from .monthly_runtime import MonthlyRuntimeConfigurationError, MonthlyRuntimeSettings


_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
_HOST = re.compile(r"^[A-Za-z0-9_.@-]{1,255}$")


def _posix(value: str, *, label: str) -> str:
    path = PurePosixPath(value)
    if (
        "\x00" in value
        or not path.is_absolute()
        or any(part in {"", ".", ".."} for part in path.parts[1:])
        or path.as_posix() != value
    ):
        raise MonthlyRuntimeConfigurationError(f"{label} must be a canonical absolute POSIX path")
    return path.as_posix()


@dataclass(frozen=True, slots=True)
class MonthlyNodeRuntimeSettings:
    wsl_distro: str
    wsl_project_root: str
    wsl_python: str
    node1_host: str
    node1_project_root: str
    node1_python: str

    def __post_init__(self) -> None:
        if _NAME.fullmatch(self.wsl_distro) is None or _HOST.fullmatch(self.node1_host) is None:
            raise MonthlyRuntimeConfigurationError("monthly node runtime name is invalid")
        for label, value in (
            ("wsl_project_root", self.wsl_project_root),
            ("wsl_python", self.wsl_python),
            ("node1_project_root", self.node1_project_root),
            ("node1_python", self.node1_python),
        ):
            _posix(value, label=label)

    @classmethod
    def from_env(cls) -> "MonthlyNodeRuntimeSettings":
        names = {
            "wsl_distro": "AISTOCK_MONTHLY_WSL_DISTRO",
            "wsl_project_root": "AISTOCK_MONTHLY_WSL_PROJECT_ROOT",
            "wsl_python": "AISTOCK_MONTHLY_WSL_PYTHON",
            "node1_host": "AISTOCK_MONTHLY_NODE1_SSH_HOST",
            "node1_project_root": "AISTOCK_MONTHLY_NODE1_PROJECT_ROOT",
            "node1_python": "AISTOCK_MONTHLY_NODE1_PYTHON",
        }
        values = {field: str(os.getenv(name) or "").strip() for field, name in names.items()}
        missing = sorted(names[field] for field, value in values.items() if not value)
        if missing:
            raise MonthlyRuntimeConfigurationError(
                "monthly node runtime environment is incomplete",
                context={"missing": missing},
            )
        return cls(**values)

    def probe_runners(self) -> Mapping[str, MonthlyNodeProbeRunner]:
        return MappingProxyType(
            {
                "wsl2-5080": build_wsl_node_probe_runner(
                    distro=self.wsl_distro,
                    project_root=self.wsl_project_root,
                    python_executable=self.wsl_python,
                ),
                "rdagent-node1": build_ssh_node_probe_runner(
                    host=self.node1_host,
                    project_root=self.node1_project_root,
                    python_executable=self.node1_python,
                ),
            }
        )

    def deploy_transports(
        self,
        runtime: MonthlyRuntimeSettings,
    ) -> Mapping[str, MonthlyNodeReleaseTransport]:
        remote = (
            "cd -- "
            + shlex.quote(self.node1_project_root)
            + " && exec "
            + shlex.quote(self.node1_python)
            + " -m backend.services.dataset_release.monthly_remote_deploy"
        )
        return MappingProxyType(
            {
                "controller": ExistingTreeNodeTransport(),
                "wsl2-5080": ImmutableStreamingNodeTransport(
                    node_id="wsl2-5080",
                    allowed_parent=runtime.wsl_release_root,
                    command_prefix=(
                        "wsl.exe",
                        "-d",
                        self.wsl_distro,
                        "--cd",
                        self.wsl_project_root,
                        "--",
                        self.wsl_python,
                        "-m",
                        "backend.services.dataset_release.monthly_remote_deploy",
                    ),
                ),
                "rdagent-node1": ImmutableStreamingNodeTransport(
                    node_id="rdagent-node1",
                    allowed_parent=runtime.node1_release_root,
                    command_prefix=(
                        "ssh",
                        "-F",
                        "NUL",
                        "-o",
                        "BatchMode=yes",
                        "-o",
                        "ConnectTimeout=30",
                        self.node1_host,
                        remote,
                    ),
                ),
            }
        )

    def consumer_executor(
        self,
        *,
        artifact_root: Path,
    ) -> RegisteredMonthlyConsumerValidationExecutor:
        return build_node_attested_consumer_validation_executor(
            artifact_root=artifact_root,
            runners=self.probe_runners(),
        )


__all__: Sequence[str] = ("MonthlyNodeRuntimeSettings",)
