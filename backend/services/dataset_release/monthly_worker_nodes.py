"""Code-owned WSL/node1 capability composition for the monthly worker."""

from __future__ import annotations

from dataclasses import dataclass, replace
import os
from pathlib import Path, PurePosixPath
import re
import shlex
from types import MappingProxyType
from typing import Mapping, Sequence
from urllib.parse import urlparse

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
from .monthly_node_tools import MonthlyNodeTools


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
    wsl_qe_api_base_url: str = "http://127.0.0.1:5080/api/v1/qe_workspace"
    node1_qe_api_base_url: str = "http://rdagent-node1:5080/api/v1/qe_workspace"

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
        for label, value in (
            ("wsl_qe_api_base_url", self.wsl_qe_api_base_url),
            ("node1_qe_api_base_url", self.node1_qe_api_base_url),
        ):
            parsed = urlparse(value)
            if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment:
                raise MonthlyRuntimeConfigurationError(f"{label} must be an absolute HTTP(S) URL")

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
        wsl_url = str(os.getenv("AISTOCK_MONTHLY_WSL_QE_API_BASE_URL") or "").strip()
        node1_url = str(os.getenv("AISTOCK_MONTHLY_NODE1_QE_API_BASE_URL") or "").strip()
        if not wsl_url:
            wsl_url = "http://127.0.0.1:5080/api/v1/qe_workspace"
        if not node1_url:
            host = values["node1_host"].split("@", 1)[-1]
            node1_url = f"http://{host}:5080/api/v1/qe_workspace"
        return cls(
            **values,
            wsl_qe_api_base_url=wsl_url.rstrip("/"),
            node1_qe_api_base_url=node1_url.rstrip("/"),
        )

    def dataset_identity_urls(self) -> Mapping[str, str]:
        return MappingProxyType(
            {
                "wsl2-5080": self.wsl_qe_api_base_url.rstrip("/") + "/dataset-identity",
                "rdagent-node1": self.node1_qe_api_base_url.rstrip("/") + "/dataset-identity",
            }
        )

    def probe_runners(self) -> Mapping[str, MonthlyNodeProbeRunner]:
        tools = self._node1_tools()
        node1 = build_ssh_node_probe_runner(
            host=self.node1_host,
            project_root=self.node1_project_root,
            python_executable=self.node1_python,
        )
        node1 = replace(node1, command_factory=lambda: tools.command("monthly_node_probe"))
        return MappingProxyType(
            {
                "wsl2-5080": build_wsl_node_probe_runner(
                    distro=self.wsl_distro,
                    project_root=self.wsl_project_root,
                    python_executable=self.wsl_python,
                ),
                "rdagent-node1": node1,
            }
        )

    def deploy_transports(
        self,
        runtime: MonthlyRuntimeSettings,
    ) -> Mapping[str, MonthlyNodeReleaseTransport]:
        tools = self._node1_tools()
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
                    command_factory=lambda: tools.command("monthly_remote_deploy"),
                ),
            }
        )

    def _node1_tools(self) -> MonthlyNodeTools:
        return MonthlyNodeTools(
            host=self.node1_host,
            legacy_project_root=self.node1_project_root,
            python_executable=self.node1_python,
            source_root=Path(__file__).resolve().parents[3],
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
