from __future__ import annotations

from pathlib import Path

import pytest

from backend.services.dataset_release.monthly_immutable_deploy import (
    ExistingTreeNodeTransport,
    ImmutableStreamingNodeTransport,
)
from backend.services.dataset_release.monthly_runtime import (
    MonthlyRuntimeConfigurationError,
    MonthlyRuntimeSettings,
)
from backend.services.dataset_release.monthly_unified import REQUIRED_CONSUMERS
from backend.services.dataset_release.monthly_worker_nodes import (
    MonthlyNodeRuntimeSettings,
)


def _base(tmp_path: Path) -> MonthlyRuntimeSettings:
    roots = [tmp_path / name for name in ("state", "controller", "profiles", "artifacts", "auth")]
    for root in roots:
        root.mkdir()
    active = tmp_path / "active.json"
    active.write_text("{}\n", encoding="utf-8")
    return MonthlyRuntimeSettings(
        state_root=roots[0],
        active_profile=active,
        controller_release_root=roots[1],
        profile_candidate_root=roots[2],
        artifact_root=roots[3],
        wsl_release_root="/mnt/wsl/releases",
        node1_release_root="/home/lc999/data/releases",
        authorization_root=roots[4],
    )


def _nodes() -> MonthlyNodeRuntimeSettings:
    return MonthlyNodeRuntimeSettings(
        wsl_distro="Ubuntu-24.04",
        wsl_project_root="/mnt/f/Dev/AIstock",
        wsl_python="/opt/conda/envs/rdagent-gpu/bin/python",
        node1_host="rdagent-node1",
        node1_project_root="/home/lc999/AIstock",
        node1_python="/home/lc999/miniconda3/envs/rdagent-gpu/bin/python",
    )


def test_node_runtime_builds_exact_code_owned_deploy_and_probe_capabilities(tmp_path: Path) -> None:
    runtime = _base(tmp_path)
    nodes = _nodes()

    transports = nodes.deploy_transports(runtime)
    probes = nodes.probe_runners()
    executor = nodes.consumer_executor(artifact_root=runtime.artifact_root)

    assert isinstance(transports["controller"], ExistingTreeNodeTransport)
    assert isinstance(transports["wsl2-5080"], ImmutableStreamingNodeTransport)
    assert isinstance(transports["rdagent-node1"], ImmutableStreamingNodeTransport)
    assert transports["wsl2-5080"].allowed_parent == runtime.wsl_release_root
    assert transports["rdagent-node1"].allowed_parent == runtime.node1_release_root
    assert transports["wsl2-5080"].command_prefix[-2:] == (
        "-m",
        "backend.services.dataset_release.monthly_remote_deploy",
    )
    assert transports["rdagent-node1"].command_prefix[:8] == (
        "ssh",
        "-F",
        "NUL",
        "-o",
        "BatchMode=yes",
        "-o",
        "ConnectTimeout=30",
        "rdagent-node1",
    )
    assert set(probes) == {"wsl2-5080", "rdagent-node1"}
    assert tuple(executor.probes) == REQUIRED_CONSUMERS


def test_node_runtime_from_env_is_separate_from_api_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = {
        "AISTOCK_MONTHLY_WSL_DISTRO": "Ubuntu-24.04",
        "AISTOCK_MONTHLY_WSL_PROJECT_ROOT": "/mnt/f/Dev/AIstock",
        "AISTOCK_MONTHLY_WSL_PYTHON": "/opt/conda/envs/rdagent-gpu/bin/python",
        "AISTOCK_MONTHLY_NODE1_SSH_HOST": "rdagent-node1",
        "AISTOCK_MONTHLY_NODE1_PROJECT_ROOT": "/home/lc999/AIstock",
        "AISTOCK_MONTHLY_NODE1_PYTHON": "/home/lc999/miniconda3/envs/rdagent-gpu/bin/python",
    }
    for name, value in names.items():
        monkeypatch.setenv(name, value)

    assert MonthlyNodeRuntimeSettings.from_env() == _nodes()
    monkeypatch.delenv("AISTOCK_MONTHLY_NODE1_PYTHON")
    with pytest.raises(MonthlyRuntimeConfigurationError, match="environment is incomplete"):
        MonthlyNodeRuntimeSettings.from_env()


@pytest.mark.parametrize(
    "field,value",
    [
        ("wsl_distro", "bad value"),
        ("node1_host", "host;rm"),
        ("wsl_project_root", "relative"),
        ("wsl_project_root", "/mnt/f/Dev/AIstock/"),
        ("wsl_python", "/opt//conda/python"),
        ("node1_python", "/home/../python"),
    ],
)
def test_node_runtime_rejects_command_injection_or_noncanonical_paths(
    field: str,
    value: str,
) -> None:
    raw = {
        "wsl_distro": "Ubuntu-24.04",
        "wsl_project_root": "/mnt/f/Dev/AIstock",
        "wsl_python": "/opt/conda/envs/rdagent-gpu/bin/python",
        "node1_host": "rdagent-node1",
        "node1_project_root": "/home/lc999/AIstock",
        "node1_python": "/home/lc999/miniconda3/envs/rdagent-gpu/bin/python",
    }
    raw[field] = value
    with pytest.raises(MonthlyRuntimeConfigurationError):
        MonthlyNodeRuntimeSettings(**raw)
