from __future__ import annotations

from pathlib import Path
import base64
import hashlib
import io
import json
import os
import subprocess
import tarfile

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


def test_probe_prepares_code_lazily_not_during_runtime_preflight():
    from backend.services.dataset_release.monthly_node_probe_runner import CodeOwnedSubprocessNodeProbeRunner

    calls = []
    runner = CodeOwnedSubprocessNodeProbeRunner(
        node_id="rdagent-node1",
        command=("old-command",),
        runner_id="test.lazy",
        command_factory=lambda: calls.append("prepare") or ("new-command",),
        executor=lambda command, **kwargs: (
            calls.append(tuple(command)) or subprocess.CompletedProcess(command, 0, b"{}\n", b"")
        ),
    )
    assert calls == []
    assert runner.run({}) == {}
    assert calls == ["prepare", ("new-command",)]


def test_streaming_deploy_prepares_tools_before_any_node_request(monkeypatch):
    from backend.services.dataset_release.monthly_immutable_deploy import MonthlyImmutableDeployError

    calls = []
    monkeypatch.setattr(ImmutableStreamingNodeTransport, "_request", lambda *args, **kwargs: {})
    transport = ImmutableStreamingNodeTransport(
        node_id="rdagent-node1",
        allowed_parent="/data/releases",
        command_prefix=("old",),
        command_factory=lambda: calls.append("prepare") or ("new",),
        command_runner=lambda command, **kwargs: (
            calls.append(tuple(command)) or subprocess.CompletedProcess(command, 1, b"", b"intentional test stop")
        ),
    )
    assert calls == []
    with pytest.raises(MonthlyImmutableDeployError, match="intentional test stop"):
        transport.deploy(None, candidate_root="/data/releases/test", files=())
    assert calls == ["prepare", ("new", "readback")]


def _tools_payload(tmp_path, *, path="backend/services/example.py", raw=b"# committed source\n"):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:") as archive:
        info = tarfile.TarInfo(path)
        info.size = len(raw)
        archive.addfile(info, io.BytesIO(raw))
    archive = buffer.getvalue()
    return {
        "tools_parent": str(tmp_path / ".aistock-monthly-tools"),
        "archive": base64.b64encode(archive).decode(),
        "manifest": {
            "schema_version": "aistock_monthly_node_tools_v1",
            "source_commit": "a" * 40,
            "archive_sha256": hashlib.sha256(archive).hexdigest(),
            "files": [{"path": path, "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}],
        },
    }


def test_tools_are_create_exclusive_and_resumable_without_legacy_overlay(tmp_path):
    from backend.services.dataset_release.monthly_node_tools import _install_node_tools

    legacy = tmp_path / "AIstock"
    legacy.mkdir()
    original = legacy / "keep.py"
    original.write_bytes(b"old project")
    payload = _tools_payload(tmp_path)
    result = _install_node_tools(payload)
    assert _install_node_tools(payload) == result
    assert Path(result["project_root"]).parent != legacy
    assert original.read_bytes() == b"old project"
    source = Path(result["project_root"]) / "backend/services/example.py"
    source.chmod(0o644)
    source.write_bytes(b"drift")
    with pytest.raises(ValueError, match="code bytes differ"):
        _install_node_tools(payload)
    assert source.read_bytes() == b"drift"


@pytest.mark.parametrize("failure", ["archive_hash", "source_hash", "escape", "duplicate", "symlink"])
def test_tools_reject_untrusted_or_ambiguous_inventory(tmp_path, failure):
    from backend.services.dataset_release.monthly_node_tools import _install_node_tools

    payload = _tools_payload(tmp_path, path="../escape.py" if failure == "escape" else "backend/example.py")
    if failure == "archive_hash":
        payload["manifest"]["archive_sha256"] = "0" * 64
    elif failure == "source_hash":
        payload["manifest"]["files"][0]["sha256"] = "0" * 64
    elif failure == "duplicate":
        payload["manifest"]["files"] *= 2
    elif failure == "symlink":
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w:") as archive:
            info = tarfile.TarInfo("backend/example.py")
            info.type, info.linkname = tarfile.SYMTYPE, "/etc/passwd"
            archive.addfile(info)
        raw = buffer.getvalue()
        payload["archive"] = base64.b64encode(raw).decode()
        payload["manifest"]["archive_sha256"] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError):
        _install_node_tools(payload)
    assert not (tmp_path.parent / "escape.py").exists()


def test_tools_bundle_uses_git_bytes_not_dirty_source_or_dotenv(tmp_path):
    from backend.services.dataset_release.monthly_node_tools import _bundle, _REQUIRED

    subprocess.run(("git", "init", str(tmp_path)), check=True, capture_output=True)
    for name in (*_REQUIRED, "backend/.env", "backend/tests/private.py", "backend/artifact.bin"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"committed")
    subprocess.run(("git", "-C", str(tmp_path), "add", "-f", "."), check=True, capture_output=True)
    subprocess.run(
        (
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "test source",
        ),
        check=True,
        capture_output=True,
    )
    commit = (
        subprocess.run(("git", "-C", str(tmp_path), "rev-parse", "HEAD"), check=True, capture_output=True)
        .stdout.decode()
        .strip()
    )
    (tmp_path / _REQUIRED[0]).write_bytes(b"dirty source ignored")
    manifest, encoded = _bundle(tmp_path, commit)
    assert set(row["path"] for row in manifest["files"]) == set(_REQUIRED)
    with tarfile.open(fileobj=io.BytesIO(base64.b64decode(encoded)), mode="r:") as archive:
        assert archive.extractfile(_REQUIRED[0]).read().strip() == b"committed"
    # An unrelated merged change cannot invalidate a worker's pinned commit.
    subprocess.run(("git", "-C", str(tmp_path), "add", "."), check=True, capture_output=True)
    subprocess.run(
        (
            "git",
            "-C",
            str(tmp_path),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "main advances",
        ),
        check=True,
        capture_output=True,
    )
    assert _bundle(tmp_path, commit) == (manifest, encoded)
    with pytest.raises(ValueError, match="unavailable"):
        _bundle(tmp_path, "f" * 40)


def test_bootstrap_is_taken_from_the_pinned_archive_not_live_source(tmp_path):
    from backend.services.dataset_release.monthly_node_tools import _bootstrap

    raw = b"def _install_node_tools(payload):\n    return 'frozen version'\n"
    payload = _tools_payload(tmp_path, path="backend/services/dataset_release/monthly_node_tools.py", raw=raw)
    code = _bootstrap(payload["manifest"], payload["archive"])
    assert code == raw.decode().strip()
    payload["manifest"]["files"][0]["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="bootstrap identity"):
        _bootstrap(payload["manifest"], payload["archive"])


def test_deploy_and_consumer_share_one_startup_commit_publisher():
    nodes = _nodes()
    assert nodes._node1_tools() is nodes._node1_tools()
    # Equivalent composition inputs cannot independently capture different HEADs.
    assert nodes._node1_tools() is _nodes()._node1_tools()


def test_deploy_and_probe_use_same_published_identity_without_network_in_preflight(tmp_path, monkeypatch):
    from backend.services.dataset_release import monthly_node_tools as tooling

    calls = []
    payload = _tools_payload(tmp_path)
    monkeypatch.setattr(tooling, "_bundle", lambda *args: (payload["manifest"], payload["archive"]))
    monkeypatch.setattr(tooling, "_bootstrap", lambda *args: "def _install_node_tools(payload): return payload")

    def publish(command, **kwargs):
        value = json.loads(kwargs["input"])
        calls.append(command)
        commit = value["manifest"]["source_commit"]
        receipt = {
            "status": "PASS",
            "source_commit": commit,
            "project_root": value["tools_parent"] + "/" + commit,
            "manifest_sha256": hashlib.sha256(tooling.canonical_json_bytes(value["manifest"]) + b"\n").hexdigest(),
            "file_count": len(value["manifest"]["files"]),
        }
        return subprocess.CompletedProcess(command, 0, json.dumps(receipt).encode(), b"")

    publisher = tooling.MonthlyNodeTools(
        "node1", "/home/user/AIstock", "/opt/python", Path(__file__).resolve().parents[3], executor=publish
    )
    publisher.source_commit = "a" * 40
    assert calls == []
    deploy = publisher.command("monthly_remote_deploy")
    probe = publisher.command("monthly_node_probe")
    assert len(calls) == 1
    assert "/home/user/.aistock-monthly-tools/" + "a" * 40 in deploy[-1]
    assert probe[-1].split(" && ")[0] == deploy[-1].split(" && ")[0]


@pytest.mark.skipif(os.name == "nt", reason="POSIX symlink bootstrap boundary")
def test_tools_do_not_follow_linked_parent(tmp_path):
    from backend.services.dataset_release.monthly_node_tools import _install_node_tools

    outside = tmp_path / "outside"
    outside.mkdir()
    payload = _tools_payload(tmp_path)
    Path(payload["tools_parent"]).symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="plain"):
        _install_node_tools(payload)
    assert list(outside.iterdir()) == []


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
