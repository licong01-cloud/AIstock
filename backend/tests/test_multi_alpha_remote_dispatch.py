from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pandas as pd
import pytest

from backend.services.multi_alpha.combine_backtest import MultiAlphaCombineBacktestError, ShellPredBacktestExecutor
from backend.services.multi_alpha.remote_dispatch import (
    ComputeNodeInfo,
    RemotePredBacktestExecutor,
    WorkspaceArtifactSyncClient,
    _build_remote_runtime_file_manifest,
    _remote_runtime_artifact_link_commands,
    _remote_runtime_file_verify_commands,
    _remote_small_files,
    _require_remote_linux_path,
    _sync_remote_runtime_artifacts,
    _remote_task_id,
    _remote_wsl_command,
    _resolve_l2_artifact_path,
    is_remote_compute_node,
)
from backend.tests.test_multi_alpha_combine_backtest import _runtime_template, _service


class _Response:
    def __init__(self, *, status_code: int = 200, headers: dict[str, str] | None = None, payload: Any | None = None, text: str = "") -> None:
        self.status_code = status_code
        self.headers = headers or {}
        self._payload = payload if payload is not None else {}
        self.text = text

    def json(self) -> Any:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)


def test_workspace_artifact_sync_head_hit_skips_upload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = tmp_path / "combined_factors_df.parquet"
    artifact.write_bytes(b"abc")
    calls: list[tuple[str, str]] = []

    def fake_head(url: str, **_kwargs: Any) -> _Response:
        calls.append(("HEAD", url))
        return _Response(
            status_code=200,
            headers={
                "X-Artifact-Exists": "1",
                "X-Artifact-Size": str(artifact.stat().st_size),
                "X-Artifact-Store-Root": "/remote/artifacts",
            },
        )

    def fake_post(url: str, **_kwargs: Any) -> _Response:
        calls.append(("POST", url))
        raise AssertionError("upload must be skipped when content-addressed artifact exists")

    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.head", fake_head)
    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.post", fake_post)

    result = WorkspaceArtifactSyncClient(base_url="http://node/api/v1/qe_workspace/artifacts").ensure_artifact(artifact, node_id="node-1")

    assert result["uploaded"] is False
    assert calls == [("HEAD", f"http://node/api/v1/qe_workspace/artifacts/{result['sha256']}")]


def test_workspace_artifact_sync_uploads_and_verifies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = tmp_path / "combined_factors_df.parquet"
    artifact.write_bytes(b"abc")
    seen_upload = {"called": False}

    def fake_head(url: str, **_kwargs: Any) -> _Response:
        exists = "1" if seen_upload["called"] else "0"
        size = str(artifact.stat().st_size) if seen_upload["called"] else "0"
        return _Response(
            status_code=200,
            headers={"X-Artifact-Exists": exists, "X-Artifact-Size": size, "X-Artifact-Store-Root": "/remote/artifacts"},
        )

    def fake_post(url: str, data: Any, **_kwargs: Any) -> _Response:
        assert data.read() == b"abc"
        seen_upload["called"] = True
        return _Response(status_code=200, payload={"ok": True})

    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.head", fake_head)
    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.post", fake_post)

    result = WorkspaceArtifactSyncClient(base_url="http://node/api/v1/qe_workspace/artifacts").ensure_artifact(artifact, node_id="node-1")

    assert result["uploaded"] is True
    assert result["size"] == 3
    assert result["status"]["artifact_store_root"] == "/remote/artifacts"


def test_workspace_artifact_sync_rejects_remote_size_mismatch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = tmp_path / "combined_factors_df.parquet"
    artifact.write_bytes(b"abc")

    def fake_head(url: str, **_kwargs: Any) -> _Response:
        return _Response(status_code=200, headers={"X-Artifact-Exists": "1", "X-Artifact-Size": "999"})

    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.head", fake_head)

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        WorkspaceArtifactSyncClient(base_url="http://node/api/v1/qe_workspace").ensure_artifact(artifact, node_id="node-1")

    assert excinfo.value.reason_code == "workspace_artifact_remote_size_mismatch"


def test_workspace_artifact_sync_rejects_invalid_upload_response(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    artifact = tmp_path / "combined_factors_df.parquet"
    artifact.write_bytes(b"abc")

    def fake_head(url: str, **_kwargs: Any) -> _Response:
        return _Response(status_code=200, headers={"X-Artifact-Exists": "0", "X-Artifact-Size": "0"})

    def fake_post(url: str, data: Any, **_kwargs: Any) -> _Response:
        assert data.read() == b"abc"
        return _Response(status_code=200, payload={"ok": False, "reason_code": "sha_mismatch"})

    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.head", fake_head)
    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.post", fake_post)

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        WorkspaceArtifactSyncClient(base_url="http://node/api/v1/qe_workspace").ensure_artifact(artifact, node_id="node-1")

    assert excinfo.value.reason_code == "workspace_artifact_upload_failed"


def test_service_uses_local_shell_executor_for_local_node(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)
    service._executor = None
    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.is_remote_compute_node", lambda node_id: False)

    assert isinstance(service._executor_for_node("wsl2-5080"), ShellPredBacktestExecutor)


def test_service_uses_remote_executor_for_remote_node(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    service, _repo, _executor, _checker = _service(tmp_path)
    service._executor = None
    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.is_remote_compute_node", lambda node_id: True)

    assert isinstance(service._executor_for_node("rdagent-node1"), RemotePredBacktestExecutor)


def test_default_l2_artifact_path_does_not_duplicate_relative_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    workspace = Path("rdagent_assets/multi_alpha_combine_backtests/macb_test/combined_equal")
    artifact = workspace / "combined_factors_df.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"parquet")

    resolved = _resolve_l2_artifact_path(workspace=workspace, backtest_config={})

    assert resolved == artifact
    assert resolved.exists()


def test_explicit_relative_l2_artifact_path_is_workspace_relative(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    artifact = workspace / "custom" / "factors.parquet"
    artifact.parent.mkdir()
    artifact.write_bytes(b"parquet")

    resolved = _resolve_l2_artifact_path(
        workspace=workspace,
        backtest_config={"combined_factors_path": "custom/factors.parquet"},
    )

    assert resolved == artifact


def test_is_remote_compute_node_uses_compute_node_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "backend.services.multi_alpha.remote_dispatch.get_compute_node_info",
        lambda node_id: ComputeNodeInfo(node_id=node_id, api_base_url="http://192.168.50.215:9000"),
    )

    assert is_remote_compute_node("rdagent-node1") is True


class _FakeArtifactClient:
    def __init__(self) -> None:
        self.calls: list[Path] = []

    def ensure_artifact(self, path: Path, *, node_id: str, verify_after_upload: bool = True) -> dict[str, Any]:
        self.calls.append(path)
        if path.name not in {"combined_factors_df.parquet", "combined_prediction.pkl"}:
            sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        else:
            sha256 = ("b" if path.suffix == ".pkl" else "a") * 64
        return {
            "sha256": sha256,
            "size": path.stat().st_size,
            "uploaded": False,
            "status": {"exists": True, "size": path.stat().st_size, "artifact_store_root": "/remote/artifacts"},
        }


class _FakeWorkspaceClient:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []
        self.status_calls = 0

    async def create_and_run_loop(self, task_id: str, loop_index: int, config: dict[str, Any], experiment_files: dict[str, str], wsl_command: str, **_kwargs: Any) -> str:
        self.payloads.append({"task_id": task_id, "loop_index": loop_index, "config": config, "experiment_files": experiment_files, "wsl_command": wsl_command})
        return f"{task_id}_Loop{loop_index}"

    async def get_loop_status(self, task_id: str, loop_id: str) -> dict[str, Any]:
        self.status_calls += 1
        return {"status": "completed"}

    async def get_workspace_file(self, task_id: str, loop_id: str, file_path: str) -> dict[str, Any] | str:
        if file_path == "qlib_results_enhanced.json":
            return {"absolute_returns": {"cagr": 1.0, "max_drawdown": -0.2, "sharpe": 2.0, "calmar": 5.0}}
        return "run log"

    async def close(self) -> None:
        return None


class _FakeSubmissionCoordinator:
    async def submit(self, *, client: Any, source: Any, payload: Any) -> Any:
        loop_id = await client.create_and_run_loop(
            payload.task_id,
            payload.loop_index,
            dict(payload.config),
            dict(payload.experiment_files),
            payload.wsl_command,
            submission_intent_hash=source.submission_intent_hash,
        )
        if loop_id.startswith(f"{payload.task_id}_"):
            loop_id = loop_id[len(payload.task_id) + 1 :]
        return SimpleNamespace(
            loop_id=loop_id,
            waiting_capacity=False,
            active_count=1,
            node_capacity=4,
            reservation_id="qer_test",
        )

    def record_authoritative_remote_status(self, **_kwargs: Any) -> dict[str, Any]:
        return {"status": "released"}


def test_remote_pred_backtest_executor_posts_loop_and_ingests_metrics(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    template = _runtime_template(tmp_path)
    (template / "qe_sector_risk_overlay.parquet").write_bytes(b"r" * (8 * 1024 * 1024))
    workspace.mkdir()
    pred = workspace / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred)
    l2 = workspace / "combined_factors_df.parquet"
    l2.write_bytes(b"parquet")
    fake_artifact = _FakeArtifactClient()
    fake_workspace = _FakeWorkspaceClient()
    small_sync_calls: list[dict[str, Any]] = []
    executor = RemotePredBacktestExecutor(
        node_resolver=lambda node_id: ComputeNodeInfo(
            node_id=node_id,
            api_base_url="http://192.168.50.215:9000",
            factor_data_dir="/home/node/aistock_cache/factor_values",
            qlib_data_path="/home/node/data/qlib_bin",
        ),
        artifact_client_factory=lambda _node_id: fake_artifact,
        workspace_client_factory=lambda _node_id: fake_workspace,
        submission_coordinator=_FakeSubmissionCoordinator(),
        small_file_syncer=lambda **kwargs: small_sync_calls.append(kwargs),
        poll_interval_seconds=0.0,
    )

    metrics = executor.execute_pred_backtest(
        workspace=workspace,
        pred_pkl=pred,
        node_id="rdagent-node1",
        backtest_config={"runtime_template_dir": str(template), "timeout_seconds": 30, "remote_artifact_store_root": "/remote/artifacts"},
    )

    assert metrics["cagr"] == 1.0
    runtime_overlay = workspace / "qe_sector_risk_overlay.parquet"
    assert fake_artifact.calls == [l2, pred, runtime_overlay]
    assert fake_workspace.payloads
    payload = fake_workspace.payloads[0]
    assert payload["experiment_files"] == {}
    assert small_sync_calls and small_sync_calls[0]["task_id"].startswith("macb_remote_")
    assert small_sync_calls[0]["task_id"].endswith("_workspace")
    assert small_sync_calls[0]["loop_index"] == 1
    assert "combined_prediction.pkl.b64" not in small_sync_calls[0]["files"]
    assert "qe_sector_risk_overlay.parquet.b64" not in small_sync_calls[0]["files"]
    assert "bash -lc" in payload["wsl_command"]
    assert "/remote/artifacts/" + "a" * 64 in payload["wsl_command"]
    assert "/remote/artifacts/" + "b" * 64 in payload["wsl_command"]
    assert payload["wsl_command"].count("ln -sfn") == 3
    assert payload["config"]["runtime_artifact_bindings"][0]["name"] == "qe_sector_risk_overlay.parquet"
    assert "combined_prediction.pkl" in payload["wsl_command"]
    assert "*.b64" in payload["wsl_command"]
    assert "../combined_prediction.pkl.b64" not in payload["wsl_command"]
    assert "--pred-backtest combined_prediction.pkl" in payload["wsl_command"]
    assert (workspace / "qlib_results_enhanced.json").exists()


def test_remote_small_files_include_runtime_deps_and_exclude_outputs(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py", "qe_suspend_filter.json", "tail_twap_v25_1_strategy.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    (workspace / "benchmark_sh000300.parquet").write_bytes(b"bench")
    (workspace / "combined_factors_df.parquet").write_bytes(b"large")
    (workspace / "qlib_results_enhanced.json").write_text("{}", encoding="utf-8")
    pred = workspace / "combined_prediction.pkl"
    pred.write_bytes(b"pred")

    files = _remote_small_files(workspace=workspace, pred_pkl=pred, include_prediction=True)

    assert "conf.yaml" in files
    assert "qe_suspend_filter.json" in files
    assert "tail_twap_v25_1_strategy.py" in files
    assert "benchmark_sh000300.parquet.b64" in files
    assert "combined_prediction.pkl.b64" in files
    assert "combined_factors_df.parquet" not in files
    assert "combined_factors_df.parquet.b64" not in files
    assert "qlib_results_enhanced.json" not in files


def test_remote_small_text_preserves_exact_utf8_crlf_bytes(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    expected = b"first line\r\nsecond line\r\n"
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_bytes(expected)

    files = _remote_small_files(
        workspace=workspace,
        pred_pkl=workspace / "combined_prediction.pkl",
        include_prediction=False,
    )

    assert set(files) == {"conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"}
    assert all(content.encode("utf-8") == expected for content in files.values())


def test_remote_small_file_scan_filters_factor_symlinks_before_windows_stat(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    factor_link_names = {"bak_basic.h5", "static_factors.parquet"}
    for name in factor_link_names:
        (workspace / name).write_bytes(b"")
    pred = workspace / "combined_prediction.pkl"
    pred.write_bytes(b"pred")
    original_is_file = Path.is_file

    def guarded_is_file(path: Path) -> bool:
        if path.name in factor_link_names:
            raise OSError(1920, "simulated DrvFS Linux symlink dereference failure")
        return original_is_file(path)

    monkeypatch.setattr(Path, "is_file", guarded_is_file)

    files = _remote_small_files(workspace=workspace, pred_pkl=pred, include_prediction=True)

    assert "bak_basic.h5" not in files
    assert "static_factors.parquet" not in files
    assert "static_factors.parquet.b64" not in files
    assert "combined_prediction.pkl.b64" in files


def test_remote_small_file_scan_reports_supported_unreadable_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    unreadable = workspace / "runtime.yaml"
    unreadable.write_text("content", encoding="utf-8")
    pred = workspace / "combined_prediction.pkl"
    pred.write_bytes(b"pred")
    original_is_file = Path.is_file

    def guarded_is_file(path: Path) -> bool:
        if path == unreadable:
            raise OSError(1920, "simulated unreadable runtime file")
        return original_is_file(path)

    monkeypatch.setattr(Path, "is_file", guarded_is_file)

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _remote_small_files(workspace=workspace, pred_pkl=pred, include_prediction=True)

    assert excinfo.value.reason_code == "remote_workspace_file_scan_failed"
    assert excinfo.value.context["path"] == str(unreadable)


def test_remote_small_files_leave_oversized_prediction_to_artifact_store(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    pred = workspace / "combined_prediction.pkl"
    pred.write_bytes(b"p" * (10 * 1024 * 1024 + 1))

    files = _remote_small_files(workspace=workspace, pred_pkl=pred, include_prediction=False)

    assert "combined_prediction.pkl.b64" not in files
    assert set(files) == {"conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"}


def test_oversized_runtime_parquet_uses_cas_and_is_not_small_file_payload(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    runtime_artifact = workspace / "qe_sector_risk_overlay.parquet"
    runtime_artifact.write_bytes(b"r" * (8 * 1024 * 1024))
    pred = workspace / "combined_prediction.pkl"
    pred.write_bytes(b"pred")
    client = _FakeArtifactClient()
    node = ComputeNodeInfo(node_id="rdagent-node1", api_base_url="http://192.168.50.215:9000")

    bindings = _sync_remote_runtime_artifacts(
        workspace=workspace,
        node=node,
        node_id=node.node_id,
        artifact_client=client,
        remote_paths={"artifact_path": "/remote/artifacts/" + "a" * 64},
    )
    files = _remote_small_files(
        workspace=workspace,
        pred_pkl=pred,
        include_prediction=False,
        cas_bound_names={str(item["name"]) for item in bindings},
    )

    expected_sha = hashlib.sha256(runtime_artifact.read_bytes()).hexdigest()
    assert client.calls == [runtime_artifact]
    assert bindings == [
        {
            "name": runtime_artifact.name,
            "binding": "workspace_artifact_cas",
            "sha256": expected_sha,
            "size": runtime_artifact.stat().st_size,
            "remote_path": f"/remote/artifacts/{expected_sha}",
            "artifact_store_root": "/remote/artifacts",
            "cas_status": "reused",
        }
    ]
    assert f"{runtime_artifact.name}.b64" not in files
    command = _remote_runtime_artifact_link_commands(bindings)
    assert "sha256sum --" in command
    assert f"/remote/artifacts/{expected_sha}" in command
    assert "ln -sfn" in command
    assert runtime_artifact.name in command


def test_runtime_file_manifest_covers_nested_assets_and_excludes_python_cache(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    package = workspace / "aistock_models"
    package.mkdir()
    (package / "model.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package / "__init__.py").write_bytes(b"")
    cache = package / "__pycache__"
    cache.mkdir()
    (cache / "model.cpython-310.pyc").write_bytes(b"cache")
    weights = workspace / "weights.npz"
    weights.write_bytes(b"w" * (8 * 1024 * 1024))

    manifest = _build_remote_runtime_file_manifest(workspace=workspace)
    entries = {item["path"]: item for item in manifest["files"]}

    assert len(manifest["manifest_hash"]) == 64
    assert entries["conf.yaml"]["transfer"] == "small_text"
    assert entries["aistock_models/model.py"]["transfer"] == "cas"
    assert entries["aistock_models/__init__.py"]["transfer"] == "empty_file"
    assert entries["weights.npz"]["transfer"] == "cas"
    assert all("__pycache__" not in path and not path.endswith(".pyc") for path in entries)

    client = _FakeArtifactClient()
    node = ComputeNodeInfo(node_id="rdagent-node1", api_base_url="http://192.168.50.215:9000")
    bindings = _sync_remote_runtime_artifacts(
        workspace=workspace,
        node=node,
        node_id=node.node_id,
        artifact_client=client,
        remote_paths={"artifact_path": "/remote/artifacts/" + "a" * 64},
        runtime_file_manifest=manifest,
    )
    files = _remote_small_files(
        workspace=workspace,
        pred_pkl=workspace / "combined_prediction.pkl",
        include_prediction=False,
        cas_bound_names={str(item["name"]) for item in bindings},
        runtime_file_manifest=manifest,
    )
    command = _remote_wsl_command(
        workspace=workspace,
        remote_paths={
            "artifact_path": "/remote/artifacts/" + "a" * 64,
            "prediction_artifact_path": "/remote/artifacts/" + "b" * 64,
            "qlib_data_path": "/home/node/data/qlib_bin",
            "factor_cache_dir": "/home/node/data/factors",
        },
        backtest_config={},
        runtime_artifact_bindings=bindings,
        runtime_file_manifest=manifest,
    )

    assert {path.relative_to(workspace).as_posix() for path in client.calls} == {
        "aistock_models/model.py",
        "weights.npz",
    }
    assert set(files) == {"conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"}
    assert "mkdir -p" in command and "aistock_models" in command
    assert ": >" in command and "aistock_models/__init__.py" in command
    assert "rglob" in command and "*.b64" in command
    assert entries["conf.yaml"]["sha256"] in command
    assert entries["aistock_models/model.py"]["sha256"] in command
    assert "QE_RUNTIME_FILE_VERIFY_FAILED" in command
    assert "kind=missing" in command
    assert "kind=size" in command
    assert "kind=sha256" in command
    assert "stat -Lc %s --" in command
    syntax = subprocess.run(
        ["bash", "-n", "-c", command],
        check=False,
        capture_output=True,
        text=True,
    )
    assert syntax.returncode == 0, syntax.stderr


def test_runtime_file_verification_dereferences_cas_symlink_and_keeps_sha_fail_closed(
    tmp_path: Path,
) -> None:
    source_workspace = tmp_path / "source"
    source_package = source_workspace / "aistock_models"
    source_package.mkdir(parents=True)
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (source_workspace / name).write_text("runtime\n", encoding="utf-8")
    expected_content = b"from .efficient_gats import EfficientGATs\n"
    source_file = source_package / "__init__.py"
    source_file.write_bytes(expected_content)
    manifest = _build_remote_runtime_file_manifest(workspace=source_workspace)

    runtime_workspace = tmp_path / "runtime"
    runtime_package = runtime_workspace / "aistock_models"
    runtime_package.mkdir(parents=True)
    cas_object = tmp_path / hashlib.sha256(expected_content).hexdigest()
    cas_object.write_bytes(expected_content)
    runtime_file = runtime_package / "__init__.py"
    try:
        runtime_file.symlink_to(cas_object)
    except OSError as exc:
        pytest.skip(f"filesystem cannot create the CAS symlink required by this contract test: {exc}")

    command = _remote_runtime_file_verify_commands(manifest)
    verified = subprocess.run(
        ["bash", "-c", command],
        cwd=runtime_workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    assert verified.returncode == 0, verified.stderr

    cas_object.write_bytes(b"x" * len(expected_content))
    rejected = subprocess.run(
        ["bash", "-c", command],
        cwd=runtime_workspace,
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode == 93
    assert "QE_RUNTIME_FILE_VERIFY_FAILED path=aistock_models/__init__.py kind=sha256" in rejected.stderr


def test_runtime_file_manifest_detects_workspace_mutation(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    manifest = _build_remote_runtime_file_manifest(workspace=workspace)
    (workspace / "conf.yaml").write_text("changed", encoding="utf-8")

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _remote_small_files(
            workspace=workspace,
            pred_pkl=workspace / "combined_prediction.pkl",
            include_prediction=False,
            runtime_file_manifest=manifest,
        )

    assert excinfo.value.reason_code == "remote_runtime_file_manifest_mismatch"


def test_runtime_artifact_binding_rejects_duplicate_workspace_name() -> None:
    binding = {
        "name": "overlay.parquet",
        "binding": "workspace_artifact_cas",
        "sha256": "a" * 64,
        "size": 11,
        "remote_path": "/remote/artifacts/" + "a" * 64,
    }

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _remote_runtime_artifact_link_commands([binding, binding])

    assert excinfo.value.reason_code == "remote_runtime_artifact_binding_invalid"


def test_small_file_packager_does_not_silently_exclude_unbound_file(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    pred = workspace / "combined_prediction.pkl"
    pred.write_bytes(b"pred")

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _remote_small_files(
            workspace=workspace,
            pred_pkl=pred,
            include_prediction=False,
            cas_bound_names={"conf.yaml"},
        )

    assert excinfo.value.reason_code == "remote_runtime_artifact_binding_invalid"


def test_runtime_artifact_cas_receipt_mismatch_is_loud(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    for name in ("conf.yaml", "qrun_limit_minute.py", "read_exp_res.py"):
        (workspace / name).write_text("content", encoding="utf-8")
    runtime_artifact = workspace / "overlay.parquet"
    runtime_artifact.write_bytes(b"r" * (8 * 1024 * 1024))

    class _MismatchedReceiptClient:
        def ensure_artifact(self, path: Path, *, node_id: str) -> dict[str, Any]:
            return {
                "sha256": "0" * 64,
                "size": path.stat().st_size,
                "uploaded": False,
                "status": {
                    "exists": True,
                    "size": path.stat().st_size,
                    "artifact_store_root": "/remote/artifacts",
                },
            }

    node = ComputeNodeInfo(node_id="rdagent-node1", api_base_url="http://192.168.50.215:9000")
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _sync_remote_runtime_artifacts(
            workspace=workspace,
            node=node,
            node_id=node.node_id,
            artifact_client=_MismatchedReceiptClient(),  # type: ignore[arg-type]
            remote_paths={"artifact_path": "/remote/artifacts/" + "a" * 64},
        )

    assert excinfo.value.reason_code == "remote_runtime_artifact_receipt_mismatch"


def test_remote_small_file_sync_posts_loop_scoped_files(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_post(url: str, json: dict[str, Any], **_kwargs: Any) -> _Response:
        calls.append({"url": url, "json": json})
        return _Response(status_code=200, payload={"success": True})

    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.post", fake_post)
    executor = RemotePredBacktestExecutor()

    executor._sync_small_files(
        node=ComputeNodeInfo(node_id="rdagent-node1", api_base_url="http://192.168.50.215:9000"),
        task_id="macb_remote_run_child",
        loop_index=3,
        files={"conf.yaml": "cfg", "combined_prediction.pkl.b64": "abc"},
        timeout_seconds=30,
    )

    assert calls == [
        {
            "url": "http://192.168.50.215:9000/api/qe/experiments/macb_remote_run_child/files/batch",
            "json": {"files": {"Loop3/conf.yaml": "cfg", "Loop3/combined_prediction.pkl.b64": "abc"}},
        }
    ]


def test_remote_small_file_sync_failure_is_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_post(url: str, json: dict[str, Any], **_kwargs: Any) -> _Response:
        return _Response(status_code=200, payload={"success": False, "failed": [{"filename": "Loop1/conf.yaml"}]})

    monkeypatch.setattr("backend.services.multi_alpha.remote_dispatch.requests.post", fake_post)
    executor = RemotePredBacktestExecutor()

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        executor._sync_small_files(
            node=ComputeNodeInfo(node_id="rdagent-node1", api_base_url="http://192.168.50.215:9000"),
            task_id="macb_remote_run_child",
            loop_index=1,
            files={"conf.yaml": "cfg"},
            timeout_seconds=30,
        )

    assert excinfo.value.reason_code == "remote_small_file_sync_failed"


def test_remote_pred_backtest_executor_requires_remote_paths(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    template = _runtime_template(tmp_path)
    workspace.mkdir()
    pred = workspace / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred)
    (workspace / "combined_factors_df.parquet").write_bytes(b"parquet")
    executor = RemotePredBacktestExecutor(
        node_resolver=lambda node_id: ComputeNodeInfo(node_id=node_id, api_base_url="http://192.168.50.215:9000"),
        artifact_client_factory=lambda _node_id: _FakeArtifactClient(),
        workspace_client_factory=lambda _node_id: _FakeWorkspaceClient(),
        small_file_syncer=lambda **_kwargs: None,
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        executor.execute_pred_backtest(
            workspace=workspace,
            pred_pkl=pred,
            node_id="rdagent-node1",
            backtest_config={"runtime_template_dir": str(template), "timeout_seconds": 30},
        )

    assert excinfo.value.reason_code == "remote_qlib_data_path_missing"


def test_remote_pred_backtest_executor_requires_artifact_store_root(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    template = _runtime_template(tmp_path)
    workspace.mkdir()
    pred = workspace / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred)
    (workspace / "combined_factors_df.parquet").write_bytes(b"parquet")

    class _NoRootArtifactClient(_FakeArtifactClient):
        def ensure_artifact(self, path: Path, *, node_id: str, verify_after_upload: bool = True) -> dict[str, Any]:
            return {"sha256": "a" * 64, "size": path.stat().st_size, "uploaded": False, "status": {"exists": True, "size": path.stat().st_size}}

    executor = RemotePredBacktestExecutor(
        node_resolver=lambda node_id: ComputeNodeInfo(
            node_id=node_id,
            api_base_url="http://192.168.50.215:9000",
            factor_data_dir="/home/node/aistock_cache/factor_values",
            qlib_data_path="/home/node/data/qlib_bin",
        ),
        artifact_client_factory=lambda _node_id: _NoRootArtifactClient(),
        workspace_client_factory=lambda _node_id: _FakeWorkspaceClient(),
        small_file_syncer=lambda **_kwargs: None,
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        executor.execute_pred_backtest(
            workspace=workspace,
            pred_pkl=pred,
            node_id="rdagent-node1",
            backtest_config={"runtime_template_dir": str(template), "timeout_seconds": 30},
        )

    assert excinfo.value.reason_code == "remote_artifact_store_root_missing"


def test_remote_pred_backtest_executor_rejects_local_paths(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    template = _runtime_template(tmp_path)
    workspace.mkdir()
    pred = workspace / "combined_prediction.pkl"
    pd.DataFrame({"score": [1.0]}).to_pickle(pred)
    (workspace / "combined_factors_df.parquet").write_bytes(b"parquet")
    executor = RemotePredBacktestExecutor(
        node_resolver=lambda node_id: ComputeNodeInfo(
            node_id=node_id,
            api_base_url="http://192.168.50.215:9000",
            factor_data_dir="/mnt/f/local/factor_values",
            qlib_data_path="/home/node/data/qlib_bin",
        ),
        artifact_client_factory=lambda _node_id: _FakeArtifactClient(),
        workspace_client_factory=lambda _node_id: _FakeWorkspaceClient(),
        small_file_syncer=lambda **_kwargs: None,
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        executor.execute_pred_backtest(
            workspace=workspace,
            pred_pkl=pred,
            node_id="rdagent-node1",
            backtest_config={"runtime_template_dir": str(template), "timeout_seconds": 30},
        )

    assert excinfo.value.reason_code == "remote_path_invalid"


def test_remote_path_allows_windows_mount_for_loopback_wsl_node() -> None:
    node = ComputeNodeInfo(node_id="wsl2-5080", api_base_url="http://127.0.0.1:9000")

    _require_remote_linux_path(
        path_name="artifact_store_root",
        value="/mnt/f/Dev/RD-Agent-state/artifact_cas",
        node=node,
    )
    _require_remote_linux_path(
        path_name="workspace_base",
        value="/mnt/f/Dev/RD-Agent-main/qe_workspace",
        node=node,
    )


@pytest.mark.parametrize(
    ("node", "value"),
    [
        (
            ComputeNodeInfo(node_id="wsl2-remote", api_base_url="http://192.168.50.215:9000"),
            "/mnt/f/Dev/RD-Agent-state/artifact_cas",
        ),
        (
            ComputeNodeInfo(node_id="local-linux", api_base_url="http://127.0.0.1:9000"),
            "/mnt/f/Dev/RD-Agent-state/artifact_cas",
        ),
        (
            ComputeNodeInfo(node_id="wsl2-5080", api_base_url="http://127.0.0.1:9000"),
            "F:\\Dev\\RD-Agent-state\\artifact_cas",
        ),
        (
            ComputeNodeInfo(node_id="wsl2-5080", api_base_url="http://127.0.0.1:9000"),
            "/mnt/shared/artifact_cas",
        ),
    ],
)
def test_remote_path_keeps_non_local_wsl_and_windows_paths_fail_closed(
    node: ComputeNodeInfo,
    value: str,
) -> None:
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _require_remote_linux_path(
            path_name="artifact_store_root",
            value=value,
            node=node,
        )

    assert excinfo.value.reason_code == "remote_path_invalid"


def test_remote_task_id_default_is_unique_per_child_workspace(tmp_path: Path) -> None:
    run_root = tmp_path / "macb_run_1"
    task_a = _remote_task_id(backtest_config={}, workspace=run_root / "combined_ic_weighted")
    task_b = _remote_task_id(backtest_config={}, workspace=run_root / "loo_leg_1")

    assert task_a != task_b
    assert task_a.endswith("_combined_ic_weighted")
    assert task_b.endswith("_loo_leg_1")


def test_remote_loop_failure_includes_run_log_tail() -> None:
    class _FailedWorkspaceClient(_FakeWorkspaceClient):
        async def get_loop_status(self, task_id: str, loop_id: str) -> dict[str, Any]:
            return {"status": "failed", "exit_code": 7}

        async def get_workspace_file(self, task_id: str, loop_id: str, file_path: str) -> dict[str, Any] | str:
            assert file_path == "run.log"
            return "x" * 2100 + "tail"

    executor = RemotePredBacktestExecutor(
        workspace_client_factory=lambda _node_id: _FailedWorkspaceClient(),
        submission_coordinator=_FakeSubmissionCoordinator(),
        poll_interval_seconds=0.0,
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        import asyncio

        asyncio.run(
            executor._run_remote_loop(
                node_id="rdagent-node1",
                task_id="task",
                loop_index=1,
                config={},
                experiment_files={},
                wsl_command="bash -lc true",
                timeout_seconds=30,
                run_id="macb_test",
                backtest_name="failed_child",
                requested_node_capacity=4,
            )
        )

    assert excinfo.value.reason_code == "remote_pred_backtest_failed"
    assert str(excinfo.value.context["stderr_tail"]).endswith("tail")


def test_remote_loop_timeout_is_loud() -> None:
    class _RunningWorkspaceClient(_FakeWorkspaceClient):
        async def get_loop_status(self, task_id: str, loop_id: str) -> dict[str, Any]:
            return {"status": "running"}

    executor = RemotePredBacktestExecutor(
        workspace_client_factory=lambda _node_id: _RunningWorkspaceClient(),
        submission_coordinator=_FakeSubmissionCoordinator(),
        poll_interval_seconds=0.0,
    )

    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        import asyncio

        asyncio.run(
            executor._run_remote_loop(
                node_id="rdagent-node1",
                task_id="task",
                loop_index=1,
                config={},
                experiment_files={},
                wsl_command="bash -lc true",
                timeout_seconds=0,
                run_id="macb_test",
                backtest_name="running_child",
                requested_node_capacity=4,
            )
        )

    assert excinfo.value.reason_code == "remote_pred_backtest_timeout"


def test_remote_wsl_command_uses_remote_paths_not_local_paths() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/workspace"),
        remote_paths={"artifact_path": "/remote/artifacts/abc", "prediction_artifact_path": "/remote/artifacts/pred", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
        backtest_config={"remote_conda_env": "AIstock"},
    )

    assert "/home/node/qlib" in command
    assert "/home/node/factor_values" in command
    assert "/remote/artifacts/abc" in command
    assert "RDAGENT_FACTOR_DATA_WSL=" in command
    assert "FACTOR_CACHE_DATA_MODE=" in command
    assert "/mnt/f/local/workspace" not in command
    assert "source ~/miniconda3/etc/profile.d/conda.sh" in command
    assert "conda activate" in command and "AIstock" in command


def test_remote_wsl_command_exports_jinja_runtime_env() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/workspace"),
        remote_paths={"artifact_path": "/remote/artifacts/abc", "prediction_artifact_path": "/remote/artifacts/pred", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
        backtest_config={"num_features": 44, "num_timesteps": 20, "remote_env": {"CUSTOM_FLAG": "yes"}},
    )

    assert "export num_features=" in command and "44" in command
    assert "export num_timesteps=" in command and "20" in command
    assert "export CUSTOM_FLAG=" in command and "yes" in command
    assert "source ~/miniconda3/etc/profile.d/conda.sh" not in command
    assert "conda activate" not in command
    assert "python qrun_limit_minute.py" in command


def test_remote_wsl_command_activates_only_explicit_conda_env() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/workspace"),
        remote_paths={"artifact_path": "/remote/artifacts/abc", "prediction_artifact_path": "/remote/artifacts/pred", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
        backtest_config={"remote_conda_env": "rdagent-gpu"},
    )

    assert "source ~/miniconda3/etc/profile.d/conda.sh" in command
    assert "conda activate" in command and "rdagent-gpu" in command
    assert "python qrun_limit_minute.py" in command


def test_remote_wsl_command_cd_to_uploaded_loop_workspace_when_available() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/run/combined_ic_weighted"),
        remote_paths={
            "artifact_path": "/remote/artifacts/abc",
            "prediction_artifact_path": "/remote/artifacts/pred",
            "qlib_data_path": "/home/node/qlib",
            "factor_cache_dir": "/home/node/factor_values",
            "workspace_base": "/home/lc999/projects/RD-Agent-main/qe_workspace",
        },
        backtest_config={"remote_task_id": "macb_remote_run_combined_ic_weighted", "remote_loop_index": 3},
    )

    assert "/home/lc999/projects/RD-Agent-main/qe_workspace/macb_remote_run_combined_ic_weighted/Loop3" in command
    assert command.index("cd ") < command.index("test -f conf.yaml")


def test_remote_wsl_command_rejects_invalid_env_key() -> None:
    with pytest.raises(MultiAlphaCombineBacktestError) as excinfo:
        _remote_wsl_command(
            workspace=Path("/mnt/f/local/workspace"),
            remote_paths={"artifact_path": "/remote/artifacts/abc", "prediction_artifact_path": "/remote/artifacts/pred", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
            backtest_config={"remote_env": {"bad-key": "x"}},
        )

    assert excinfo.value.reason_code == "remote_env_invalid"
