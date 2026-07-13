from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from backend.services.multi_alpha.combine_backtest import MultiAlphaCombineBacktestError, ShellPredBacktestExecutor
from backend.services.multi_alpha.remote_dispatch import (
    ComputeNodeInfo,
    RemotePredBacktestExecutor,
    WorkspaceArtifactSyncClient,
    _remote_small_files,
    _remote_task_id,
    _remote_wsl_command,
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
        return {
            "sha256": "a" * 64,
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


def test_remote_pred_backtest_executor_posts_loop_and_ingests_metrics(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    template = _runtime_template(tmp_path)
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
    assert fake_artifact.calls == [l2]
    assert fake_workspace.payloads
    payload = fake_workspace.payloads[0]
    assert payload["experiment_files"] == {}
    assert small_sync_calls and small_sync_calls[0]["task_id"].startswith("macb_remote_")
    assert small_sync_calls[0]["task_id"].endswith("_workspace")
    assert small_sync_calls[0]["loop_index"] == 1
    assert "combined_prediction.pkl.b64" in small_sync_calls[0]["files"]
    assert "bash -lc" in payload["wsl_command"]
    assert "/remote/artifacts/" + "a" * 64 in payload["wsl_command"]
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

    files = _remote_small_files(workspace=workspace, pred_pkl=pred)

    assert "conf.yaml" in files
    assert "qe_suspend_filter.json" in files
    assert "tail_twap_v25_1_strategy.py" in files
    assert "benchmark_sh000300.parquet.b64" in files
    assert "combined_prediction.pkl.b64" in files
    assert "combined_factors_df.parquet" not in files
    assert "combined_factors_df.parquet.b64" not in files
    assert "qlib_results_enhanced.json" not in files


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

    executor = RemotePredBacktestExecutor(workspace_client_factory=lambda _node_id: _FailedWorkspaceClient(), poll_interval_seconds=0.0)

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
            )
        )

    assert excinfo.value.reason_code == "remote_pred_backtest_failed"
    assert str(excinfo.value.context["stderr_tail"]).endswith("tail")


def test_remote_loop_timeout_is_loud() -> None:
    class _RunningWorkspaceClient(_FakeWorkspaceClient):
        async def get_loop_status(self, task_id: str, loop_id: str) -> dict[str, Any]:
            return {"status": "running"}

    executor = RemotePredBacktestExecutor(workspace_client_factory=lambda _node_id: _RunningWorkspaceClient(), poll_interval_seconds=0.0)

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
            )
        )

    assert excinfo.value.reason_code == "remote_pred_backtest_timeout"


def test_remote_wsl_command_uses_remote_paths_not_local_paths() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/workspace"),
        remote_paths={"artifact_path": "/remote/artifacts/abc", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
        backtest_config={"remote_conda_env": "AIstock"},
    )

    assert "/home/node/qlib" in command
    assert "/home/node/factor_values" in command
    assert "/remote/artifacts/abc" in command
    assert "RDAGENT_FACTOR_DATA_WSL=" in command
    assert "FACTOR_CACHE_DATA_MODE=" in command
    assert "/mnt/f/local/workspace" not in command


def test_remote_wsl_command_exports_jinja_runtime_env() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/workspace"),
        remote_paths={"artifact_path": "/remote/artifacts/abc", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
        backtest_config={"num_features": 44, "num_timesteps": 20, "remote_env": {"CUSTOM_FLAG": "yes"}},
    )

    assert "export num_features=" in command and "44" in command
    assert "export num_timesteps=" in command and "20" in command
    assert "export CUSTOM_FLAG=" in command and "yes" in command


def test_remote_wsl_command_cd_to_uploaded_loop_workspace_when_available() -> None:
    command = _remote_wsl_command(
        workspace=Path("/mnt/f/local/run/combined_ic_weighted"),
        remote_paths={
            "artifact_path": "/remote/artifacts/abc",
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
            remote_paths={"artifact_path": "/remote/artifacts/abc", "qlib_data_path": "/home/node/qlib", "factor_cache_dir": "/home/node/factor_values"},
            backtest_config={"remote_env": {"bad-key": "x"}},
        )

    assert excinfo.value.reason_code == "remote_env_invalid"
