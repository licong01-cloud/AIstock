from __future__ import annotations

import asyncio
import base64
import importlib.util
import os
import pickle
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from backend.services.quantevolver.experiment_config import ExperimentConfig
from backend.services.quantevolver.executors.backtest import BacktestExecutor, BacktestMode
from backend.services.quantevolver.executors.base import ExecutionContext


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = PROJECT_ROOT / "scripts" / "qrun_limit_minute.py"
RUNNER_SOURCE = RUNNER_PATH.read_text(encoding="utf-8")

HAS_RECORDER_ISOLATION_CONTRACT = all(
    token in RUNNER_SOURCE
    for token in (
        "qe_recorder_isolation.json",
        "QE_BACKTEST_TARGET_MLRUNS_IS_SYMLINK",
        "QE_BACKTEST_SOURCE_TARGET_REALPATH_COLLISION",
    )
)
HAS_SOURCE_PARAMS_CONTRACT = (
    "QE_BACKTEST_SOURCE_PARAMS_DIR" in RUNNER_SOURCE
    or "source_model" in RUNNER_SOURCE
)
HAS_RECORDER_HELPERS = all(
    token in RUNNER_SOURCE
    for token in (
        "BacktestRecorderIsolationError",
        "_prepare_backtest_recorder_isolation",
        "_validate_backtest_recorder_isolation_manifest",
    )
)


def _install_qrun_stubs(monkeypatch) -> None:
    qlib = types.ModuleType("qlib")
    qlib_model = types.ModuleType("qlib.model")
    qlib_model_trainer = types.ModuleType("qlib.model.trainer")
    qlib_model_trainer.task_train = lambda *args, **kwargs: None
    qlib_model_trainer.fill_placeholder = lambda cfg, values: cfg
    qlib_workflow = types.ModuleType("qlib.workflow")
    qlib_workflow_cli = types.ModuleType("qlib.workflow.cli")
    qlib_workflow_cli.sys_config = lambda *args, **kwargs: None
    qlib_config = types.ModuleType("qlib.config")
    qlib_config.C = {"exp_manager": {"kwargs": {}}}

    for name, module in {
        "qlib": qlib,
        "qlib.model": qlib_model,
        "qlib.model.trainer": qlib_model_trainer,
        "qlib.workflow": qlib_workflow,
        "qlib.workflow.cli": qlib_workflow_cli,
        "qlib.config": qlib_config,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _load_runner(monkeypatch):
    _install_qrun_stubs(monkeypatch)
    spec = importlib.util.spec_from_file_location("qrun_limit_minute_hotfix_test", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _write_params(root: Path) -> Path:
    params_path = root / "1" / "run-a" / "artifacts" / "params.pkl"
    params_path.parent.mkdir(parents=True, exist_ok=True)
    params_path.write_bytes(pickle.dumps({"model": "ok"}))
    return params_path


def _executor_context(loop_index: int) -> ExecutionContext:
    return ExecutionContext(
        task_id="qe_target",
        loop_index=loop_index,
        experiment_name=f"qe_target/Loop{loop_index}",
        model_source={
            "source_task_id": "qe_source",
            "source_loop": "Loop1",
            "cross_node": True,
        },
        extra_experiment_files={
            "mlruns_params.tar.gz.b64": base64.b64encode(b"params-tar").decode("ascii"),
        },
    )


@pytest.mark.xfail(
    not HAS_RECORDER_ISOLATION_CONTRACT,
    reason=(
        "Agent A recorder-isolation runner contract is not present yet; "
        "target mlruns symlink must fail fast once implemented."
    ),
    strict=True,
)
def test_backtest_only_target_mlruns_rejects_symlink_contract() -> None:
    assert "is_symlink" in RUNNER_SOURCE
    assert "QE_BACKTEST_TARGET_MLRUNS_IS_SYMLINK" in RUNNER_SOURCE
    assert "qe_recorder_isolation.json" in RUNNER_SOURCE


@pytest.mark.xfail(
    not HAS_RECORDER_ISOLATION_CONTRACT,
    reason=(
        "Agent A recorder-isolation runner contract is not present yet; "
        "source/target realpath collision must fail fast once implemented."
    ),
    strict=True,
)
def test_backtest_only_rejects_same_source_target_realpath_contract() -> None:
    assert "realpath" in RUNNER_SOURCE or ".resolve()" in RUNNER_SOURCE
    assert "QE_BACKTEST_SOURCE_TARGET_REALPATH_COLLISION" in RUNNER_SOURCE
    assert "source_mlruns_realpath" in RUNNER_SOURCE
    assert "target_mlruns_realpath" in RUNNER_SOURCE


@pytest.mark.xfail(
    not HAS_RECORDER_ISOLATION_CONTRACT,
    reason=(
        "Agent A recorder-isolation runner contract is not present yet; "
        "target mlruns under source mlruns must fail fast once implemented."
    ),
    strict=True,
)
def test_backtest_only_rejects_target_under_source_mlruns_contract() -> None:
    assert "QE_BACKTEST_RECORDER_NOT_ISOLATED" in RUNNER_SOURCE
    assert "commonpath" in RUNNER_SOURCE or "is_relative_to" in RUNNER_SOURCE
    assert "target_mlruns_realpath" in RUNNER_SOURCE


@pytest.mark.xfail(
    not HAS_SOURCE_PARAMS_CONTRACT,
    reason=(
        "Agent A source-params reader is not present yet; backtest-only must "
        "load model params from source_model/QE_BACKTEST_SOURCE_PARAMS_DIR, not target mlruns."
    ),
    strict=True,
)
def test_backtest_only_prefers_source_params_dir_before_target_mlruns() -> None:
    source_markers = [
        idx
        for marker in ("QE_BACKTEST_SOURCE_PARAMS_DIR", "source_model")
        if (idx := RUNNER_SOURCE.find(marker)) >= 0
    ]
    assert source_markers, "runner must expose a source params directory contract"

    loose_target_marker = RUNNER_SOURCE.find('_load_backtest_only_model_from_loose_params(Path("mlruns"))')
    if loose_target_marker >= 0:
        assert min(source_markers) < loose_target_marker
    assert "MLFLOW_TRACKING_URI" in RUNNER_SOURCE
    assert "qe_recorder_isolation.json" in RUNNER_SOURCE


@pytest.mark.xfail(
    not HAS_RECORDER_HELPERS,
    reason=(
        "Agent A behavior helpers are not present yet; after integration this "
        "must verify concrete filesystem isolation instead of source text only."
    ),
    strict=True,
)
def test_backtest_only_behavior_writes_isolation_manifest_and_target_binding(tmp_path, monkeypatch) -> None:
    runner = _load_runner(monkeypatch)
    monkeypatch.delenv(runner.SOURCE_PARAMS_ENV, raising=False)
    monkeypatch.delenv(runner.SOURCE_MLRUNS_ENV, raising=False)
    extracted_source = tmp_path / "mlruns"
    params_path = _write_params(extracted_source)

    monkeypatch.chdir(tmp_path)
    payload = runner._prepare_backtest_recorder_isolation(
        "exp",
        {"source_task_id": "qe_src", "source_loop": "Loop1", "target_loop_id": "Loop2"},
    )
    runner._validate_backtest_recorder_isolation_manifest(payload)

    assert (tmp_path / runner.RECORDER_ISOLATION_FILE).exists()
    assert payload["recorder_isolation_status"] == "passed"
    assert payload["source_task_id"] == "qe_src"
    assert Path(payload["source_mlruns_realpath"]) != Path(payload["target_mlruns_realpath"])
    assert not params_path.exists()
    assert (tmp_path / "source_model" / "mlruns").exists()
    assert (tmp_path / "mlruns").is_dir()
    assert os.environ["MLFLOW_TRACKING_URI"] == payload["target_mlruns_realpath"]


@pytest.mark.xfail(
    not HAS_RECORDER_HELPERS,
    reason="Agent A final recorder validation helper is not present yet.",
    strict=True,
)
def test_backtest_only_behavior_rejects_target_reparse_swap(tmp_path, monkeypatch) -> None:
    runner = _load_runner(monkeypatch)
    monkeypatch.delenv(runner.SOURCE_PARAMS_ENV, raising=False)
    monkeypatch.delenv(runner.SOURCE_MLRUNS_ENV, raising=False)
    extracted_source = tmp_path / "mlruns"
    _write_params(extracted_source)

    monkeypatch.chdir(tmp_path)
    payload = runner._prepare_backtest_recorder_isolation("exp")
    target = tmp_path / "mlruns"

    def fake_reparse_or_symlink(path):
        return path.resolve() == target.resolve()

    monkeypatch.setattr(runner, "_is_reparse_or_symlink", fake_reparse_or_symlink)
    with pytest.raises(runner.BacktestRecorderIsolationError, match=runner.ERR_TARGET_MLRUNS_SYMLINK):
        runner._validate_backtest_recorder_isolation_manifest(payload)


def test_two_backtest_only_target_loops_reuse_one_source_payload_independently() -> None:
    composer = MagicMock()
    composer.compose_experiment_in_memory.return_value = {
        "experiment_files": {"conf.yaml": "mock_yaml"},
        "wsl_command": "python qrun_limit_minute.py conf.yaml",
    }
    client = AsyncMock()
    client.create_and_run_loop.side_effect = ["Loop1", "Loop2"]
    executor = BacktestExecutor(composer, client)
    cfg = ExperimentConfig(factor_names=["f1"], model_id="lgbm")

    results = [
        asyncio.run(executor.submit(cfg, _executor_context(1), mode=BacktestMode.BACKTEST_ONLY)),
        asyncio.run(executor.submit(cfg, _executor_context(2), mode=BacktestMode.BACKTEST_ONLY)),
    ]

    assert [result.job_id for result in results] == ["Loop1", "Loop2"]
    assert all("--backtest-only" in result.wsl_command for result in results)
    calls = client.create_and_run_loop.call_args_list
    assert [call.args[1] for call in calls] == [1, 2]
    assert calls[0].kwargs["model_source"] == calls[1].kwargs["model_source"]
    assert calls[0].args[3]["mlruns_params.tar.gz.b64"] == calls[1].args[3]["mlruns_params.tar.gz.b64"]

