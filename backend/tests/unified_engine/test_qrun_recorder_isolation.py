import importlib.util
import os
import pickle
import sys
import types
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _clear_qe_backtest_env():
    names = (
        "QE_BACKTEST_SOURCE_PARAMS_DIR",
        "QE_BACKTEST_SOURCE_MLRUNS_DIR",
        "QE_BACKTEST_ALLOW_LEGACY_MLRUNS_SOURCE",
        "MLFLOW_TRACKING_URI",
    )
    for name in names:
        os.environ.pop(name, None)
    yield
    for name in names:
        os.environ.pop(name, None)


def _install_qrun_stubs(monkeypatch):
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
    module_path = Path("scripts/qrun_limit_minute.py").resolve()
    spec = importlib.util.spec_from_file_location("qrun_limit_minute_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _write_params(root: Path):
    params_path = root / "1" / "run-a" / "artifacts" / "params.pkl"
    params_path.parent.mkdir(parents=True, exist_ok=True)
    params_path.write_bytes(pickle.dumps({"model": "ok"}))
    return params_path


def test_backtest_only_target_mlruns_rejects_symlink(tmp_path, monkeypatch):
    runner = _load_runner(monkeypatch)
    source = tmp_path / "source"
    _write_params(source)
    target = tmp_path / "mlruns"
    try:
        target.symlink_to(source, target_is_directory=True)
    except OSError as exc:
        target.mkdir()
        original_is_symlink = runner.Path.is_symlink

        def fake_is_symlink(path):
            if path.resolve() == target.resolve():
                return True
            return original_is_symlink(path)

        monkeypatch.setattr(runner.Path, "is_symlink", fake_is_symlink)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(runner.SOURCE_PARAMS_ENV, str(source))
    with pytest.raises(runner.BacktestRecorderIsolationError, match=runner.ERR_TARGET_MLRUNS_SYMLINK):
        runner._prepare_backtest_recorder_isolation("exp")


def test_backtest_only_rejects_same_source_target_realpath(tmp_path, monkeypatch):
    runner = _load_runner(monkeypatch)
    target = tmp_path / "mlruns"
    _write_params(target)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(runner.SOURCE_MLRUNS_ENV, str(target))
    monkeypatch.setenv(runner.SOURCE_PARAMS_ENV, str(target))
    with pytest.raises(runner.BacktestRecorderIsolationError, match=runner.ERR_REALPATH_COLLISION):
        runner._prepare_backtest_recorder_isolation("exp")


def test_backtest_only_rejects_target_under_source_mlruns(tmp_path, monkeypatch):
    runner = _load_runner(monkeypatch)
    source = tmp_path / "source_mlruns"
    _write_params(source)
    loop_dir = source / "Loop2"
    loop_dir.mkdir(parents=True)

    monkeypatch.chdir(loop_dir)
    monkeypatch.setenv(runner.SOURCE_MLRUNS_ENV, str(source))
    monkeypatch.setenv(runner.SOURCE_PARAMS_ENV, str(source))
    with pytest.raises(runner.BacktestRecorderIsolationError, match=runner.ERR_RECORDER_NOT_ISOLATED):
        runner._prepare_backtest_recorder_isolation("exp")


def test_qe_recorder_isolation_manifest_written_and_payload_relocated(tmp_path, monkeypatch):
    runner = _load_runner(monkeypatch)
    extracted_source = tmp_path / "mlruns"
    params_path = _write_params(extracted_source)

    monkeypatch.chdir(tmp_path)
    payload = runner._prepare_backtest_recorder_isolation(
        "exp",
        {"source_task_id": "qe_src", "source_loop": "Loop1", "target_loop_id": "Loop2"},
    )

    manifest = tmp_path / runner.RECORDER_ISOLATION_FILE
    assert manifest.exists()
    assert payload["recorder_isolation_status"] == "passed"
    assert payload["source_task_id"] == "qe_src"
    assert payload["target_mlruns_is_symlink"] is False
    assert Path(payload["source_mlruns_realpath"]) != Path(payload["target_mlruns_realpath"])
    assert not (tmp_path / "mlruns").is_symlink()
    assert (tmp_path / "mlruns").is_dir()
    assert not params_path.exists()
    assert (tmp_path / "source_model" / "mlruns").exists()
    assert os.environ["MLFLOW_TRACKING_URI"] == payload["target_mlruns_realpath"]


def test_backtest_only_final_validation_rejects_target_reparse_swap(tmp_path, monkeypatch):
    runner = _load_runner(monkeypatch)
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


def test_backtest_only_final_validation_rejects_changed_target_realpath(tmp_path, monkeypatch):
    runner = _load_runner(monkeypatch)
    extracted_source = tmp_path / "mlruns"
    _write_params(extracted_source)

    monkeypatch.chdir(tmp_path)
    payload = runner._prepare_backtest_recorder_isolation("exp")
    payload = {**payload, "target_mlruns_realpath": str(tmp_path / "other_mlruns")}

    with pytest.raises(runner.BacktestRecorderIsolationError, match=runner.ERR_RECORDER_NOT_ISOLATED):
        runner._validate_backtest_recorder_isolation_manifest(payload)
