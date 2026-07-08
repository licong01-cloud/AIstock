from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = PROJECT_ROOT / "scripts" / "qrun_limit_minute.py"


def _load_runner(monkeypatch: pytest.MonkeyPatch):
    qlib = types.ModuleType("qlib")
    qlib_model = types.ModuleType("qlib.model")
    qlib_model_trainer = types.ModuleType("qlib.model.trainer")
    qlib_model_trainer.task_train = lambda *args, **kwargs: None
    qlib_model_trainer.fill_placeholder = lambda cfg, values: cfg
    qlib_workflow = types.ModuleType("qlib.workflow")
    qlib_workflow.__path__ = []
    qlib_workflow_cli = types.ModuleType("qlib.workflow.cli")
    qlib_workflow_cli.sys_config = lambda *args, **kwargs: None
    qlib_record_temp = types.ModuleType("qlib.workflow.record_temp")
    qlib_workflow.record_temp = qlib_record_temp
    qlib_config = types.ModuleType("qlib.config")
    qlib_config.C = {"exp_manager": {"kwargs": {}}}

    for name, module in {
        "qlib": qlib,
        "qlib.model": qlib_model,
        "qlib.model.trainer": qlib_model_trainer,
        "qlib.workflow": qlib_workflow,
        "qlib.workflow.cli": qlib_workflow_cli,
        "qlib.workflow.record_temp": qlib_record_temp,
        "qlib.config": qlib_config,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    spec = importlib.util.spec_from_file_location("qrun_limit_minute_metric_retry_test", RUNNER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module, qlib_record_temp


class _FakeRecorder:
    def __init__(self, run_dir: Path) -> None:
        self.info = {"id": "run-1", "experiment_id": "0"}
        self._run_dir = run_dir

    def get_local_dir(self) -> str:
        return str(self._run_dir)


def test_qrun_record_check_retries_empty_mlflow_metric_once(tmp_path, monkeypatch, capsys) -> None:
    runner, record_temp = _load_runner(monkeypatch)
    monkeypatch.setenv(runner.MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC_ENV, "0")

    class FlakyRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.calls = 0

        @property
        def recorder(self):
            return self._recorder

        def check(self, include_self: bool = False, parents: bool = True):
            self.calls += 1
            if self.calls == 1:
                raise ValueError("Metric 'Rank IC' is malformed. No data found.")
            return "ok"

    record_temp.RecordTemp = FlakyRecordTemp
    runner._install_mlflow_metric_read_retry()

    run_dir = tmp_path / "mlruns" / "0" / "run-1"
    record = record_temp.RecordTemp(_FakeRecorder(run_dir))

    assert record.check() == "ok"
    assert record.calls == 2
    out = capsys.readouterr().out
    assert "transient MLflow empty metric read" in out
    assert "Rank IC" in out
    assert str(run_dir / "metrics" / "Rank IC") in out


def test_qrun_record_check_keeps_true_missing_artifact_loud(tmp_path, monkeypatch, capsys) -> None:
    runner, record_temp = _load_runner(monkeypatch)
    monkeypatch.setenv(runner.MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC_ENV, "0")

    class MissingArtifactRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.calls = 0

        @property
        def recorder(self):
            return self._recorder

        def check(self, include_self: bool = False, parents: bool = True):
            self.calls += 1
            raise FileNotFoundError("pred.pkl")

    record_temp.RecordTemp = MissingArtifactRecordTemp
    runner._install_mlflow_metric_read_retry()
    record = record_temp.RecordTemp(_FakeRecorder(tmp_path / "run"))

    with pytest.raises(FileNotFoundError, match="pred.pkl"):
        record.check()
    assert record.calls == 1
    assert "transient MLflow empty metric read" not in capsys.readouterr().out


def test_qrun_record_check_retry_exhaustion_reports_metric_and_path(tmp_path, monkeypatch) -> None:
    runner, record_temp = _load_runner(monkeypatch)
    monkeypatch.setenv(runner.MLFLOW_EMPTY_METRIC_RETRY_ATTEMPTS_ENV, "1")
    monkeypatch.setenv(runner.MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC_ENV, "0")

    class AlwaysEmptyMetricRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.calls = 0

        @property
        def recorder(self):
            return self._recorder

        def check(self, include_self: bool = False, parents: bool = True):
            self.calls += 1
            raise ValueError("Metric 'Rank IC' is malformed. No data found.")

    record_temp.RecordTemp = AlwaysEmptyMetricRecordTemp
    runner._install_mlflow_metric_read_retry()
    run_dir = tmp_path / "mlruns" / "0" / "run-1"
    record = record_temp.RecordTemp(_FakeRecorder(run_dir))

    with pytest.raises(runner.QEMlflowMetricReadRaceError) as exc_info:
        record.check()
    assert record.calls == 2
    message = str(exc_info.value)
    assert "Rank IC" in message
    assert str(run_dir / "metrics" / "Rank IC") in message
