from __future__ import annotations

import importlib.util
import hashlib
import json
import sys
import types
from pathlib import Path

import pandas as pd
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNNER_PATH = PROJECT_ROOT / "scripts" / "qrun_limit_minute.py"
DAY_RUNNER_PATH = PROJECT_ROOT / "scripts" / "qrun_limit.py"


class _FakeQlibConfig(dict):
    def get_kernels(self, freq: str) -> int:
        del freq
        return int(self["kernels"])


def _load_runner(monkeypatch: pytest.MonkeyPatch, runner_path: Path = RUNNER_PATH):
    qlib = types.ModuleType("qlib")
    qlib_model = types.ModuleType("qlib.model")
    qlib_model_trainer = types.ModuleType("qlib.model.trainer")
    qlib_model_trainer.task_train = lambda *args, **kwargs: None
    qlib_model_trainer.fill_placeholder = lambda cfg, values: cfg
    qlib_data = types.ModuleType("qlib.data")
    qlib_data_dataset = types.ModuleType("qlib.data.dataset")
    qlib_data_dataset.Dataset = object
    qlib_model_base = types.ModuleType("qlib.model.base")
    qlib_model_base.Model = object
    qlib_utils = types.ModuleType("qlib.utils")
    qlib_utils.init_instance_by_config = lambda *args, **kwargs: None
    qlib_workflow = types.ModuleType("qlib.workflow")
    qlib_workflow.__path__ = []
    qlib_workflow_cli = types.ModuleType("qlib.workflow.cli")
    qlib_workflow_cli.sys_config = lambda *args, **kwargs: None
    qlib_workflow_cli.task_train = lambda *args, **kwargs: None
    qlib_record_temp = types.ModuleType("qlib.workflow.record_temp")
    qlib_workflow.record_temp = qlib_record_temp
    qlib_config = types.ModuleType("qlib.config")
    qlib_config.C = _FakeQlibConfig(kernels=26, exp_manager={"kwargs": {}})
    qlib.init_calls = []

    def fake_init(**kwargs) -> None:
        qlib.init_calls.append(dict(kwargs))
        # Model QlibConfig.set(): qlib.init resets C before applying kwargs.
        qlib_config.C["kernels"] = kwargs.get("kernels", 26)

    qlib.init = fake_init

    for name, module in {
        "qlib": qlib,
        "qlib.model": qlib_model,
        "qlib.model.trainer": qlib_model_trainer,
        "qlib.data": qlib_data,
        "qlib.data.dataset": qlib_data_dataset,
        "qlib.model.base": qlib_model_base,
        "qlib.utils": qlib_utils,
        "qlib.workflow": qlib_workflow,
        "qlib.workflow.cli": qlib_workflow_cli,
        "qlib.workflow.record_temp": qlib_record_temp,
        "qlib.config": qlib_config,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)

    spec = importlib.util.spec_from_file_location(f"{runner_path.stem}_metric_retry_test", runner_path)
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


def _minute_config(minute_root: Path, day_root: Path | None = None) -> dict:
    day_root = day_root or minute_root.parent / "day"
    return {
        "market": "filtered_pool_20260630",
        "qlib_init": {"provider_uri": {"day": str(day_root), "1min": str(minute_root)}},
        "port_analysis_config": {
            "executor": {"class": "NestedExecutor"},
            "backtest": {
                "start_time": "2026-06-01",
                "end_time": "2026-06-29",
                "exchange_kwargs": {"freq": "1min"},
            },
        },
    }


def test_qrun_minute_quote_universe_requires_day_minute_window_parity(tmp_path, monkeypatch) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    day_root = tmp_path / "day"
    minute_root = tmp_path / "minute"
    day_instruments = day_root / "instruments"
    minute_instruments = minute_root / "instruments"
    day_instruments.mkdir(parents=True)
    minute_instruments.mkdir(parents=True)
    pool_name = config_market = "filtered_pool_20260630"
    (day_instruments / f"{pool_name}.txt").write_text(
        "000001.SZ\t2018-08-01\t2026-06-30\n",
        encoding="utf-8",
    )
    config = _minute_config(minute_root, day_root)
    assert config["market"] == config_market

    with pytest.raises(RuntimeError, match="QE_MINUTE_INSTRUMENT_FILE_MISSING"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    minute_pool = minute_instruments / f"{pool_name}.txt"
    minute_pool.write_text("000001.SZ\tnot-a-date\t2026-06-30\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="QE_MINUTE_INSTRUMENT_FILE_INVALID"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    minute_pool.write_text("000001.SZ\t2024-01-02 09:30:00\t2026-04-28 15:00:00\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="QE_MINUTE_INSTRUMENT_COVERAGE_MISMATCH"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    # Instrument membership is a trading-day contract: a minute span beginning
    # at 09:30 on its first listed day must cover the day-level 00:00 boundary.
    minute_pool.write_text("000001.SZ\t2026-06-01 09:30:00\t2026-06-30 15:00:00\n", encoding="utf-8")
    runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)
    assert config["port_analysis_config"]["backtest"]["exchange_kwargs"]["codes"] == pool_name

    config["port_analysis_config"]["backtest"]["start_time"] = "not-a-date"
    with pytest.raises(RuntimeError, match="QE_MINUTE_BACKTEST_WINDOW_INVALID"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)


def test_qrun_minute_coverage_accepts_only_suspend_or_explicit_full_window_explanations(
    tmp_path, monkeypatch
) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    day_root = tmp_path / "day"
    minute_root = tmp_path / "minute"
    (day_root / "instruments").mkdir(parents=True)
    (day_root / "calendars").mkdir(parents=True)
    (minute_root / "instruments").mkdir(parents=True)
    pool_name = "filtered_pool_20260630"
    (day_root / "instruments" / f"{pool_name}.txt").write_text(
        "000627.SZ\t2026-06-01\t2026-06-29\n"
        "601989.SH\t2026-06-01\t2026-06-29\n",
        encoding="utf-8",
    )
    (minute_root / "instruments" / f"{pool_name}.txt").write_text(
        "000627.SZ\t2026-06-01 09:30:00\t2026-06-26 15:00:00\n"
        "601989.SH\t2026-06-01 09:30:00\t2026-06-26 15:00:00\n",
        encoding="utf-8",
    )
    (day_root / "calendars" / "day.txt").write_text(
        "2026-06-26\n2026-06-27\n2026-06-28\n2026-06-29\n",
        encoding="utf-8",
    )
    exclusions = [
        {
            "schema_version": "qe_execution_data_exclusion_v1",
            "instrument": "601989.SH",
            "scope": "full_backtest_window",
            "start_date": "2026-06-01",
            "end_date": "2026-06-29",
            "reason_code": "minute_source_gap_confirmed_unfillable",
            "evidence_sha256": "b" * 64,
        }
    ]
    artifact = {
        "enabled": True,
        "suspended_by_date": {
            "2026-06-26": [],
            "2026-06-27": ["000627.SZ"],
            "2026-06-28": ["000627.SZ"],
            "2026-06-29": ["000627.SZ"],
        },
        "execution_data_exclusions": exclusions,
        "execution_data_exclusion_contract_sha256": hashlib.sha256(
            json.dumps(
                exclusions,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }
    (tmp_path / "qe_suspend_filter.json").write_text(
        json.dumps(artifact), encoding="utf-8"
    )
    config = _minute_config(minute_root, day_root)

    runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    assert config["qe_minute_coverage_summary"] == {
        "schema_version": "qe_minute_coverage_summary_v1",
        "selection_market": pool_name,
        "expected_spans": 2,
        "suspension_explained_days": 3,
        "execution_exclusion_explained_days": 3,
        "execution_data_exclusion_count": 1,
    }

    artifact["execution_data_exclusions"] = []
    artifact["execution_data_exclusion_contract_sha256"] = hashlib.sha256(b"[]").hexdigest()
    (tmp_path / "qe_suspend_filter.json").write_text(
        json.dumps(artifact), encoding="utf-8"
    )
    with pytest.raises(RuntimeError, match="601989.SH:2026-06-27"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)


def test_qrun_builds_suspend_artifact_before_minute_coverage_validation() -> None:
    run_main_source = RUNNER_PATH.read_text(encoding="utf-8").split(
        "def _run_main(args):", 1
    )[1]

    assert run_main_source.index("ensure_frozen_suspend_filter_artifact(cwd=") < run_main_source.index(
        "_validate_minute_instrument_coverage_contract(config, cwd="
    )


@pytest.mark.parametrize("binding_schema", ["qe_direct_v2_dataset_binding_v2", "qe_direct_v2_dataset_binding_v3"])
def test_qrun_minute_quote_universe_excludes_day_only_benchmark_catalog_entry(
    tmp_path, monkeypatch, binding_schema
) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    day_root = tmp_path / "day"
    minute_root = tmp_path / "minute"
    day_instruments = day_root / "instruments"
    minute_instruments = minute_root / "instruments"
    day_instruments.mkdir(parents=True)
    minute_instruments.mkdir(parents=True)
    stock_row = "000001.SZ\t2018-08-01\t2026-06-30\n"
    benchmark_row = "000300.SH\t2018-08-01\t2026-06-30\n"
    (day_instruments / "all.txt").write_text(stock_row + benchmark_row, encoding="utf-8")
    (day_instruments / "stock_universe.txt").write_text(stock_row, encoding="utf-8")
    minute_all = minute_instruments / "all.txt"
    minute_all.write_text(
        "000001.SZ\t2026-06-01 09:30:00\t2026-06-30 15:00:00\n",
        encoding="utf-8",
    )
    config = _minute_config(minute_root, day_root)
    config["market"] = "stock_universe"
    selection_pins = {
        "stock_pool": "stock_universe",
        "instruments_sha256": hashlib.sha256(
            (day_instruments / "stock_universe.txt").read_bytes()
        ).hexdigest(),
    }
    if binding_schema == "qe_direct_v2_dataset_binding_v3":
        selection_pins = {
            "mode": "stock_universe",
            "pool_ids": [],
            "instrument_name": "stock_universe",
            "instruments_file": "stock_universe.txt",
            "instruments_sha256": selection_pins["instruments_sha256"],
        }
    binding_path = tmp_path / "qe_direct_v2_dataset_binding.json"
    binding_path.write_text(
        json.dumps(
            {
                "schema_version": binding_schema,
                "selection_pins": selection_pins,
                "minute_pins": {
                    "instruments_sha256": hashlib.sha256(minute_all.read_bytes()).hexdigest(),
                },
            }
        ),
        encoding="utf-8",
    )

    config["port_analysis_config"]["backtest"]["exchange_kwargs"]["codes"] = "stock_universe"
    with pytest.raises(RuntimeError, match="QE_MINUTE_INSTRUMENT_FILE_MISSING"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    config["port_analysis_config"]["backtest"]["exchange_kwargs"]["codes"] = "all"
    runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    assert config["market"] == "stock_universe"
    assert config["port_analysis_config"]["backtest"]["exchange_kwargs"]["codes"] == "all"

    minute_all.write_text(
        "000001.SZ\t2026-06-02 09:30:00\t2026-06-30 15:00:00\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="QE_MINUTE_INSTRUMENT_BINDING_HASH_MISMATCH"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)

    if binding_schema == "qe_direct_v2_dataset_binding_v3":
        invalid_binding = json.loads(binding_path.read_text(encoding="utf-8"))
        invalid_binding["selection_pins"]["instruments_file"] = "other.txt"
        binding_path.write_text(json.dumps(invalid_binding), encoding="utf-8")
        with pytest.raises(RuntimeError, match="QE_MINUTE_INSTRUMENT_BINDING_INVALID"):
            runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)


def test_qrun_minute_quote_universe_missing_market_fails_closed(tmp_path, monkeypatch) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    config = _minute_config(tmp_path / "minute")
    config["market"] = ""

    with pytest.raises(RuntimeError, match="QE_MINUTE_QUOTE_UNIVERSE_MISSING"):
        runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)


def test_qrun_pred_backtest_rejects_nonempty_prediction_with_zero_execution(tmp_path, monkeypatch) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    config = _minute_config(tmp_path / "minute")
    index = pd.MultiIndex.from_product(
        [pd.to_datetime(["2026-06-01", "2026-06-02"]), ["000001.SZ"]],
        names=["datetime", "instrument"],
    )
    prediction = pd.DataFrame({"score": [0.1, 0.2]}, index=index)

    class Recorder:
        indicators = pd.DataFrame({"count": [0, 0], "deal_amount": [0.0, 0.0]})

        def load_object(self, name: str):
            assert name == "portfolio_analysis/indicators_normal_1day.pkl"
            return self.indicators

    recorder = Recorder()
    with pytest.raises(RuntimeError, match="QE_MINUTE_BACKTEST_ZERO_TRADES"):
        runner._validate_pred_backtest_has_execution(recorder, config, prediction)

    recorder.indicators = pd.DataFrame({"count": [1, 0], "deal_amount": [1000.0, 0.0]})
    runner._validate_pred_backtest_has_execution(recorder, config, prediction)


def test_qrun_pred_backtest_filters_replayed_prediction_to_run_scoped_dataset(
    monkeypatch,
) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    prediction_index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-08-03"), "000001.SZ"),
            (pd.Timestamp("2026-08-03"), "000002.SZ"),
            (pd.Timestamp("2026-08-04"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    label_index = pd.MultiIndex.from_tuples(
        [
            (pd.Timestamp("2026-08-03"), "000002.SZ"),
            (pd.Timestamp("2026-08-04"), "000002.SZ"),
        ],
        names=["datetime", "instrument"],
    )
    prediction = pd.DataFrame({"score": [0.9, 0.8, 0.7]}, index=prediction_index)
    label = pd.DataFrame({"label": [0.1, 0.2]}, index=label_index)

    filtered = runner._filter_pred_backtest_to_dataset(prediction, label)

    assert filtered.index.tolist() == label_index.tolist()
    assert prediction.shape[0] == 3


def test_qrun_pred_backtest_rejects_empty_run_scoped_prediction(monkeypatch) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    prediction = pd.DataFrame(
        {"score": [0.9]},
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-08-03"), "000001.SZ")],
            names=["datetime", "instrument"],
        ),
    )
    label = pd.DataFrame(
        {"label": [0.1]},
        index=pd.MultiIndex.from_tuples(
            [(pd.Timestamp("2026-08-03"), "000002.SZ")],
            names=["datetime", "instrument"],
        ),
    )

    with pytest.raises(RuntimeError, match="QE_PRED_BACKTEST_UNIVERSE_EMPTY"):
        runner._filter_pred_backtest_to_dataset(prediction, label)


def test_qrun_minute_guards_do_not_change_non_minute_backtests(tmp_path, monkeypatch) -> None:
    runner, _record_temp = _load_runner(monkeypatch)
    config = _minute_config(tmp_path / "missing-minute")
    config["port_analysis_config"]["backtest"]["exchange_kwargs"]["freq"] = "day"

    class Recorder:
        def load_object(self, _name: str):
            raise AssertionError("non-minute guard must not inspect execution artifacts")

    prediction = pd.DataFrame()
    runner._validate_minute_instrument_coverage_contract(config, cwd=tmp_path)
    runner._validate_pred_backtest_has_execution(Recorder(), config, prediction)



@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_industry_provider_injection_only_for_industry_requested(tmp_path, monkeypatch, runner_path) -> None:
    runner, _record_temp = _load_runner(monkeypatch, runner_path)
    calls = []
    provider_module = types.ModuleType("aistock_models.gats_industry_provider")

    def _inject(config, *, cwd=None, print_fn=print):
        calls.append({"config": config, "cwd": Path(cwd)})
        return "provider"

    provider_module.inject_gats_industry_provider_if_needed = _inject
    monkeypatch.setitem(sys.modules, "aistock_models.gats_industry_provider", provider_module)
    monkeypatch.chdir(tmp_path)

    assert runner._task_train_with_gats_industry_provider(
        {"task": {"model": {"kwargs": {"gats_adjacency_mode": "off"}}}},
        "exp-off",
    ) is None
    assert calls == []

    config = {
        "qe_runtime": {"gats_industry_source_path": str(tmp_path / "sector_data.h5")},
        "task": {"model": {"kwargs": {"gats_adjacency_mode": "industry_bias"}}},
    }
    assert runner._task_train_with_gats_industry_provider(
        config,
        "exp-bias",
    ) is None
    assert calls == [{"config": config, "cwd": tmp_path}]

    config_embedding = {
        "task": {"model": {"kwargs": {"gats_adjacency_mode": "off", "gats_industry_embedding": "on"}}},
    }
    assert runner._task_train_with_gats_industry_provider(
        config_embedding,
        "exp-embedding",
    ) is None
    assert calls[-1] == {"config": config_embedding, "cwd": tmp_path}

@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_check_retries_empty_mlflow_metric_once(
    tmp_path, monkeypatch, capsys, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)
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

        def load(self, name: str, parents: bool = True):
            return (name, parents)

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


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_check_keeps_true_missing_artifact_loud(
    tmp_path, monkeypatch, capsys, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)
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

        def load(self, name: str, parents: bool = True):
            return (name, parents)

    record_temp.RecordTemp = MissingArtifactRecordTemp
    runner._install_mlflow_metric_read_retry()
    record = record_temp.RecordTemp(_FakeRecorder(tmp_path / "run"))

    with pytest.raises(FileNotFoundError, match="pred.pkl"):
        record.check()
    assert record.calls == 1
    assert "transient MLflow empty metric read" not in capsys.readouterr().out


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_check_retry_exhaustion_reports_metric_and_path(
    tmp_path, monkeypatch, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)
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

        def load(self, name: str, parents: bool = True):
            return (name, parents)

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


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_load_drains_prior_async_metric_writes_before_artifact_read(
    tmp_path, monkeypatch, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)

    class OrderedAsyncRecorder(_FakeRecorder):
        def __init__(self, run_dir: Path) -> None:
            super().__init__(run_dir)
            self.metric_ready = False
            self.barrier_calls = 0
            self.async_log = self._enqueue

        def _enqueue(self, operation, *args, **kwargs):
            self.barrier_calls += 1
            self.metric_ready = True
            operation(*args, **kwargs)

    class BarrierRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.load_calls = 0

        @property
        def recorder(self):
            return self._recorder

        def load(self, name: str, parents: bool = True):
            self.load_calls += 1
            if not self.recorder.metric_ready:
                raise AssertionError("artifact read started before queued metric writes drained")
            return {"name": name, "parents": parents}

        def check(self, include_self: bool = False, parents: bool = True):
            return True

    record_temp.RecordTemp = BarrierRecordTemp
    runner._install_mlflow_metric_read_retry()
    recorder = OrderedAsyncRecorder(tmp_path / "mlruns" / "0" / "run-1")
    record = record_temp.RecordTemp(recorder)

    assert record.load("pred.pkl") == {"name": "pred.pkl", "parents": True}
    assert recorder.barrier_calls == 1
    assert record.load_calls == 1


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_load_retries_wrapped_load_object_empty_metric(
    tmp_path, monkeypatch, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)
    monkeypatch.setenv(runner.MLFLOW_EMPTY_METRIC_RETRY_SLEEP_SEC_ENV, "0")

    class LoadObjectError(Exception):
        pass

    class FlakyLoadRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.load_calls = 0

        @property
        def recorder(self):
            return self._recorder

        def load(self, name: str, parents: bool = True):
            self.load_calls += 1
            if self.load_calls == 1:
                try:
                    raise ValueError("Metric 'IC' is malformed. No data found.")
                except ValueError as cause:
                    raise LoadObjectError(str(cause)) from cause
            return name

        def check(self, include_self: bool = False, parents: bool = True):
            return True

    record_temp.RecordTemp = FlakyLoadRecordTemp
    runner._install_mlflow_metric_read_retry()
    record = record_temp.RecordTemp(_FakeRecorder(tmp_path / "run"))

    assert record.load("pred.pkl") == "pred.pkl"
    assert record.load_calls == 2


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_load_does_not_retry_unrelated_load_object_error(
    tmp_path, monkeypatch, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)

    class LoadObjectError(Exception):
        pass

    class MissingLoadRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.load_calls = 0

        @property
        def recorder(self):
            return self._recorder

        def load(self, name: str, parents: bool = True):
            self.load_calls += 1
            raise LoadObjectError(f"artifact not found: {name}")

        def check(self, include_self: bool = False, parents: bool = True):
            return True

    record_temp.RecordTemp = MissingLoadRecordTemp
    runner._install_mlflow_metric_read_retry()
    record = record_temp.RecordTemp(_FakeRecorder(tmp_path / "run"))

    with pytest.raises(LoadObjectError, match="artifact not found"):
        record.load("pred.pkl")
    assert record.load_calls == 1


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_load_fails_before_read_when_async_barrier_times_out(
    tmp_path, monkeypatch, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)
    monkeypatch.setenv(runner.MLFLOW_ASYNC_DRAIN_TIMEOUT_SEC_ENV, "0")

    class StalledRecorder(_FakeRecorder):
        def __init__(self, run_dir: Path) -> None:
            super().__init__(run_dir)
            self.async_log = lambda _operation: None

    class NeverReadRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.load_calls = 0

        @property
        def recorder(self):
            return self._recorder

        def load(self, name: str, parents: bool = True):
            self.load_calls += 1
            return name

        def check(self, include_self: bool = False, parents: bool = True):
            return True

    record_temp.RecordTemp = NeverReadRecordTemp
    runner._install_mlflow_metric_read_retry()
    record = record_temp.RecordTemp(StalledRecorder(tmp_path / "run"))

    with pytest.raises(runner.QEMlflowAsyncDrainError, match="barrier timed out"):
        record.load("pred.pkl")
    assert record.load_calls == 0


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_record_load_fails_before_enqueue_when_async_writer_is_dead(
    tmp_path, monkeypatch, runner_path
) -> None:
    runner, record_temp = _load_runner(monkeypatch, runner_path)

    class DeadWorker:
        @staticmethod
        def is_alive() -> bool:
            return False

    class DeadAsyncLog:
        def __init__(self) -> None:
            self._t = DeadWorker()
            self.enqueue_calls = 0

        def __call__(self, _operation) -> None:
            self.enqueue_calls += 1

    class DeadWriterRecorder(_FakeRecorder):
        def __init__(self, run_dir: Path) -> None:
            super().__init__(run_dir)
            self.async_log = DeadAsyncLog()

    class NeverReadRecordTemp:
        def __init__(self, recorder) -> None:
            self._recorder = recorder
            self.load_calls = 0

        @property
        def recorder(self):
            return self._recorder

        def load(self, name: str, parents: bool = True):
            self.load_calls += 1
            return name

        def check(self, include_self: bool = False, parents: bool = True):
            return True

    record_temp.RecordTemp = NeverReadRecordTemp
    runner._install_mlflow_metric_read_retry()
    recorder = DeadWriterRecorder(tmp_path / "run")
    record = record_temp.RecordTemp(recorder)

    with pytest.raises(runner.QEMlflowAsyncDrainError, match="not alive"):
        record.load("pred.pkl")
    assert recorder.async_log.enqueue_calls == 0
    assert record.load_calls == 0


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_installs_mlflow_retry_after_init_before_task_train(runner_path: Path) -> None:
    source = runner_path.read_text(encoding="utf-8")
    run_main = source[source.index("def _run_main") :]

    init_index = run_main.index("qlib.init(")
    install_index = run_main.index("_install_mlflow_metric_read_retry()")
    train_index = run_main.index("recorder = _task_train_with_gats_industry_provider(")
    assert init_index < install_index < train_index


@pytest.mark.parametrize(
    ("runner_path", "freq"),
    [(RUNNER_PATH, "1min"), (DAY_RUNNER_PATH, "day")],
)
def test_qrun_passes_four_kernel_limit_through_qlib_init(
    monkeypatch: pytest.MonkeyPatch,
    runner_path: Path,
    freq: str,
) -> None:
    runner, _record_temp = _load_runner(monkeypatch, runner_path)
    config = {
        "qlib_init": {
            "provider_uri": {"day": "/data/day", "1min": "/data/minute"},
            "kernels": 99,
        }
    }
    exp_manager = {"kwargs": {"uri": "file:/tmp/mlruns"}}

    qlib_init_config = runner._qlib_init_config_with_kernel_limit(config)
    runner.qlib.init(**qlib_init_config, exp_manager=exp_manager)
    runner._verify_qlib_kernel_limit(freq)

    assert len(runner.qlib.init_calls) == 1
    assert runner.qlib.init_calls[0]["kernels"] == 4
    assert runner.qlib.init_calls[0]["provider_uri"] == config["qlib_init"]["provider_uri"]
    assert runner.qlib.init_calls[0]["exp_manager"] is exp_manager
    assert runner.C.get_kernels(freq) == 4
    assert config["qlib_init"]["kernels"] == 99


@pytest.mark.parametrize(
    ("runner_path", "freq"),
    [(RUNNER_PATH, "1min"), (DAY_RUNNER_PATH, "day")],
)
def test_qrun_fails_closed_when_effective_kernel_limit_does_not_stick(
    monkeypatch: pytest.MonkeyPatch,
    runner_path: Path,
    freq: str,
) -> None:
    runner, _record_temp = _load_runner(monkeypatch, runner_path)

    def ignore_requested_kernels(**_kwargs) -> None:
        runner.C["kernels"] = 26

    monkeypatch.setattr(runner.qlib, "init", ignore_requested_kernels)
    qlib_init_config = runner._qlib_init_config_with_kernel_limit(
        {"qlib_init": {"provider_uri": "/data/day"}}
    )
    runner.qlib.init(**qlib_init_config, exp_manager={"kwargs": {}})

    with pytest.raises(RuntimeError, match="QE_QLIB_KERNEL_LIMIT_NOT_EFFECTIVE"):
        runner._verify_qlib_kernel_limit(freq)

    assert runner.C.get_kernels(freq) == 26


@pytest.mark.parametrize("runner_path", [RUNNER_PATH, DAY_RUNNER_PATH])
def test_qrun_verifies_kernel_limit_before_provider_read(runner_path: Path) -> None:
    source = runner_path.read_text(encoding="utf-8")
    run_main = source[source.index("def _run_main") :]

    build_at = run_main.index("_qlib_init_config_with_kernel_limit(config)")
    init_at = run_main.index("qlib.init(**qlib_init_config")
    verify_at = run_main.index("_verify_qlib_kernel_limit(")
    provider_read_at = run_main.index("load_benchmark_series(config)")

    assert build_at < init_at < verify_at < provider_read_at
    assert 'C["kernels"] = 4' not in run_main
