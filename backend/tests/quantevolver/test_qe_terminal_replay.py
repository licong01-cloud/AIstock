"""Terminal replay preserves account economics and rejects false physical windows."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

from backend.services.quantevolver.terminal_replay_consumer import (
    apply_terminal_replay, extract_terminal_replay, require_terminal_replay_mode,
)
from scripts.qe_terminal_replay_preflight import normalize_spec, validate

ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def fixture(tmp_path):
    binding = {"cutoff": "2026-09-30", "provider_uri_day": str(tmp_path / "day"),
               "provider_uri_1min": str(tmp_path / "minute"),
               "selection_pins": {"instrument_name": "frozen_pit"}}
    for freq, pin, text in [("day", "day_pins", "2026-09-28\n2026-09-29\n2026-09-30\n"),
                             ("1min", "minute_pins", "2026-09-30 14:59:00\n2026-09-30 15:00:00\n")]:
        root = Path(binding["provider_uri_day" if freq == "day" else "provider_uri_1min"])
        (root / "calendars").mkdir(parents=True)
        raw = text.encode()
        (root / "calendars" / (freq + ".txt")).write_bytes(raw)
        binding[pin] = {"calendar_sha256": hashlib.sha256(raw).hexdigest()}
    prediction = tmp_path / "prediction.pkl"
    pd.DataFrame({"score": [.3]}, index=pd.MultiIndex.from_tuples(
        [(pd.Timestamp("2026-09-29"), "SH600000")], names=["datetime", "instrument"])).to_pickle(prediction)
    spec = {"schema_version": "qe_terminal_replay_v1", "start": "2026-09-29", "terminal_date": "2026-09-30",
            "expected_sessions": 2, "dataset_manifest_sha256": "a" * 64,
            "prediction_sha256": hashlib.sha256(prediction.read_bytes()).hexdigest()}
    conf = {"market": "frozen_pit", "qlib_init": {"provider_uri": {
        "day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}},
        "port_analysis_config": {
            "backtest": {"start_time": spec["start"], "end_time": spec["terminal_date"], "account": 10_000_000,
                         "exchange_kwargs": {"freq": "1min", "open_cost": .0003, "close_cost": .0013}},
            "strategy": {"class": "OriginalAF", "kwargs": {"topk": 20, "max_weight": .05, "max_n_drop": 5}},
            "executor": {"class": "NestedExecutor", "module_path": "qlib.backtest.executor", "kwargs": {
                "inner_strategy": {"class": "TWAP", "kwargs": {"tail_boost": True}},
                "inner_executor": {"class": "SimulatorExecutor", "module_path": "qlib.backtest.executor", "kwargs": {"time_per_step": "1min"}}}}}}
    result = {"experiment_files": {"conf.yaml": yaml.safe_dump(conf)}, "direct_v2_dataset_binding": binding,
              "wsl_command": "python qrun_limit_minute.py conf.yaml", "wsl_command_core": "python qrun_limit_minute.py conf.yaml"}
    return SimpleNamespace(spec=spec, binding=binding, conf=conf, result=result, prediction=prediction)


def test_disabled_is_identity_and_mode_requires_exact_prediction(fixture):
    assert apply_terminal_replay(fixture.result, None, scripts_dir=ROOT / "scripts") is fixture.result
    assert extract_terminal_replay({"other": 3}) == ({"other": 3}, None)
    require_terminal_replay_mode(None, mode="full_train", source=None)
    for mode, source, match in [("full_train", None, "MODE"), ("backtest_only", None, "MODE"),
                                ("prediction_replay", SimpleNamespace(sha256="b" * 64), "SOURCE_IDENTITY")]:
        with pytest.raises(ValueError, match=match):
            require_terminal_replay_mode(fixture.spec, mode=mode, source=source)
    require_terminal_replay_mode(fixture.spec, mode="prediction_replay", source=SimpleNamespace(sha256=fixture.spec["prediction_sha256"]))


def test_source_identity_conflict_and_invalid_spec(fixture):
    params = {"qe_terminal_replay": fixture.spec, "_qe_active_dataset_summary": {"dataset_manifest_sha256": "a" * 64}}
    remaining, spec = extract_terminal_replay(params)
    assert "qe_terminal_replay" not in remaining and spec == fixture.spec
    with pytest.raises(ValueError, match="ACTIVE_IDENTITY"):
        extract_terminal_replay({"qe_terminal_replay": fixture.spec})
    for key in ("qe_l2_relative_manifest", "qe_l2_continuous_manifest", "qe_l2_study_manifest"):
        with pytest.raises(ValueError, match="HMM_CONFLICT"):
            extract_terminal_replay({**params, key: {"enabled": True}})
    for changes in ({"expected_sessions": True}, {"expected_sessions": 0}, {"start": "2026-02-31"},
                    {"terminal_date": "2026-10-30"}, {"prediction_sha256": "unknown"}, {"extra": True}):
        with pytest.raises(ValueError, match="QE_TERMINAL_REPLAY"):
            normalize_spec({**fixture.spec, **changes})


def test_only_executor_classes_change_and_real_assets_read(fixture):
    before = deepcopy(fixture.conf)
    result = apply_terminal_replay(fixture.result, fixture.spec, scripts_dir=ROOT / "scripts")
    files = result["experiment_files"]
    actual = yaml.safe_load(files["conf.yaml"])
    expected = deepcopy(before)
    executor = expected["port_analysis_config"]["executor"]
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    executor["kwargs"]["inner_executor"].update({"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"})
    assert actual == expected
    assert result["wsl_command"].count("python qrun_limit_minute.py") == 1
    assert "--prediction frozen_prediction.pkl && python qrun_limit_minute.py" in result["wsl_command"]
    assert "hmm" not in files["qe_terminal_replay_contract.json"]
    envelope = json.loads(files["qe_terminal_replay_contract.json"])
    proof = validate(envelope, actual, fixture.prediction)
    assert proof["days"] == 2 and proof["last_signal_date"] == "2026-09-29"
    assert proof["last_minute"] == "2026-09-30 15:00:00"


def test_frozen_neural_recipe_template_is_preserved_without_rendering(fixture):
    template = 'task:\n  model:\n    kwargs:\n      d_feat: {{ num_features }}\n      step_len: {{ num_timesteps }}\n'
    fixture.result["experiment_files"]["conf.yaml"] = fixture.result["experiment_files"]["conf.yaml"].replace(
        "port_analysis_config:\n", "port_analysis_config: &port_analysis_config\n")
    fixture.result["experiment_files"]["conf.yaml"] += template + 'record:\n  config: *port_analysis_config\n'
    result = apply_terminal_replay(fixture.result, fixture.spec, scripts_dir=ROOT / "scripts")
    assert template in result["experiment_files"]["conf.yaml"]
    from scripts.qe_terminal_replay_preflight import load_config
    parsed = load_config(result["experiment_files"]["conf.yaml"])
    assert parsed["record"]["config"] == parsed["port_analysis_config"]
    proof = validate(json.loads(result["experiment_files"]["qe_terminal_replay_contract.json"]),
                     load_config(result["experiment_files"]["conf.yaml"]), fixture.prediction)
    assert proof["days"] == 2


@pytest.mark.parametrize("fault", ["calendar_hash", "terminal_bar", "count", "prediction_hash", "window", "provider", "frequency"])
def test_worker_never_accepts_missing_or_altered_physical_inputs(fixture, fault):
    result = apply_terminal_replay(fixture.result, fixture.spec, scripts_dir=ROOT / "scripts")
    envelope = json.loads(result["experiment_files"]["qe_terminal_replay_contract.json"])
    conf = yaml.safe_load(result["experiment_files"]["conf.yaml"])
    if fault in ("calendar_hash", "terminal_bar"):
        path = Path(fixture.binding["provider_uri_1min"]) / "calendars/1min.txt"
        path.write_text("2026-09-30 14:58:00\n2026-09-30 14:59:00\n", encoding="utf-8")
        if fault == "terminal_bar":
            envelope["dataset_binding"]["minute_pins"]["calendar_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    elif fault == "count":
        envelope["spec"]["expected_sessions"] = 1
    elif fault == "prediction_hash":
        fixture.prediction.write_bytes(b"not a pickle; must fail before deserialization")
    elif fault == "window":
        conf["port_analysis_config"]["backtest"]["end_time"] = "2026-09-29"
    elif fault == "provider":
        conf["qlib_init"]["provider_uri"]["day"] = "other_dataset"
    else:
        conf["port_analysis_config"]["backtest"]["exchange_kwargs"]["freq"] = "day"
    with pytest.raises(ValueError, match="QE_TERMINAL_REPLAY"):
        validate(envelope, conf, fixture.prediction)
