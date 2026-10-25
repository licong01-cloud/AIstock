"""Focused compose and reader failures; real full-asset validation runs on node1."""
from copy import deepcopy
import hashlib
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "scripts"))
from qe_l2_risk_consumer import checked_asset, L2ContractError
from backend.services.quantevolver.l2_risk_consumer import extract_study, apply_study, bind_execution_metadata


def study_fixture():
    from backend.services.quantevolver.l2_risk_consumer import APPROVED_RISK_SHA256
    import yaml

    binding = {"provider_uri_day": "/frozen/day", "provider_uri_1min": "/frozen/minute",
               "selection_pins": {"instrument_name": "stock_universe", "instruments_sha256": "pit",
                                  "coverage_receipt_sha256": "window-specific"}}
    manifest = {"contract": "QE-L2-RISK-ENTRY-GATE-D1..D7", "policy": "qe_l2_risk_entry_gate_v1",
                "threshold": .20, "start": "2026-04-01", "end": "2026-09-30", "decision_days": 125,
                "dataset_binding": binding, "assets": {k: {} for k in
                  ("risk", "mapping", "mapping_receipt", "authority", "prediction", "calendar", "pit")}}
    manifest["assets"]["risk"]["sha256"] = APPROVED_RISK_SHA256
    conf = {"qlib_init": {"provider_uri": {"day": "/frozen/day", "1min": "/frozen/minute"}},
            "market": "stock_universe", "task": {"dataset": {"kwargs": {"segments": {"test": ["2026-03-31", "2026-09-30"]}}}},
            "port_analysis_config": {
                "strategy": {"class": "SuspendFilterScoreWeightedTopkStrategyV2", "module_path": "qe_suspend_filter_score_weighted_strategy", "kwargs": {}},
                "backtest": {"start_time": "2026-04-01", "end_time": "2026-09-30", "account": 100_000_000, "exchange_kwargs": {"freq": "1min"}},
                "executor": {"class": "NestedExecutor", "kwargs": {"inner_executor": {"class": "SimulatorExecutor"}}}}}
    result = {"direct_v2_dataset_binding": deepcopy(binding), "experiment_files": {"conf.yaml": yaml.safe_dump(conf)},
              "wsl_command": "python qrun_limit_minute.py conf.yaml"}
    return manifest, conf, result


def test_compose_shared_binding_and_only_explicit_overlay():
    import yaml

    manifest, _, result = study_fixture()
    result["direct_v2_dataset_binding"]["selection_pins"]["coverage_receipt_sha256"] = "another-window"
    outputs = []
    for enabled in (False, True):
        _, study = extract_study({"qe_l2_risk_enabled": enabled, "qe_l2_study_manifest": manifest})
        output = apply_study(deepcopy(result), study, scripts_dir=Path(__file__).resolve().parents[3] / "scripts")
        outputs.append(yaml.safe_load(output["experiment_files"]["conf.yaml"]))
        assert "--config conf.yaml --prediction frozen_prediction.pkl" in output["wsl_command"]
        assert output["wsl_command"].index("qe_l2_risk_consumer.py") < output["wsl_command"].index("qrun_limit_minute.py")
    left, right = [c["port_analysis_config"] for c in outputs]
    assert left["backtest"] == right["backtest"]
    assert left["executor"] == right["executor"]
    assert left["strategy"]["class"] == "SuspendFilterScoreWeightedTopkStrategyV2"
    assert right["strategy"]["class"] == "L2RiskEntryGateScoreWeightedTopkStrategyV2"


@pytest.mark.parametrize("change", ["provider", "pit", "missing"])
def test_compose_rejects_actual_binding_drift(change):
    manifest, _, result = study_fixture()
    if change == "provider":
        result["direct_v2_dataset_binding"]["provider_uri_day"] = "/current/active"
    elif change == "pit":
        result["direct_v2_dataset_binding"]["selection_pins"]["instruments_sha256"] = "changed"
    else:
        result.pop("direct_v2_dataset_binding")
    _, study = extract_study({"qe_l2_study_manifest": manifest})
    with pytest.raises(ValueError, match="EXECUTION_BINDING"):
        apply_study(result, study, scripts_dir="unused")


def test_unapproved_risk_asset_fails_before_compose():
    manifest, _, _ = study_fixture()
    manifest["assets"]["risk"]["sha256"] = "0" * 64
    with pytest.raises(ValueError, match="UNAPPROVED_RISK_ASSET"):
        extract_study({"qe_l2_study_manifest": manifest})


def test_actual_replay_file_and_provider_are_verified(tmp_path):
    import yaml
    from qe_l2_risk_consumer import validate_execution_inputs

    manifest, conf, _ = study_fixture()
    pred = tmp_path / "prediction.pkl"
    pred.write_bytes(b"approved frozen AF")
    manifest["assets"]["prediction"]["sha256"] = hashlib.sha256(pred.read_bytes()).hexdigest()
    config = tmp_path / "conf.yaml"
    config.write_text(yaml.safe_dump(conf), encoding="utf-8")
    validate_execution_inputs(manifest, config, pred)
    pred.write_bytes(b"different AF prediction")
    with pytest.raises(L2ContractError, match="ASSET_HASH_MISMATCH"):
        validate_execution_inputs(manifest, config, pred)
    conf["qlib_init"]["provider_uri"]["1min"] = "/another/minute"
    config.write_text(yaml.safe_dump(conf), encoding="utf-8")
    with pytest.raises(L2ContractError, match="EXECUTION_PROVIDER_MISMATCH"):
        validate_execution_inputs(manifest, config, pred)


def test_manifest_binding_must_agree_with_release_before_io():
    from qe_l2_risk_consumer import validate_frozen_dataset

    manifest, _, _ = study_fixture()
    manifest["dataset_binding"]["release_id"] = "different"
    manifest["release_identity"] = {"release_id": "frozen"}
    with pytest.raises(L2ContractError, match="DATASET_BINDING_MISMATCH"):
        validate_frozen_dataset(manifest)


def test_default_off_is_identity():
    result = {"experiment_files": {"conf.yaml": "unchanged"}, "wsl_command": "original"}
    original = deepcopy(result)
    params, study = extract_study({"other": 7})
    assert params == {"other": 7} and study is None
    assert apply_study(result, study, scripts_dir="absent") is result
    assert result == original


@pytest.mark.parametrize("params,code", [
    ({"qe_l2_risk_enabled": True}, "MANIFEST_REQUIRED"),
    ({"qe_l2_risk_enabled": "false"}, "ENABLED_INVALID"),
    ({"qe_l2_study_manifest": {}}, "CONTRACT_INVALID"),
    ({"qe_l2_study_manifest": {"contract": "QE-L2-RISK-ENTRY-GATE-D1..D7"}, "enable_sector_hmm": True}, "OTHER_OVERLAY_FORBIDDEN"),
])
def test_invalid_opt_in_fails(params, code):
    with pytest.raises(ValueError, match=code):
        extract_study(params)


def test_hash_mismatch_stops_before_reading(tmp_path):
    path = tmp_path / "asset"
    path.write_bytes(b"tampered")
    with pytest.raises(L2ContractError, match="HASH_MISMATCH"):
        checked_asset({"path": str(path), "sha256": "0" * 64})


def test_registration_preserves_actual_minute_contract_for_archive():
    from backend.services.qe_archive.source_assembler import QEArchiveSourceAssembler

    conf = {"port_analysis_config": {
        "backtest": {"exchange_kwargs": {"freq": "1min", "limit_threshold": ["$limit_up", "$limit_down"]}},
        "executor": {"kwargs": {"inner_strategy": {"class": "TailTWAPWithLimitStrategy",
            "module_path": "tail_twap_strategy", "kwargs": {"unfilled_handler": "TAIL_BOOST",
            "unfilled_trigger_minute": 235, "unfilled_backup_depth": 15}}}},
        "strategy": {"kwargs": {"suspend_filter_strict": True}}}}
    original = {"qe_l2_risk_enabled": True, "random_seed": 42}
    params = bind_execution_metadata(original, conf)
    payload = QEArchiveSourceAssembler.build_experiment_payload({"experiment_id": "l2-test",
        "status": "completed", "custom_params": params})
    assert payload["freq"] == "1min" and payload["limit_suspend_authoritative"] is True
    assert params["execution_algo"] == "TWAP" and params["execution_algo_params"]["unfilled_trigger_minute"] == 235
    assert original == {"qe_l2_risk_enabled": True, "random_seed": 42}
    conf["port_analysis_config"]["backtest"]["exchange_kwargs"]["freq"] = "day"
    with pytest.raises(ValueError, match="REGISTRATION_EVIDENCE_INVALID"):
        bind_execution_metadata(original, conf)


def test_minute_runner_keeps_guards_for_exact_terminal_wrapper():
    import ast

    source = (Path(__file__).resolve().parents[3] / "scripts/qrun_limit_minute.py").read_text(encoding="utf-8-sig")
    function = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == "_is_minute_nested_backtest")
    namespace = {}
    exec(compile(ast.Module(body=[function], type_ignores=[]), "minute_guard", "exec"), namespace)
    check = namespace["_is_minute_nested_backtest"]
    conf = {"port_analysis_config": {"executor": {"class": "NestedExecutor"},
            "backtest": {"exchange_kwargs": {"freq": "1min"}}}}
    assert check(conf)
    executor = conf["port_analysis_config"]["executor"]
    executor["class"] = "BoundedTerminalNestedExecutor"
    assert not check(conf)
    executor["module_path"] = "qe_terminal_interval_executor"
    assert check(conf)
    conf["port_analysis_config"]["backtest"]["exchange_kwargs"]["freq"] = "day"
    assert not check(conf)
