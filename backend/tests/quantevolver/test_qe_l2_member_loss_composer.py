"""Formal reader and Composer isolation with explicit synthetic admitted assets."""
# ruff: noqa: E402 -- the worker uses the scripts import root.
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys

import pandas as pd
import pytest
import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
import qe_l2_member_loss_admission as admission
import qe_l2_member_loss_consumer as consumer
from qe_l2_continuous_consumer import prediction_panel_sha256
from backend.services.quantevolver.l2_member_loss_consumer import apply_member_loss, extract_member_loss, OTHER_OVERLAYS
from backend.tests.quantevolver.test_qe_l2_member_loss_consumer import sample as sample  # noqa: F401


def write(root, name, value):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = value if isinstance(value, bytes) else json.dumps(value, allow_nan=False).encode()
    path.write_bytes(raw)
    return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}


@pytest.fixture
def formal(sample, tmp_path, monkeypatch):  # noqa: F811
    prediction, risk_binding, signal_calendar = deepcopy(sample)
    prefix = [str(d.date()) for d in pd.bdate_range("2026-01-05", "2026-03-30")]
    calendar = prefix + signal_calendar
    calendar_ref = write(tmp_path, "day/calendars/day.txt", ("\n".join(calendar) + "\n").encode())
    minute = write(tmp_path, "minute/calendars/1min.txt", b"2026-10-29 15:00:00\n")
    day_meta, minute_meta = write(tmp_path, "day/meta_export.json", {}), write(tmp_path, "minute/meta_export.json", {})
    dataset = write(tmp_path, "qe_dataset_manifest.json", {"EXPLICIT_SYNTHETIC_FIXTURE": True})
    pit = write(tmp_path, "day/instruments/stock_universe.txt", b"000001.SZ\t2026-01-05\t2026-10-30\n")
    authority = {"identity": {"bundle_hash": "fixture"},
                 "research_basis": {"active_mode": "historical_replay", "historical_classification_basis": "stable_taxonomy_backcast", "canonical_hash": "basis"},
                 "l2_projection": {"rows": [{"canonical_l2_code": s} for s in risk_binding["industry_catalog"]], "canonical_hash": "catalog"}}
    authority_ref = write(tmp_path, "authority.json", authority)
    risk_binding["calendar_sha256"] = calendar_ref["sha256"]
    # Signal-window dates remain the same after adding pre-April AF reference context.
    loss = write(tmp_path, "member_loss.json", prediction)
    risk_binding["prediction_sha256"] = loss["sha256"]
    frame = pd.DataFrame({"score": [0.01] * len(calendar)}, index=pd.MultiIndex.from_arrays(
        [pd.to_datetime(calendar), ["000001.SZ"] * len(calendar)], names=["datetime", "instrument"]))
    pred_path = tmp_path / "AF.pkl"
    frame.to_pickle(pred_path)
    pred_ref = {"path": str(pred_path), "sha256": hashlib.sha256(pred_path.read_bytes()).hexdigest()}
    projected = frame.loc[pd.Timestamp("2026-03-31"):]
    projection = {"schema_version": "qe_l2_member_AF_projection_v1", "source_prediction_sha256": pred_ref["sha256"],
                  "source_start": "2026-01-05", "source_end": "2026-10-30", "start": "2026-03-31", "end": "2026-10-30",
                  "panel_sha256": prediction_panel_sha256(projected)}
    af = {"schema_version": "qe_frozen_inference_result_v1", "status": "verified", "fit_calls": 0,
          "reference_equivalent": True, "source_bytes_unchanged": True, "reference_max_abs_error": 0,
          "spec": {"input_manifest_sha256": admission.TARGET_MANIFEST}, "processor_state_before": "same", "processor_state_after": "same",
          "model_weights_before": "same", "model_weights_after": "same", "executable_prediction_panel_sha256": prediction_panel_sha256(frame)}
    af_ref = write(tmp_path, "AF_receipt.json", af)
    mm = {"non_as_known_taxonomy": True, "active_classification_basis": "stable_taxonomy_backcast", "candidate_bundle_hash": "fixture",
          "research_basis_contract_sha256": "basis", "l2_code_projection_sha256": "catalog", "classification_authority_receipt_hash": "classification"}
    rows = []
    for i, stamp in enumerate(projected.index.get_level_values("datetime")):
        day = str(stamp.date())
        trade = signal_calendar[i + 1] if i + 1 < len(signal_calendar) else None
        rows.append({"as_of_date": day, "trade_date": trade, "instrument": "000001.SZ", "l2_code": risk_binding["industry_catalog"][0],
                     "mapping_status": "resolved", "mapping_reason": None, "non_as_known_taxonomy": True,
                     "classification_receipt_hash": "classification", "used_in_142_signal_days": trade is not None,
                     "used_in_141_economic_days": trade is not None and trade <= "2026-10-29"})
    map_path = tmp_path / "mapping.parquet"
    pd.DataFrame(rows).to_parquet(map_path, index=False)
    map_ref = {"path": str(map_path), "sha256": hashlib.sha256(map_path.read_bytes()).hexdigest()}
    map_receipt = write(tmp_path, "mapping_receipt.json", {"mapping_manifest": mm, "mapping_sha256": map_ref["sha256"],
                        "authority_spec_sha256": authority_ref["sha256"], "membership_file_sha256": pit["sha256"]})
    binding = {"candidate_root": str(tmp_path), "release_id": consumer.RELEASE, "cutoff": consumer.SIGNAL_END,
               "provider_uri_day": str(tmp_path / "day"), "provider_uri_1min": str(tmp_path / "minute"),
               "day_pins": {"universe_key": "fixture", "rule_version": "fixture", "calendar_sha256": calendar_ref["sha256"], "meta_export_sha256": day_meta["sha256"]},
               "minute_pins": {"universe_key": "fixture", "rule_version": "fixture", "calendar_sha256": minute["sha256"], "meta_export_sha256": minute_meta["sha256"]},
               "selection_pins": {"instrument_name": "stock_universe", "instruments_sha256": pit["sha256"]}}
    release = {"dataset_manifest_sha256": admission.TARGET_MANIFEST, "release_id": consumer.RELEASE, "cutoff": consumer.SIGNAL_END,
               "universe_key": "fixture", "rule_version": "fixture", "metadata_sha256": {"calendar": calendar_ref["sha256"],
               "selection_universe": pit["sha256"], "dataset_manifest": dataset["sha256"]}}
    worker = write(tmp_path, "worker.py", b"fixture_worker = True\n")
    code = {"worker.py": worker["sha256"]}
    execution = {"EXPLICIT_SYNTHETIC_EXECUTION": True}
    m = {"contract": admission.CONTRACT, "schema_version": admission.VERSION, "arm": "C", "seed": 42,
         "asset_admission_version": "explicit-test-only", "start": consumer.START, "end": consumer.ECONOMIC_END,
         "signal_end": consumer.SIGNAL_END, "decision_days": 141, "signal_days": 142, "kappa": 0.10, "max_weight": 0.05,
         "base_source_sha256": admission.BASE_SOURCE_SHA, "base_contract_sha256": "a" * 64,
         "dataset_binding": binding, "release_identity": release, "risk_binding": risk_binding,
         "authority_canonical_sha256": consumer.canonical_sha(authority), "prediction_projection": projection,
         "assets": {"loss": loss, "prediction": pred_ref, "AF_receipt": af_ref, "mapping": map_ref,
                    "mapping_receipt": map_receipt, "authority": authority_ref, "calendar": calendar_ref, "pit": pit},
         "execution_files_sha256": code, "execution_contract": execution,
         "execution_sha256": consumer.canonical_sha({"files": code, "contract": execution})}
    record = {**admission.identity(m), "AF_by_seed": {"42": {"prediction_sha256": pred_ref["sha256"],
              "receipt_sha256": af_ref["sha256"], "projection": projection}}}
    from scripts import qe_l2_member_loss_admission as package_admission
    for module in (admission, package_admission):
        monkeypatch.setattr(module, "ADMISSIONS", {"explicit-test-only": record})
    manifest = tmp_path / "manifest.json"
    write(tmp_path, manifest.name, m)
    monkeypatch.chdir(tmp_path)
    return SimpleNamespace(root=tmp_path, manifest=m, path=manifest, record=record, calendar=calendar)


def test_formal_reader_keeps_full_grid_and_b0_does_not_open_loss(formal):
    inputs = consumer.MemberEntryInputs(formal.path)
    assert len(inputs.risk) == 18602 and len(inputs.execution_as_of) == 141
    assert inputs.execution_as_of["2026-10-29"] == "2026-10-28"
    assert len(inputs.risk_context("2026-09-30", ["000001.SZ"])["sector_rows"]) == 131
    with pytest.raises(ValueError, match="EXECUTION_DAY"):
        inputs.risk_context("2026-10-30", [])
    formal.manifest["arm"] = "B0"
    formal.manifest["assets"]["loss"]["path"] = str(formal.root / "must_not_open")
    write(formal.root, formal.path.name, formal.manifest)
    control = consumer.MemberEntryInputs(formal.path)
    assert control.risk == {} and control.risk_context("2026-10-29", ["unknown"]) is None


def readmit_fixture(formal):
    """Change a synthetic admitted byte pin to reach semantic rejection checks."""
    m = formal.manifest
    formal.record.update(admission.identity(m))
    formal.record["AF_by_seed"]["42"] = {
        "prediction_sha256": m["assets"]["prediction"]["sha256"],
        "receipt_sha256": m["assets"]["AF_receipt"]["sha256"], "projection": m["prediction_projection"],
    }
    write(formal.root, formal.path.name, m)


def test_native_series_AF_is_read_without_rewriting_admitted_bytes(formal):
    ref = formal.manifest["assets"]["prediction"]
    path = Path(ref["path"])
    pd.read_pickle(path).score.to_pickle(path)
    ref["sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    formal.manifest["prediction_projection"]["source_prediction_sha256"] = ref["sha256"]
    readmit_fixture(formal)
    before = path.read_bytes()
    assert consumer.MemberEntryInputs(formal.path).receipt["economic_days"] == 141
    assert path.read_bytes() == before


@pytest.mark.parametrize("change", [
    {"reference_equivalent": False}, {"reference_max_abs_error": 1e-12},
    {"fit_calls": 1}, {"processor_state_after": "drift"}, {"model_weights_after": "drift"},
])
def test_non_equivalent_or_changed_frozen_AF_is_rejected_even_when_readmitted(formal, change):
    ref = formal.manifest["assets"]["AF_receipt"]
    value = json.loads(Path(ref["path"]).read_bytes())
    formal.manifest["assets"]["AF_receipt"] = write(formal.root, "AF_receipt.json", {**value, **change})
    readmit_fixture(formal)
    with pytest.raises(ValueError, match="AF_FROZEN_RECEIPT"):
        consumer.MemberEntryInputs(formal.path)


@pytest.mark.parametrize("change,reason", [
    ({"asset_admission_version": "caller-allowlist"}, "ADMISSION_UNKNOWN"),
    ({"seed": 2024}, "AF_ADMISSION"), ({"end": "2026-10-28"}, "CONTRACT"),
    ({"kappa": 0.20}, "CONTRACT"), ({"target_source": {}}, "CONTRACT"),
    ({"base_source_sha256": "0" * 64}, "CONTRACT"),
])
def test_requests_cannot_self_admit_or_change_contract(formal, change, reason):
    value = {**formal.manifest, **change, "caller_allowlist": formal.record}
    with pytest.raises(ValueError, match=reason):
        admission.validate_admission(value)


def test_formal_reader_checks_execution_and_mapping_bytes(formal):
    (formal.root / "worker.py").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="HASH_MISMATCH"):
        consumer.MemberEntryInputs(formal.path)


def test_default_off_and_overlay_mutual_exclusion(formal):
    original = {"sentinel": object()}
    assert apply_member_loss(original, None, scripts_dir=ROOT / "scripts") is original
    assert extract_member_loss({"ordinary": 1}) == ({"ordinary": 1}, None)
    for key in OTHER_OVERLAYS:
        with pytest.raises(ValueError, match="OTHER_OVERLAY"):
            extract_member_loss({"qe_l2_member_loss_manifest": formal.manifest, key: True})


def original_port():
    # Reviewed C1 settings: the production Composer checks the independent SHA.
    return {
        "executor": {"class": "NestedExecutor", "module_path": "qlib.backtest.executor", "kwargs": {
            "time_per_step": "day", "inner_executor": {"class": "SimulatorExecutor", "module_path": "qlib.backtest.executor",
            "kwargs": {"time_per_step": "1min", "generate_portfolio_metrics": False}},
            "inner_strategy": {"class": "TailTWAPWithLimitStrategy", "module_path": "tail_twap_strategy",
            "kwargs": {"unfilled_handler": "TAIL_BOOST", "unfilled_trigger_minute": 235, "unfilled_backup_depth": 15}},
            "generate_portfolio_metrics": True}},
        "strategy": {"class": "SuspendFilterScoreWeightedTopkStrategyV2", "module_path": "qe_suspend_filter_score_weighted_strategy",
            "kwargs": {"signal": "<PRED>", "topk": 50, "n_drop": 1, "method_buy": "top", "hold_thresh": 1, "method_sell": "bottom",
            "risk_degree": 0.95, "only_tradable": True, "forbid_all_trade_at_limit": False, "max_n_drop": 5, "max_weight": 0.05,
            "min_n_drop": 0, "min_weight": 0.005, "temperature": 1, "weight_method": "softmax", "min_improvement": 0.01,
            "threshold_floor": 0.005, "threshold_method": "adaptive", "max_position_ratio": 0.95, "adaptive_multiplier": 0.5,
            "score_clip_quantile": 0, "enable_dynamic_ndrop": True, "filter_suspended_on_signal": True,
            "suspend_filter_file": "qe_suspend_filter.json", "suspend_filter_strict": True,
            "risk_policy_enabled": True, "risk_policy_file": "qe_event_risk_policy.json", "risk_policy_strict": True}},
        "backtest": {"start_time": "2026-04-01", "end_time": "2026-10-29", "account": 100000000, "benchmark": None,
            "exchange_kwargs": {"freq": "1min", "limit_threshold": ["$limit_up", "$limit_down"], "deal_price": "close",
            "open_cost": 0.000095, "close_cost": 0.000595, "min_cost": 5, "trade_unit": 100, "codes": "all"}},
    }


def test_real_source_hook_and_composer_preserve_terminal_and_fee_contract(formal):
    m = formal.manifest
    conf = {"qlib_init": {"provider_uri": {"day": m["dataset_binding"]["provider_uri_day"], "1min": m["dataset_binding"]["provider_uri_1min"]}},
            "market": "stock_universe", "port_analysis_config": original_port(),
            "task": {"dataset": {"kwargs": {"segments": {"test": ["2026-03-31", "2026-10-30"]}}}}}
    result = {"direct_v2_dataset_binding": deepcopy(m["dataset_binding"]), "experiment_files": {
        "conf.yaml": yaml.safe_dump(conf), "custom_strategy.py": (ROOT / "scripts/score_weighted_strategy_v2_deterministic_v1.py").read_text(encoding="utf-8")},
        "wsl_command": "python qrun_limit_minute.py conf.yaml"}
    before = deepcopy(result)
    composed = apply_member_loss(result, m, scripts_dir=ROOT / "scripts")
    final = yaml.safe_load(composed["experiment_files"]["conf.yaml"])
    assert final["port_analysis_config"]["backtest"] == conf["port_analysis_config"]["backtest"]
    assert final["port_analysis_config"]["executor"]["class"] == "BoundedTerminalNestedExecutor"
    assert final["port_analysis_config"]["strategy"]["kwargs"]["topk"] == 50
    assert "--formal-manifest qe_l2_member_loss_manifest.json" in composed["wsl_command"]
    bad = deepcopy(before)
    changed = yaml.safe_load(bad["experiment_files"]["conf.yaml"])
    changed["port_analysis_config"]["backtest"]["exchange_kwargs"]["close_cost"] = 0
    bad["experiment_files"]["conf.yaml"] = yaml.safe_dump(changed)
    with pytest.raises(ValueError, match="BASE_EXECUTION_DRIFT"):
        apply_member_loss(bad, m, scripts_dir=ROOT / "scripts")


def test_executor_refuses_training_or_substituted_AF_before_composer():
    import asyncio
    from backend.tests.unified_engine.test_qe_prediction_replay import _config, _context, _executor, _source
    from backend.services.quantevolver.executors.backtest import BacktestMode

    cfg = _config()
    cfg.model_params_base = {"qe_l2_member_loss_manifest": {"assets": {"prediction": {"sha256": "wrong"}}}}
    for mode, reason in [(BacktestMode.FULL_TRAIN, "REQUIRES_PREDICTION_REPLAY"),
                         (BacktestMode.PREDICTION_REPLAY, "SOURCE_PREDICTION_MISMATCH")]:
        executor, coordinator = _executor()
        with pytest.raises(ValueError, match=reason):
            asyncio.run(executor.submit(cfg, _context(_source()), mode=mode))
        executor.composer.compose_experiment_in_memory.assert_not_called()
        assert coordinator.payload is None


def test_executor_replay_keeps_member_preflight_and_never_prepares_factors():
    import asyncio
    from backend.tests.unified_engine.test_qe_prediction_replay import _config, _context, _executor, _source
    from backend.services.quantevolver.executors.backtest import BacktestMode

    source = _source()
    cfg = _config()
    cfg.model_params_base = {"qe_l2_member_loss_manifest": {"assets": {"prediction": {"sha256": source.sha256}}}}
    preflight = "python qe_l2_member_loss_consumer.py --formal-manifest manifest.json --config conf.yaml --prediction frozen_prediction.pkl"
    executor, coordinator = _executor(
        "cd /workspace && python prepare_factors.py && . ./.factor_env && "
        + preflight + " && python qrun_limit_minute.py conf.yaml"
    )
    result = asyncio.run(executor.submit(cfg, _context(source), mode=BacktestMode.PREDICTION_REPLAY))
    assert preflight in result.wsl_command
    assert result.wsl_command.count("--pred-backtest frozen_prediction.pkl") == 1
    assert "prepare_factors.py" not in result.wsl_command
    assert coordinator.source.backtest_only and not coordinator.source.parallel_training_eligible


def test_formal_cli_rejects_prediction_window_shrink_even_if_port_unchanged(formal, monkeypatch, capsys):
    m = formal.manifest
    conf = {"qlib_init": {"provider_uri": {"day": m["dataset_binding"]["provider_uri_day"],
            "1min": m["dataset_binding"]["provider_uri_1min"]}}, "market": "stock_universe",
            "port_analysis_config": original_port(),
            "task": {"dataset": {"kwargs": {"segments": {"test": ["2026-03-31", "2026-10-30"]}}}}}
    execution = {k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}
    m["execution_contract"] = execution
    m["execution_sha256"] = consumer.canonical_sha({"files": m["execution_files_sha256"], "contract": execution})
    write(formal.root, formal.path.name, m)
    path = formal.root / "conf.yaml"
    path.write_text(yaml.safe_dump(conf), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["consumer", "--formal-manifest", str(formal.path), "--config", str(path),
                                     "--prediction", m["assets"]["prediction"]["path"]])
    assert consumer.main() == 0
    assert json.loads(capsys.readouterr().out)["economic_days"] == 141
    conf["task"]["dataset"]["kwargs"]["segments"]["test"][0] = "2026-04-30"
    path.write_text(yaml.safe_dump(conf), encoding="utf-8")
    assert consumer.main() == 2
    assert "EXECUTION_PREDICTION_WINDOW" in capsys.readouterr().out
