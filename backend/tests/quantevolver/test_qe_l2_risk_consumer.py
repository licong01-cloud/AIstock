"""Focused compose and reader failures; real full-asset validation runs on node1."""
from copy import deepcopy
import hashlib
import json
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
    manifest["release_identity"] = {
        "dataset_manifest_sha256": "522e8e35896e85fc9289ca2ceca1cd6127f4c7978a72766887c0429dc9b7f410",
        "frozen_release_generation": "20260930-monthly-v2-unified-correction-4ab0b48642c3"}
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
    with pytest.raises(L2ContractError, match="LEGACY_ASSET_REBIND_FORBIDDEN"):
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


@pytest.mark.parametrize("version", ["unreviewed", "", 123, []])
def test_request_cannot_self_approve_new_asset(version):
    manifest, _, _ = study_fixture()
    manifest.update(asset_admission_version=version, asset_admissions={"unreviewed": "self-approved"})
    manifest["assets"]["risk"]["sha256"] = "9" * 64
    with pytest.raises(ValueError, match="ADMISSION_UNKNOWN"):
        extract_study({"qe_l2_study_manifest": manifest})


def test_legacy_asset_cannot_be_rebound_to_new_release_before_io():
    from qe_l2_risk_consumer import L2RiskInputs

    manifest, _, _ = study_fixture()
    manifest["release_identity"]["dataset_manifest_sha256"] = "d" * 64
    with pytest.raises(ValueError, match="LEGACY_ASSET_REBIND_FORBIDDEN"):
        extract_study({"qe_l2_study_manifest": manifest})
    # The reader checks admission before attempting any nonexistent asset path.
    manifest.update(schema_version="qe_l2_risk_consumer_v1")
    import tempfile

    with tempfile.TemporaryDirectory() as folder:
        path = Path(folder) / "manifest.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with pytest.raises(L2ContractError, match="LEGACY_ASSET_REBIND_FORBIDDEN"):
            L2RiskInputs(path)


def batch_fixture(tmp_path, monkeypatch, *, versioned=True):
    from scripts import qe_l2_risk_admission as admissions
    from scripts.run_qe_l2_risk_controlled_study import H1_GENERATION, H1_MANIFEST, SEEDS

    manifest, _, _ = study_fixture()
    manifest["release_identity"].update(release_id="qe_hmm_full_v2_20260930", cutoff="2026-09-30")
    batch = None
    if versioned:
        manifest.update(asset_admission_version="test-only-new-asset", authority_canonical_sha256="a" * 64)
        manifest["assets"]["risk"]["sha256"] = "9" * 64
        manifest["release_identity"].update(dataset_manifest_sha256=H1_MANIFEST, frozen_release_generation=H1_GENERATION)
        # Simulate a separately source-reviewed admission; requests cannot do this.
        record = {"risk_sha256": "9" * 64,
                  "release_identity_sha256": admissions.canonical_sha(manifest["release_identity"]),
                  "dataset_binding_sha256": admissions.canonical_sha(admissions.physical_binding(manifest["dataset_binding"])),
                  "authority_canonical_sha256": "a" * 64}
        monkeypatch.setattr(admissions, "ADMISSIONS", {**admissions.ADMISSIONS, "test-only-new-asset": record})
        batch = {"schema_version": "qe_l2_controlled_batch_v2", "status": "APPROVED",
                 "contract": manifest["contract"],
                 "authorization_scope": "qe_hmm_long_plan_h1", "batch_id": "qe_l2_h1_test_20261025",
                 "generation": H1_GENERATION, "dataset_manifest_sha256": H1_MANIFEST,
                 "release_id": "qe_hmm_full_v2_20260930", "cutoff": "2026-09-30",
                 "asset_admission_version": "test-only-new-asset"}
    (tmp_path / "user_approval_receipt.json").write_text(json.dumps({
        "status": "APPROVED", "contract": manifest["contract"]}), encoding="utf-8")
    assets = []
    for seed in SEEDS:
        current = deepcopy(manifest)
        current["assets"]["prediction"]["sha256"] = hashlib.sha256(str(seed).encode()).hexdigest()
        assets.append({"seed": seed, "prediction_sha256": current["assets"]["prediction"]["sha256"]})
        (tmp_path / f"consumer_manifest_seed{seed}.json").write_text(json.dumps(current), encoding="utf-8")
    (tmp_path / "shared_prediction_receipt.json").write_text(json.dumps({
        "assets": assets, "dataset_binding": manifest["dataset_binding"]}), encoding="utf-8")
    if versioned:
        batch["shared_prediction_receipt_sha256"] = hashlib.sha256((tmp_path / "shared_prediction_receipt.json").read_bytes()).hexdigest()
        batch["consumer_manifest_sha256"] = {
            str(seed): hashlib.sha256((tmp_path / f"consumer_manifest_seed{seed}.json").read_bytes()).hexdigest()
            for seed in SEEDS}
        (tmp_path / "study_batch_receipt.json").write_text(json.dumps(batch), encoding="utf-8")
    return batch


@pytest.mark.parametrize("field", ["risk", "release", "pit", "authority"])
def test_reviewed_version_rejects_cross_identity_changes(tmp_path, monkeypatch, field):
    batch_fixture(tmp_path, monkeypatch)
    manifest = json.loads((tmp_path / "consumer_manifest_seed42.json").read_text())
    extract_study({"qe_l2_study_manifest": manifest})
    if field == "risk":
        manifest["assets"]["risk"]["sha256"] = "f" * 64
    elif field == "release":
        manifest["release_identity"]["cutoff"] = "2026-08-31"
    elif field == "pit":
        manifest["dataset_binding"]["selection_pins"]["instruments_sha256"] = "changed"
    else:
        manifest["authority_canonical_sha256"] = "changed"
    with pytest.raises(ValueError, match="ADMISSION_MISMATCH"):
        extract_study({"qe_l2_study_manifest": manifest})


def test_versioned_compose_packages_same_admission_source(tmp_path, monkeypatch):
    from scripts.qe_l2_risk_admission import validate_admission

    batch_fixture(tmp_path, monkeypatch)
    manifest = json.loads((tmp_path / "consumer_manifest_seed42.json").read_text())
    manifest["dataset_binding"]["selection_pins"]["coverage_receipt_sha256"] = "another-window"
    validate_admission(manifest)
    _, study = extract_study({"qe_l2_risk_enabled": True, "qe_l2_study_manifest": manifest})
    _, _, result = study_fixture()
    output = apply_study(result, study, scripts_dir=Path(__file__).resolve().parents[3] / "scripts")
    assert output["experiment_files"]["qe_l2_risk_admission.py"] == (
        Path(__file__).resolve().parents[3] / "scripts/qe_l2_risk_admission.py").read_text(encoding="utf-8")


@pytest.mark.parametrize("seeds", [[42, 2024], [42, 2024, 2026, 7], [42, 42, 2026], [2026, 2024, 42]])
def test_batch_requires_exact_three_ordered_seeds(tmp_path, monkeypatch, seeds):
    from scripts.run_qe_l2_risk_controlled_study import load_batch_inputs

    batch_fixture(tmp_path, monkeypatch)
    path = tmp_path / "shared_prediction_receipt.json"
    shared = json.loads(path.read_text())
    shared["assets"] = [{"seed": seed} for seed in seeds]
    path.write_text(json.dumps(shared), encoding="utf-8")
    with pytest.raises(ValueError, match="SEEDS_CHANGED"):
        load_batch_inputs(tmp_path)
    assert not (tmp_path / "composed").exists()


def test_batch_rejects_wrong_shared_prediction(tmp_path, monkeypatch):
    from scripts.run_qe_l2_risk_controlled_study import load_batch_inputs

    batch_fixture(tmp_path, monkeypatch)
    path = tmp_path / "shared_prediction_receipt.json"
    shared = json.loads(path.read_text())
    shared["assets"][1]["prediction_sha256"] = "f" * 64
    path.write_text(json.dumps(shared), encoding="utf-8")
    batch_path = tmp_path / "study_batch_receipt.json"
    batch = json.loads(batch_path.read_text())
    batch["shared_prediction_receipt_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    batch_path.write_text(json.dumps(batch), encoding="utf-8")
    with pytest.raises(ValueError, match="SHARED_PREDICTION_IDENTITY_MISMATCH"):
        load_batch_inputs(tmp_path)


def test_batch_rejects_receipt_changed_after_freeze(tmp_path, monkeypatch):
    from scripts.run_qe_l2_risk_controlled_study import load_batch_inputs

    batch_fixture(tmp_path, monkeypatch)
    path = tmp_path / "shared_prediction_receipt.json"
    shared = json.loads(path.read_text())
    shared["assets"][0]["model_sha256"] = "another-model"
    path.write_text(json.dumps(shared), encoding="utf-8")
    with pytest.raises(ValueError, match="BATCH_INPUT_DIGEST_MISMATCH"):
        load_batch_inputs(tmp_path)


@pytest.mark.parametrize("field,value", [("batch_id", "qe_l2_entry_20261023"), ("batch_id", "../escape"),
    ("status", "PENDING"), ("authorization_scope", "other"), ("generation", "other-active")])
def test_batch_rejects_old_ids_and_unapproved_identity(tmp_path, monkeypatch, field, value):
    from scripts.run_qe_l2_risk_controlled_study import load_batch_inputs

    batch = batch_fixture(tmp_path, monkeypatch)
    batch[field] = value
    (tmp_path / "study_batch_receipt.json").write_text(json.dumps(batch), encoding="utf-8")
    with pytest.raises(ValueError, match="BATCH_IDENTITY_INVALID"):
        load_batch_inputs(tmp_path)


def test_active_refresh_stops_pinned_h1_without_switching(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from scripts.run_qe_l2_risk_controlled_study import verify_batch_profile

    batch = batch_fixture(tmp_path, monkeypatch)
    profile = SimpleNamespace(generation=batch["generation"], release_id=batch["release_id"], cutoff=batch["cutoff"],
                              raw={"components": {"dataset_manifest_sha256": batch["dataset_manifest_sha256"]}})
    verify_batch_profile(batch, profile)
    profile.generation += "-suspendfacts1"
    with pytest.raises(ValueError, match="APPROVED_PROFILE_IDENTITY_CHANGED"):
        verify_batch_profile(batch, profile)


@pytest.mark.parametrize("versioned", [False, True])
def test_submit_rechecks_frozen_batch_and_worker_source(tmp_path, monkeypatch, versioned):
    from scripts.run_qe_l2_risk_controlled_study import load_batch_inputs, verify_batch_plan

    batch_fixture(tmp_path, monkeypatch, versioned=versioned)
    _, shared, batch = load_batch_inputs(tmp_path)
    manifest = json.loads((tmp_path / "consumer_manifest_seed42.json").read_text())
    task = batch["batch_id"] + "_s42_b"
    plan = {"task_id": task, "experiment_id": hashlib.sha256(task.encode()).hexdigest()[:8], "seed": 42, "arm": "B",
            "prediction_sha256": shared["assets"][0]["prediction_sha256"], "batch_identity": batch,
            "custom_params": {"qe_l2_study_manifest": manifest, "qe_l2_risk_enabled": False}}
    files = {"qe_l2_risk_manifest.json": json.dumps(manifest), "qe_l2_risk_admission.py": (
        Path(__file__).resolve().parents[3] / "scripts/qe_l2_risk_admission.py").read_text(encoding="utf-8")}
    assert verify_batch_plan(tmp_path, plan, files) == batch
    for key, value in [("task_id", "qe_l2_entry_20261023_s42_l"), ("experiment_id", "other"),
                       ("prediction_sha256", "f" * 64), ("batch_identity", {})]:
        changed = deepcopy(plan)
        changed[key] = value
        with pytest.raises(ValueError, match="BATCH_PLAN_IDENTITY_MISMATCH"):
            verify_batch_plan(tmp_path, changed, files)
    if versioned:
        files["qe_l2_risk_admission.py"] += "\n# unreviewed change"
        with pytest.raises(ValueError, match="WORKER_ADMISSION_SOURCE_MISMATCH"):
            verify_batch_plan(tmp_path, plan, files)
