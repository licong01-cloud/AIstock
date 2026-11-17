"""Compose a model-free absolute-target account through the existing QE runner."""
from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path
import shlex
import uuid

import yaml

from backend.services.quantevolver.qe_absolute_targets import MANIFEST_FILE, PARAM, SCHEMA_V2, TARGET_FILE, file_sha256, normalize_contract, require, validate_exposure_audit


def validate_absolute_result(value, contract):
    contract = normalize_contract(contract)
    require(isinstance(value, dict), "QE_ABSOLUTE_TARGET_RESULT_REQUIRED")
    require(value.get("schema_version") == contract["schema_version"]
            and value.get("status") == "COMPLETE_ACCOUNT_AND_TARGET_CONSUMPTION"
            and value.get("arm") == contract["arm"]
            and value.get("targets_sha256") == contract["targets"]["sha256"]
            and value.get("dataset") == contract["dataset"]
            and value.get("window") == contract["window"]
            and value.get("account_days") == contract["window"]["account_days"]
            and value.get("target_days_consumed") == contract["window"]["account_days"]
            and value.get("new_fit_calls") == 0 and value.get("new_predict_calls") == 0
            and value.get("IC") is None and value.get("RankIC") is None
            and value.get("label_status") == "NOT_APPLICABLE"
            and value.get("known_at") == "UNVERIFIED", "QE_ABSOLUTE_TARGET_RESULT_MISMATCH")
    from backend.services.quantevolver.qe_absolute_targets import SHA

    require(bool(SHA.fullmatch(str(value.get("actions_sha256")))), "QE_ABSOLUTE_TARGET_ACTIONS_HASH_REQUIRED")
    if contract["schema_version"] == SCHEMA_V2:
        require(value.get("exposure_policy") == contract["exposure_policy"],
                "QE_ABSOLUTE_TARGET_EXPOSURE_RESULT_POLICY")
        validate_exposure_audit(value.get("exposure_audit"), contract)
    return dict(value)


async def recover_absolute_target_results(scheduler, *, task_id, loop_index, task, client,
                                          effective_node_id, config):
    """Recover collection/warehouse only; never POST another account or prediction."""
    loop_id = f"Loop{loop_index}"
    before = await client.inspect_loop_submission(task_id, loop_id)
    require((before.task_id, before.loop_id) == (task_id, loop_id) and before.status in {"completed", "failed"},
            "QE_ABSOLUTE_TARGET_RECOVERY_REMOTE_NOT_TERMINAL")
    from backend.services.quantevolver.qe_absolute_targets import SHA

    require(all(SHA.fullmatch(str(getattr(before, k, ""))) for k in ("request_digest", "submission_intent_hash")),
            "QE_ABSOLUTE_TARGET_RECOVERY_SUBMISSION_IDENTITY_REQUIRED")
    result = validate_absolute_result(
        await client.get_workspace_file(task_id, loop_id, "qe_absolute_target_result.json"),
        config[PARAM])
    recorder_before = await client.get_workspace_file(task_id, loop_id, "qe_current_recorder.json")
    require(isinstance(recorder_before, dict) and recorder_before.get("recorder_id"),
            "QE_ABSOLUTE_TARGET_RECOVERY_RECORDER_REQUIRED")
    metrics = await client.get_loop_metrics(task_id, loop_id)
    enhanced = await client.get_enhanced_metrics(task_id, loop_id)
    require(enhanced.get("absolute_target_result") == result, "QE_ABSOLUTE_TARGET_RECOVERY_ENHANCED_MISMATCH")
    after = await client.inspect_loop_submission(task_id, loop_id)
    require(all(getattr(before, k, None) == getattr(after, k, None) for k in
                ("task_id", "loop_id", "status", "request_digest", "submission_intent_hash"))
            and recorder_before == await client.get_workspace_file(task_id, loop_id, "qe_current_recorder.json"),
            "QE_ABSOLUTE_TARGET_RECOVERY_ATTEMPT_CHANGED")
    metrics.update(absolute_target_result=result, enhanced_metrics=enhanced, IC=None, Rank_IC=None,
                   label_status="NOT_APPLICABLE", execution_node_id=effective_node_id)
    experiment_id = scheduler._register_results_only_loop(
        task_id=task_id, loop_index=loop_index, evolution_loop_db_id=f"{task_id}_Loop{loop_index}",
        loop_id=loop_id, task=task, config=config, metrics=metrics,
        artifacts_summary={"absolute_target_result": result, "prediction": "NOT_APPLICABLE",
                           "metrics_source": "original_completed_absolute_target_account"})
    scheduler._archive_completed_loop_best_effort(task_id, f"{task_id}_Loop{loop_index}", loop_index)
    scheduler._record_research_backtest_best_effort(task_id, f"{task_id}_Loop{loop_index}", loop_index,
                                                   experiment_id=experiment_id)
    status = scheduler.recompute_custom_evo_task_status(task_id)
    return {"loop_id": f"{task_id}_Loop{loop_index}", "mode": "results_only", "experiment_id": experiment_id,
            "task_status": status, "new_fit_predict_backtest": 0}


def require_bound_contract(contract, summary):
    contract = normalize_contract(contract)
    expected = contract["dataset"]
    require(isinstance(summary, dict), "QE_ABSOLUTE_TARGET_ACTIVE_BINDING_REQUIRED")
    require(summary.get("release_id") == expected["release_id"]
            and summary.get("dataset_manifest_sha256") == expected["manifest_sha256"]
            and summary.get("profile_sha256") == expected["profile_sha256"],
            "QE_ABSOLUTE_TARGET_ACTIVE_IDENTITY_MISMATCH")
    return contract


def absolute_target_command_core(parts, activate):
    """Allow legacy Conda hooks; retain strict execution after activation."""
    require(parts.count(activate) == 1, "QE_ABSOLUTE_TARGET_BOOTSTRAP_COUNT")
    parts = list(parts)
    # Some installed hooks append to an optional unset LD_LIBRARY_PATH. Do not
    # modify those environments or invent a library path. Activation failures
    # still stop the && chain; nounset is restored before any scientific step.
    parts[parts.index(activate)] = "set +u && " + activate + " && set -u"
    return " && ".join(parts)


def compose_absolute_targets(composer, *, custom_params, factor_names, model_id, strategy_id,
                             data_split, experiment_name, execution_algo, execution_algo_params,
                             strategy_params, node_id, task_id, loop_index, train_only, skip_db_save, **unused):
    from .config_composer import (
        QE_DIRECT_V2_DATASET_BINDING_FILE, QE_MINUTE_RUNTIME_HELPER_FILES,
        QE_UNIVERSE_COVERAGE_RECEIPT_FILE, _direct_v2_dataset_binding,
    )
    from .qe_active_dataset_profile import QE_RUN_COVERAGE_RECEIPT_PARAM, QE_RUN_STOCK_POOL_CONTENT_PARAM

    require(not factor_names and not model_id and not strategy_id and not train_only,
            "QE_ABSOLUTE_TARGET_MODEL_FREE_COMPOSER")
    params = dict(custom_params or {})
    contract = require_bound_contract(params[PARAM], params.get("_qe_active_dataset_summary"))
    forbidden = [k for k in params if k.startswith("qe_l2_") or k in {
        "qe_stock_probability_study", "qe_terminal_replay", "qe_trend_entry_manifest", "enable_sector_hmm",
        "enable_hmm_risk_gate", "_seed_ensemble_config", "random_seed", "label_horizon"}]
    require(not forbidden, "QE_ABSOLUTE_TARGET_CONSUMER_CONFLICT: " + ",".join(forbidden))
    require(not set(strategy_params or {}) - {"initial_cash", "risk_policy"}, "QE_ABSOLUTE_TARGET_STRATEGY_OVERRIDE")
    require((strategy_params or {}).get("initial_cash", contract["initial_cash"]) == contract["initial_cash"],
            "QE_ABSOLUTE_TARGET_CASH_OVERRIDE")
    binding = _direct_v2_dataset_binding(params)
    require(binding is not None and binding.release_id == contract["dataset"]["release_id"],
            "QE_ABSOLUTE_TARGET_DIRECT_BINDING_REQUIRED")
    w = contract["window"]
    require(data_split and data_split["test_start"] == w["account_start"]
            and data_split["backtest_end"] == w["account_end"], "QE_ABSOLUTE_TARGET_SPLIT_MISMATCH")
    require(file_sha256(contract["targets"]["path"]) == contract["targets"]["sha256"],
            "QE_ABSOLUTE_TARGET_HASH_MISMATCH")
    import pyarrow.parquet as pq

    target_path = Path(contract["targets"]["path"])
    require(target_path.suffix == ".parquet" and not target_path.is_symlink()
            and target_path.stat().st_size <= 128 * 1024**2, "QE_ABSOLUTE_TARGET_ASSET_BOUNDS")
    metadata = pq.ParquetFile(target_path)
    require(0 < metadata.metadata.num_rows <= 5_000_000 and metadata.schema_arrow.names == [
        "signal_date", "target_date", "instrument", "sector", "state", "member_count", "weight_T", "weight_B"],
        "QE_ABSOLUTE_TARGET_ASSET_SCHEMA")
    reference = contract["execution_reference"]
    raw = Path(reference["path"]).read_bytes()
    require(hashlib.sha256(raw).hexdigest() == reference["sha256"], "QE_ABSOLUTE_TARGET_EXECUTION_HASH")
    original = yaml.safe_load(raw)
    port = original["port_analysis_config"]
    algo = composer._execution_algo_config(execution_algo, execution_algo_params)
    inner = port["executor"]["kwargs"]["inner_strategy"]
    require(inner == {"class": algo["class"], "module_path": algo["module_path"], "kwargs": algo["kwargs"]},
            "QE_ABSOLUTE_TARGET_EXECUTION_OVERRIDE")
    require(port["executor"]["class"] == "NestedExecutor"
            and port["executor"]["kwargs"]["time_per_step"] == "day"
            and port["executor"]["kwargs"]["inner_executor"]["class"] == "SimulatorExecutor"
            and port["executor"]["kwargs"]["inner_executor"]["kwargs"]["time_per_step"] == "1min",
            "QE_ABSOLUTE_TARGET_EXECUTOR_CONTRACT")
    cfg = composer._fetch_workspace_config(node_id)
    experiment_id = str(uuid.uuid4())[:8]
    experiment_name = experiment_name or f"qe_absolute_targets_{experiment_id}"
    workdir = cfg["workspace_base"].rstrip("/") + "/" + experiment_name
    files = {}
    day_provider = binding.provider_uri_day
    prepare = None
    pool_content = params.get(QE_RUN_STOCK_POOL_CONTENT_PARAM)
    if pool_content is not None:
        selection = binding.selection_pins
        require(isinstance(pool_content, str) and hashlib.sha256(pool_content.encode()).hexdigest()
                == selection["instruments_sha256"], "QE_ABSOLUTE_TARGET_POOL_HASH")
        filename = selection["instruments_file"]
        require(Path(filename).name == filename, "QE_ABSOLUTE_TARGET_POOL_FILENAME")
        files[filename] = pool_content
        day_provider = workdir + "/qe_provider_day"
        source, dest = shlex.quote(binding.provider_uri_day), shlex.quote(day_provider)
        prepare = " && ".join([f"mkdir -p {dest}/instruments",
                               *[f"ln -sfn {source}/{name} {dest}/{name}" for name in
                                 ("calendars", "features", "meta_export.json")],
                               f"cp -f {shlex.quote(filename)} {dest}/instruments/{shlex.quote(filename)}"])
    _, frozen = composer._prepare_risk_policy_runtime(custom_params=params, data_split=data_split,
                                                      qlib_data_path=day_provider)
    require(frozen is not None, "QE_ABSOLUTE_TARGET_FROZEN_POLICY_REQUIRED")
    files["qe_frozen_build_spec.json"] = frozen
    files[QE_DIRECT_V2_DATASET_BINDING_FILE] = json.dumps(binding.as_dict(), ensure_ascii=False, sort_keys=True)
    coverage = params.get(QE_RUN_COVERAGE_RECEIPT_PARAM)
    require(isinstance(coverage, str) and hashlib.sha256(coverage.encode()).hexdigest()
            == binding.selection_pins["coverage_receipt_sha256"], "QE_ABSOLUTE_TARGET_COVERAGE_HASH")
    files[QE_UNIVERSE_COVERAGE_RECEIPT_FILE] = coverage
    files[MANIFEST_FILE] = json.dumps(contract, ensure_ascii=False, sort_keys=True)
    files[TARGET_FILE + ".b64"] = base64.b64encode(Path(contract["targets"]["path"]).read_bytes()).decode("ascii")
    # Original target bytes and the exact executable reference remain auditable.
    files["qe_absolute_target_execution_reference.yaml"] = raw.decode("utf-8-sig")
    port = {"executor": port["executor"],
            "strategy": {"class": "AbsoluteTargetStrategy", "module_path": "qe_absolute_target_strategy",
                         "kwargs": {"manifest_file": MANIFEST_FILE,
                                    "calendar_file": day_provider.rstrip("/") + "/calendars/day.txt"}},
            "backtest": {**port["backtest"], "start_time": w["account_start"], "end_time": w["account_end"],
                         "account": contract["initial_cash"]}}
    conf = {"qlib_init": {**original["qlib_init"],
                          "provider_uri": {"day": day_provider, "1min": binding.provider_uri_1min}},
            "market": binding.selection_pins["instrument_name"], "benchmark": original["benchmark"],
            "experiment_name": experiment_name,
            "port_analysis_config": port, "task": {"record": []},
            "qe_runtime": {"mode": PARAM, "training": "NOT_APPLICABLE", "prediction": "NOT_APPLICABLE"},
            PARAM: contract}
    files["conf.yaml"] = yaml.safe_dump(conf, allow_unicode=True, sort_keys=False)
    scripts = Path(__file__).resolve().parents[3] / "scripts"
    # Explicit dependency closure, shared helpers stay authoritative.
    for name in {*QE_MINUTE_RUNTIME_HELPER_FILES, "qrun_limit_minute.py", "tail_twap_strategy.py",
                 "qe_runtime_resource.py", "qe_absolute_targets.py", "qe_absolute_target_strategy.py",
                 "qe_absolute_target_record.py"}:
        source = (Path(__file__).parent / name if name in {
            "qe_absolute_targets.py", "qe_absolute_target_strategy.py", "qe_absolute_target_record.py",
        } else composer._resolve_qe_helper_asset(scripts, name))
        require(source.is_file(), "QE_ABSOLUTE_TARGET_HELPER_MISSING: " + name)
        files[name] = source.read_text(encoding="utf-8-sig")
    files["read_exp_res.py"] = composer._get_read_exp_res_content()
    _, parts = composer._build_auto_wsl_command_parts(
        workdir, node_id=node_id, backtest_freq="1min", factor_cache_dir=cfg.get("factor_cache_dir"),
        factor_data_dir=binding.factor_data_dir, task_id=task_id, loop_index=loop_index,
        direct_v2_validation_enabled=True, index_context_path=binding.index_context_path,
        day_provider_prepare_command=prepare)
    require(parts.count("python qrun_limit_minute.py conf.yaml") == 1, "QE_ABSOLUTE_TARGET_RUNNER_COUNT")
    parts[parts.index("python qrun_limit_minute.py conf.yaml")] += " --absolute-target-replay"
    core = absolute_target_command_core(parts, composer._build_conda_activate_chain())
    if not skip_db_save:
        composer._save_experiment_record(
            experiment_id=experiment_id, experiment_name=experiment_name, exp_dir="<in_memory>/" + experiment_name,
            factor_names=[], model_id=None, strategy_id=None, data_split=data_split, custom_params=params,
            evolution_goal=None, llm_hypothesis=None)
    # The shared WSL envelope validates the leading canonical workspace cd.
    # Keep strict bash options inside that workspace without changing the core.
    return {"experiment_files": files, "wsl_command": "cd -- " + shlex.quote(workdir) + " && set -euo pipefail && " + core,
            "wsl_command_core": core, "wsl_workdir": workdir, "experiment_name": experiment_name,
            "experiment_id": experiment_id, "factor_count": 0, "has_custom_factors": False,
            "long_trend_evaluation_descriptor": None, "direct_v2_dataset_binding": binding.as_dict()}
