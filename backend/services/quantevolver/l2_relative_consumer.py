"""Optional QE compose boundary for the approved six-account loss experiment."""
from copy import deepcopy
import ast
import base64
import hashlib
import json
from pathlib import Path
import re

import yaml

from scripts.qe_l2_relative_admission import BASE_SOURCE_SHA, validate_admission
from scripts.qe_l2_risk_admission import canonical_sha, physical_binding

PARAM = "qe_l2_relative_manifest"
MANIFEST = "qe_l2_relative_manifest.json"
# Reviewed C1 execution, with only the approved wrapper and bounded terminal
# interval substitutions. Pins every fee, strategy and inner execution option.
EXECUTION_PORT_SHA = "afbb6f11361b19c1cfc599107053064aa5eea1305b1e1da9202bfad786b5c8de"
WORKERS = (
    "qe_l2_relative_admission.py", "qe_l2_relative_consumer.py", "qe_l2_relative_strategy.py",
    "qe_l2_continuous_consumer.py", "qe_l2_continuous_admission.py",
    "qe_l2_risk_consumer.py", "qe_l2_risk_admission.py", "qe_terminal_interval_executor.py",
)


def extract_relative(params):
    params = dict(params or {})
    manifest = params.pop(PARAM, None)
    if manifest is None:
        return params, None
    if any(params.get(k) for k in ["qe_l2_continuous_manifest", "qe_l2_study_manifest",
                                  "qe_stock_probability_study", "qe_l2_risk_enabled", "enable_sector_hmm",
                                  "enable_hmm_risk_gate", "sector_risk_overlay_enabled"]):
        raise ValueError("QE_L2_RELATIVE_OTHER_OVERLAY_FORBIDDEN")
    validate_admission(manifest)
    return params, deepcopy(manifest)


def bind_relative_hooks(source):
    from .l2_continuous_consumer import bind_base_target_hooks

    adapted, receipt = bind_base_target_hooks(source)
    if receipt["original_source_sha256"] != BASE_SOURCE_SHA:
        raise ValueError("QE_L2_RELATIVE_BASE_SOURCE_DRIFT")
    tree = ast.parse(adapted)
    sites = [n for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr == "_adjust_target_weight_map"]
    if len(sites) != 1 or sites[0].keywords:
        raise ValueError("QE_L2_RELATIVE_RANK_HOOK_UNSUPPORTED")
    sites[0].keywords.append(ast.keyword(arg="ranked", value=ast.Name(id="ranked", ctx=ast.Load())))
    source = ast.unparse(ast.fix_missing_locations(tree)) + "\n"
    receipt.update(relative_rank_hook=True, relative_source_sha256=hashlib.sha256(source.encode()).hexdigest())
    return source, receipt


def apply_relative(result, manifest, *, scripts_dir):
    if manifest is None:
        return result
    validate_admission(manifest)
    m = deepcopy(manifest)
    binding = m["dataset_binding"]
    observed = result.get("direct_v2_dataset_binding")
    if not isinstance(observed, dict) or physical_binding(binding) != physical_binding(observed):
        raise ValueError("QE_L2_RELATIVE_EXECUTION_BINDING")
    required = {"loss", "prediction", "mapping", "mapping_receipt", "authority", "calendar", "pit"}
    if m["arm"] == "R":
        required.add("targets")
    if set(m["assets"]) != required:
        raise ValueError("QE_L2_RELATIVE_ASSET_FIELDS")
    if (m["assets"]["calendar"]["sha256"] != binding["day_pins"]["calendar_sha256"]
            or m["assets"]["pit"]["sha256"] != binding["selection_pins"]["instruments_sha256"]):
        raise ValueError("QE_L2_RELATIVE_TRANSPORT_PIN")
    files = result["experiment_files"]
    for key, ref in m["assets"].items():
        if key == "prediction":
            ref["path"] = "frozen_prediction.pkl"
            continue
        if key in ("calendar", "pit"):
            continue
        raw = Path(ref["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != ref["sha256"]:
            raise ValueError("QE_L2_RELATIVE_TRANSPORT_HASH: " + key)
        dest = "qe_l2_relative_" + key + Path(ref["path"]).suffix
        files[dest + ".b64"] = base64.b64encode(raw).decode("ascii")
        ref["path"] = dest
    conf = yaml.safe_load(files["conf.yaml"])
    if (conf["qlib_init"]["provider_uri"] != {"day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}
            or conf["market"] != binding["selection_pins"]["instrument_name"]):
        raise ValueError("QE_L2_RELATIVE_PROVIDER")
    port = conf["port_analysis_config"]
    strategy, backtest, executor = port["strategy"], port["backtest"], port["executor"]
    if (strategy["class"], strategy["module_path"]) != (
            "SuspendFilterScoreWeightedTopkStrategyV2", "qe_suspend_filter_score_weighted_strategy"):
        raise ValueError("QE_L2_RELATIVE_BASE_STRATEGY")
    if ((m["start"], m["end"], m["decision_days"]) != ("2026-04-01", "2026-09-30", 125)
            or str(conf["task"]["dataset"]["kwargs"]["segments"]["test"][0]) != "2026-03-31"
            or str(backtest["end_time"]) != "2026-09-30" or backtest["account"] != 100_000_000
            or backtest["exchange_kwargs"]["freq"] != "1min" or executor["class"] != "NestedExecutor"
            or executor["kwargs"]["inner_executor"]["class"] != "SimulatorExecutor"):
        raise ValueError("QE_L2_RELATIVE_EXECUTION_CONTRACT")
    files["custom_strategy.py"], receipt = bind_relative_hooks(files["custom_strategy.py"])
    files["qe_l2_relative_base_adapter.json"] = json.dumps(receipt, sort_keys=True)
    for name in WORKERS:
        files[name] = (Path(scripts_dir) / name).read_text(encoding="utf-8")
    # A reference cannot be reused after source drift in the packaged workers.
    code = {name: hashlib.sha256(content.encode()).hexdigest()
            for name, content in sorted(files.items())
            if name.endswith(".py") and name != "prepare_factors.py"}
    backtest["start_time"] = "2026-04-01"
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    executor["kwargs"]["inner_executor"].update(
        {"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"})
    strategy.update({"class": "L2RelativeScoreWeightedTopkStrategyV2", "module_path": "qe_l2_relative_strategy"})
    strategy["kwargs"]["qe_l2_relative_manifest_file"] = MANIFEST
    execution_contract = {k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}
    execution_contract = json.loads(json.dumps(execution_contract, default=lambda x: x.isoformat()))
    if canonical_sha(execution_contract["port_analysis_config"]) != EXECUTION_PORT_SHA:
        raise ValueError("QE_L2_RELATIVE_BASE_EXECUTION_DRIFT")
    execution_sha = canonical_sha({"files": code, "contract": execution_contract})
    if m["arm"] == "R" and m.get("target_source", {}).get("execution_sha256") != execution_sha:
        raise ValueError("QE_L2_RELATIVE_REFERENCE_EXECUTION_DRIFT")
    m["execution_sha256"] = execution_sha
    m["execution_files_sha256"] = code
    m["execution_contract"] = execution_contract
    files["conf.yaml"] = yaml.safe_dump(conf, sort_keys=False, allow_unicode=True)
    files[MANIFEST] = json.dumps(m, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
    for key in ("wsl_command", "wsl_command_core"):
        if result.get(key):
            command, count = re.subn(
                r"(?<!\S)python qrun_limit_minute\.py",
                f"python qe_l2_relative_consumer.py {MANIFEST} --config conf.yaml --prediction frozen_prediction.pkl && python qrun_limit_minute.py",
                result[key])
            if count != 1:
                raise ValueError("QE_L2_RELATIVE_RUNNER")
            result[key] = command
    return result
