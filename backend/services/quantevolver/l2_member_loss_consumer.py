"""Default-off, immutable-admission member-loss natural-entry Composer boundary."""
from copy import deepcopy
import base64
import hashlib
import json
from pathlib import Path
import re

import yaml
from scripts.qe_l2_member_loss_admission import validate_admission
from scripts.qe_l2_risk_admission import canonical_sha, physical_binding
from .l2_natural_entry_consumer import bind_natural_entry_hook, WORKERS as NATURAL_WORKERS
from .l2_relative_consumer import EXECUTION_PORT_SHA

PARAM = "qe_l2_member_loss_manifest"
MANIFEST = "qe_l2_member_loss_manifest.json"
WORKERS = (*NATURAL_WORKERS, "qe_l2_member_loss_admission.py", "qe_l2_member_loss_consumer.py", "qe_l2_member_loss_strategy.py")
OTHER_OVERLAYS = (
    "qe_l2_natural_entry_manifest", "qe_terminal_replay", "qe_l2_relative_manifest", "qe_l2_continuous_manifest",
    "qe_l2_study_manifest", "qe_stock_probability_study", "qe_l2_risk_enabled", "enable_sector_hmm",
    "enable_hmm_risk_gate", "sector_risk_overlay_enabled",
)


def extract_member_loss(params):
    params = dict(params or {})
    manifest = params.pop(PARAM, None)
    if manifest is None:
        return params, None
    if any(params.get(k) for k in OTHER_OVERLAYS):
        raise ValueError("QE_L2_MEMBER_OTHER_OVERLAY_FORBIDDEN")
    validate_admission(manifest)
    return params, deepcopy(manifest)


def apply_member_loss(result, manifest, *, scripts_dir):
    if manifest is None:
        return result
    validate_admission(manifest)
    m = deepcopy(manifest)
    binding, observed = m["dataset_binding"], result.get("direct_v2_dataset_binding")
    if not isinstance(observed, dict) or physical_binding(binding) != physical_binding(observed):
        raise ValueError("QE_L2_MEMBER_EXECUTION_BINDING")
    if (
        m["assets"]["calendar"]["sha256"] != binding["day_pins"]["calendar_sha256"]
        or m["assets"]["pit"]["sha256"] != binding["selection_pins"]["instruments_sha256"]
    ):
        raise ValueError("QE_L2_MEMBER_TRANSPORT_PIN")
    files = result["experiment_files"]
    conf = yaml.safe_load(files["conf.yaml"])
    if (
        conf["qlib_init"]["provider_uri"] != {"day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}
        or conf["market"] != binding["selection_pins"]["instrument_name"]
    ):
        raise ValueError("QE_L2_MEMBER_PROVIDER")
    port = conf["port_analysis_config"]
    strategy, backtest, executor = port["strategy"], port["backtest"], port["executor"]
    if (strategy["class"], strategy["module_path"]) != (
        "SuspendFilterScoreWeightedTopkStrategyV2", "qe_suspend_filter_score_weighted_strategy"
    ):
        raise ValueError("QE_L2_MEMBER_BASE_STRATEGY")
    if (
        list(map(str, conf["task"]["dataset"]["kwargs"]["segments"]["test"])) != ["2026-03-31", "2026-10-30"]
        or str(backtest["end_time"]) != "2026-10-29" or backtest["account"] != 100_000_000
        or backtest["exchange_kwargs"]["freq"] != "1min" or strategy["kwargs"].get("topk") != 50
        or executor["class"] != "NestedExecutor" or executor["kwargs"]["inner_executor"]["class"] != "SimulatorExecutor"
    ):
        raise ValueError("QE_L2_MEMBER_EXECUTION_CONTRACT")
    # Pin original C1 policy, fees, group/weight/exit and inner execution values.
    # Only the explicit new window and reviewed wrapper/terminal substitutions differ.
    check = deepcopy(port)
    check["backtest"].update(start_time="2026-04-01", end_time="2026-09-30")
    check["executor"].update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    check["executor"]["kwargs"]["inner_executor"].update(
        {"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"})
    check["strategy"].update({"class": "L2RelativeScoreWeightedTopkStrategyV2", "module_path": "qe_l2_relative_strategy"})
    check["strategy"]["kwargs"]["qe_l2_relative_manifest_file"] = "qe_l2_relative_manifest.json"
    if canonical_sha(json.loads(json.dumps(check, default=str))) != EXECUTION_PORT_SHA:
        raise ValueError("QE_L2_MEMBER_BASE_EXECUTION_DRIFT")
    files["custom_strategy.py"], adapter = bind_natural_entry_hook(files["custom_strategy.py"])
    files["qe_l2_member_loss_base_adapter.json"] = json.dumps(adapter, sort_keys=True)
    for key, ref in m["assets"].items():
        if key == "prediction":
            ref["path"] = "frozen_prediction.pkl"
            continue
        if key in ("calendar", "pit") or (key == "loss" and m["arm"] == "B0"):
            continue
        raw = Path(ref["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != ref["sha256"]:
            raise ValueError("QE_L2_MEMBER_TRANSPORT_HASH: " + key)
        destination = "qe_l2_member_loss_" + key + Path(ref["path"]).suffix
        files[destination + ".b64"] = base64.b64encode(raw).decode("ascii")
        ref["path"] = destination
    for name in WORKERS:
        files[name] = (Path(scripts_dir)/name).read_text(encoding="utf-8")
    code = {name: hashlib.sha256(text.encode()).hexdigest() for name, text in sorted(files.items())
            if name.endswith(".py") and name != "prepare_factors.py"}
    backtest["start_time"] = "2026-04-01"
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    executor["kwargs"]["inner_executor"].update(
        {"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"})
    strategy.update({"class": "L2MemberEntryScoreWeightedTopkStrategyV2", "module_path": "qe_l2_member_loss_strategy"})
    strategy["kwargs"]["qe_l2_member_loss_manifest_file"] = MANIFEST
    execution = json.loads(json.dumps({k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}, default=str))
    m.update(execution_files_sha256=code, execution_contract=execution,
             execution_sha256=canonical_sha({"files": code, "contract": execution}))
    files["conf.yaml"] = yaml.safe_dump(conf, sort_keys=False, allow_unicode=True)
    files[MANIFEST] = json.dumps(m, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
    for key in ("wsl_command", "wsl_command_core"):
        if result.get(key):
            command, count = re.subn(
                r"(?<!\S)python qrun_limit_minute\.py",
                f"python qe_l2_member_loss_consumer.py --formal-manifest {MANIFEST} --config conf.yaml --prediction frozen_prediction.pkl && python qrun_limit_minute.py",
                result[key],
            )
            if count != 1:
                raise ValueError("QE_L2_MEMBER_RUNNER")
            result[key] = command
    return result
