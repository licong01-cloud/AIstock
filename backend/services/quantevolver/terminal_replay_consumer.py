"""Optional terminal-bar wiring; ordinary QE and HMM consumers remain unchanged."""
from copy import deepcopy
import json
from pathlib import Path
import re

import yaml

from scripts.qe_terminal_replay_preflight import PARAM, load_config, mask_template_expressions, normalize_spec, require


def extract_terminal_replay(params):
    params = dict(params or {})
    value = params.pop(PARAM, None)
    if value is None:
        return params, None
    spec = normalize_spec(value)
    require(not any(params.get(k) for k in ("qe_l2_relative_manifest", "qe_l2_continuous_manifest", "qe_l2_study_manifest")), "HMM_CONFLICT")
    summary = params.get("_qe_active_dataset_summary")
    require(isinstance(summary, dict) and summary.get("dataset_manifest_sha256") == spec["dataset_manifest_sha256"], "ACTIVE_IDENTITY")
    return params, spec


def require_terminal_replay_mode(value, *, mode, source):
    if value is None:
        return
    spec = normalize_spec(value)
    require(mode == "prediction_replay" and source is not None, "MODE")
    require(source.sha256 == spec["prediction_sha256"], "SOURCE_IDENTITY")


def _node(mapping, key):
    require(isinstance(mapping, yaml.MappingNode), "CONFIG_MAPPING")
    matches = [value for field, value in mapping.value if field.value == key]
    require(len(matches) == 1, "CONFIG_FIELD")
    return matches[0]


def apply_terminal_replay(result, spec, *, scripts_dir):
    if spec is None:
        return result
    spec = normalize_spec(spec)
    binding = result.get("direct_v2_dataset_binding")
    require(isinstance(binding, dict) and binding.get("cutoff") == spec["terminal_date"], "BINDING_CUTOFF")
    files = result["experiment_files"]
    original_yaml = files["conf.yaml"]
    conf = load_config(original_yaml)
    require(conf["qlib_init"]["provider_uri"] == {"day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}
            and conf["market"] == binding["selection_pins"]["instrument_name"], "PROVIDERS_OR_PIT")
    port = conf["port_analysis_config"]
    require(str(port["backtest"]["start_time"]) == spec["start"]
            and str(port["backtest"]["end_time"]) == spec["terminal_date"]
            and port["backtest"]["exchange_kwargs"]["freq"] == "1min", "CONFIG_WINDOW")
    executor = port["executor"]
    inner = executor["kwargs"]["inner_executor"]
    require((executor["class"], inner["class"]) == ("NestedExecutor", "SimulatorExecutor"), "EXECUTORS")
    require(executor["module_path"] == inner["module_path"] == "qlib.backtest.executor", "EXECUTOR_MODULE")
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    inner.update({"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"})
    for name in ("qe_terminal_replay_preflight.py", "qe_terminal_interval_executor.py"):
        require(name not in files, "WORKER_COLLISION")
        files[name] = (Path(scripts_dir) / name).read_text(encoding="utf-8")
    envelope_name = "qe_terminal_replay_contract.json"
    require(envelope_name not in files, "ENVELOPE_COLLISION")
    files[envelope_name] = json.dumps({"spec": spec, "dataset_binding": deepcopy(binding)}, ensure_ascii=False, sort_keys=True)
    # Change exactly four scalar values. Preserve all original anchors,
    # aliases and the frozen ALSTM recipe's unevaluated Jinja byte-for-byte.
    tree = yaml.compose(mask_template_expressions(original_yaml))
    port_node = _node(tree, "port_analysis_config")
    start, end = port_node.start_mark.index, port_node.end_mark.index
    require("{{" not in original_yaml[start:end], "TEMPLATED_PORTFOLIO_UNSUPPORTED")
    executor_node = _node(port_node, "executor")
    inner_node = _node(_node(executor_node, "kwargs"), "inner_executor")
    replacements = []
    for owner, values in ((executor_node, executor), (inner_node, inner)):
        for field in ("class", "module_path"):
            node = _node(owner, field)
            a, b = node.start_mark.index, node.end_mark.index
            require(isinstance(node, yaml.ScalarNode) and start <= a < b <= end, "EXECUTOR_SCALAR")
            replacements.append((a, b, json.dumps(values[field])))
    require(len({a for a, _, _ in replacements}) == 4, "EXECUTOR_ALIAS_UNSUPPORTED")
    updated = original_yaml
    for a, b, value in sorted(replacements, reverse=True):
        updated = updated[:a] + value + updated[b:]
    files["conf.yaml"] = updated
    for key in ("wsl_command", "wsl_command_core"):
        if result.get(key):
            command, count = re.subn(r"(?<!\S)python qrun_limit_minute\.py",
                f"python qe_terminal_replay_preflight.py {envelope_name} --config conf.yaml --prediction frozen_prediction.pkl && python qrun_limit_minute.py",
                result[key])
            require(count == 1, "RUNNER")
            result[key] = command
    return result
