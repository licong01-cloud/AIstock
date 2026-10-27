"""Optional compose boundary for the independent continuous L2 contract."""

from copy import deepcopy
import base64
import hashlib
import json
from pathlib import Path
import re

import yaml
import pandas as pd

from scripts.qe_l2_continuous_admission import CONTRACT, validate_admission, validate_reference

PARAM = "qe_l2_continuous_manifest"
MANIFEST = "qe_l2_continuous_manifest.json"


def extract_continuous(custom_params):
    params = dict(custom_params or {})
    manifest = params.pop(PARAM, None)
    if manifest is None:
        return params, None
    if not isinstance(manifest, dict) or manifest.get("contract") != CONTRACT:
        raise ValueError("QE_L2_CONTINUOUS_CONTRACT_INVALID")
    if any(
        params.get(k)
        for k in (
            "qe_l2_study_manifest",
            "qe_l2_risk_enabled",
            "enable_sector_hmm",
            "enable_hmm_risk_gate",
            "sector_risk_overlay_enabled",
        )
    ):
        raise ValueError("QE_L2_CONTINUOUS_OTHER_OVERLAY_FORBIDDEN")
    validate_admission(manifest)
    if manifest.get("arm") not in ("B", "M0", "M1", "X0", "X1"):
        raise ValueError("QE_L2_CONTINUOUS_ARM_INVALID")
    return params, deepcopy(manifest)


def apply_continuous(result, manifest, *, scripts_dir):
    if manifest is None:
        return result
    validate_admission(manifest)
    from scripts.qe_l2_risk_admission import physical_binding

    binding = manifest["dataset_binding"]
    observed = result.get("direct_v2_dataset_binding")
    if not isinstance(observed, dict) or physical_binding(binding) != physical_binding(observed):
        raise ValueError("QE_L2_CONTINUOUS_EXECUTION_BINDING_MISMATCH")
    files = result["experiment_files"]
    manifest = deepcopy(manifest)
    expected_assets = {"M0", "M1", "prediction", "mapping", "mapping_receipt", "authority", "calendar", "pit"}
    if manifest["arm"] in ("X0", "X1"):
        expected_assets.add("reference")
    if set(manifest["assets"]) != expected_assets:
        raise ValueError("QE_L2_CONTINUOUS_ASSET_FIELDS_INVALID")
    if (
        manifest["assets"]["calendar"]["sha256"] != binding["day_pins"]["calendar_sha256"]
        or manifest["assets"]["pit"]["sha256"] != binding["selection_pins"]["instruments_sha256"]
    ):
        raise ValueError("QE_L2_CONTINUOUS_TRANSPORT_PIN_MISMATCH")
    if manifest["arm"] in ("X0", "X1"):
        calendar_ref = manifest["assets"]["calendar"]
        raw_calendar = Path(calendar_ref["path"]).read_bytes()
        if hashlib.sha256(raw_calendar).hexdigest() != calendar_ref["sha256"]:
            raise ValueError("QE_L2_CONTINUOUS_TRANSPORT_HASH_MISMATCH: calendar")
        calendar = pd.DatetimeIndex(pd.to_datetime(raw_calendar.decode("utf-8").splitlines()))
        selected = calendar[(calendar >= manifest["start"]) & (calendar <= manifest["end"])]
        if (
            not calendar.is_unique
            or not calendar.is_monotonic_increasing
            or len(selected) != 125
            or calendar.get_loc(selected[0]) == 0
        ):
            raise ValueError("QE_L2_CONTINUOUS_REFERENCE_CALENDAR_INVALID")
        as_of = {str(d.date()): str(calendar[calendar.get_loc(d) - 1].date()) for d in selected}
        ref = manifest["assets"]["reference"]
        payload = Path(ref["path"]).read_bytes()
        if hashlib.sha256(payload).hexdigest() != ref["sha256"]:
            raise ValueError("QE_L2_CONTINUOUS_TRANSPORT_HASH_MISMATCH: reference")
        validate_reference(json.loads(payload), manifest, as_of)
    # The existing workspace transport decodes .b64 before the command starts.
    # Prediction replay itself supplies the one shared frozen_prediction.pkl.
    for name, ref in manifest["assets"].items():
        if name == "prediction":
            ref["path"] = "frozen_prediction.pkl"
            continue
        payload = Path(ref["path"]).read_bytes()
        if hashlib.sha256(payload).hexdigest() != ref["sha256"]:
            raise ValueError("QE_L2_CONTINUOUS_TRANSPORT_HASH_MISMATCH: " + name)
        destination = "qe_l2_continuous_assets/" + name + (".parquet" if name == "mapping" else ".json")
        if destination + ".b64" in files:
            raise ValueError("QE_L2_CONTINUOUS_TRANSPORT_FILE_COLLISION")
        files[destination + ".b64"] = base64.b64encode(payload).decode("ascii")
        ref["path"] = destination
    conf = yaml.safe_load(files["conf.yaml"])
    if (
        conf["qlib_init"]["provider_uri"] != {"day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}
        or conf["market"] != binding["selection_pins"]["instrument_name"]
    ):
        raise ValueError("QE_L2_CONTINUOUS_EXECUTION_PROVIDER_MISMATCH")
    port = conf["port_analysis_config"]
    strategy = port["strategy"]
    backtest = port["backtest"]
    executor = port["executor"]
    if (strategy["class"], strategy["module_path"]) != (
        "SuspendFilterScoreWeightedTopkStrategyV2",
        "qe_suspend_filter_score_weighted_strategy",
    ):
        raise ValueError("QE_L2_CONTINUOUS_BASE_STRATEGY_INVALID")
    if any(
        strategy["kwargs"].get(k)
        for k in ("enable_sector_hmm", "enable_hmm_risk_gate", "sector_risk_overlay_enabled", "qe_l2_risk_enabled")
    ):
        raise ValueError("QE_L2_CONTINUOUS_OTHER_OVERLAY_FORBIDDEN")
    if (manifest["start"], manifest["end"], manifest["decision_days"]) != ("2026-04-01", "2026-09-30", 125):
        raise ValueError("QE_L2_CONTINUOUS_WINDOW_INVALID")
    if (
        str(conf["task"]["dataset"]["kwargs"]["segments"]["test"][0]) != "2026-03-31"
        or str(backtest["end_time"]) != "2026-09-30"
    ):
        raise ValueError("QE_L2_CONTINUOUS_SIGNAL_OR_TERMINAL_INVALID")
    if backtest["account"] != 100_000_000 or backtest["exchange_kwargs"]["freq"] != "1min":
        raise ValueError("QE_L2_CONTINUOUS_EXECUTION_CONTRACT_INVALID")
    if executor["class"] != "NestedExecutor" or executor["kwargs"]["inner_executor"]["class"] != "SimulatorExecutor":
        raise ValueError("QE_L2_CONTINUOUS_EXECUTOR_INVALID")
    backtest["start_time"] = "2026-04-01"
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    executor["kwargs"]["inner_executor"].update(
        {"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"}
    )
    strategy.update({"class": "L2ContinuousScoreWeightedTopkStrategyV2", "module_path": "qe_l2_continuous_consumer"})
    strategy["kwargs"]["qe_l2_continuous_manifest_file"] = MANIFEST
    files["conf.yaml"] = yaml.safe_dump(conf, sort_keys=False, allow_unicode=True)
    files[MANIFEST] = json.dumps(manifest, sort_keys=True, ensure_ascii=False, indent=2, allow_nan=False)
    for name in [
        "qe_l2_continuous_consumer.py",
        "qe_l2_continuous_admission.py",
        "qe_l2_risk_consumer.py",
        "qe_l2_risk_admission.py",
        "qe_terminal_interval_executor.py",
    ]:
        files[name] = (Path(scripts_dir) / name).read_text(encoding="utf-8")
    for key in ["wsl_command", "wsl_command_core"]:
        if result.get(key):
            command, count = re.subn(
                r"(?<!\S)python qrun_limit_minute\.py",
                f"python qe_l2_continuous_consumer.py {MANIFEST} --config conf.yaml --prediction frozen_prediction.pkl && python qrun_limit_minute.py",
                result[key],
            )
            if count != 1:
                raise ValueError("QE_L2_CONTINUOUS_RUNNER_INVALID")
            result[key] = command
    return result
