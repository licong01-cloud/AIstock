"""Control-plane compose for the independently approved L2 entry-gate study."""
from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import re

import yaml

CONTRACT = "QE-L2-RISK-ENTRY-GATE-D1..D7"
MANIFEST_FILE = "qe_l2_risk_manifest.json"
STUDY_PARAM = "qe_l2_study_manifest"
APPROVED_RISK_SHA256 = "5e6a2784a304b2a34cb8cbf6859aab0a9c154a29ba5fac246ff35eebcd9ca0e5"


def bind_execution_metadata(params, conf):
    """Register the actual composed execution contract, before reserving a run."""
    from backend.services.quantevolver.runtime_contract import merge_qe_minute_runtime_contract

    port = conf["port_analysis_config"]
    exchange = port["backtest"]["exchange_kwargs"]
    inner = port["executor"]["kwargs"]["inner_strategy"]
    strategy = port["strategy"]["kwargs"]
    if (exchange.get("freq") != "1min"
            or exchange.get("limit_threshold") != ["$limit_up", "$limit_down"]
            or inner.get("class") != "TailTWAPWithLimitStrategy"
            or inner.get("module_path") != "tail_twap_strategy"
            or not strategy.get("suspend_filter_strict")):
        raise ValueError("QE_L2_EXECUTION_REGISTRATION_EVIDENCE_INVALID")
    result = merge_qe_minute_runtime_contract(
        params, config={"backtest_freq": exchange["freq"]},
        execution_algo="TWAP", execution_algo_params=inner["kwargs"],
        source="qe_l2_frozen_composed_config", require_minute=True)
    result["limit_suspend_authoritative"] = True
    return result


def extract_study(custom_params):
    params = dict(custom_params or {})
    enabled = params.pop("qe_l2_risk_enabled", False)
    if type(enabled) is not bool:
        raise ValueError("QE_L2_ENABLED_INVALID")
    manifest = params.pop(STUDY_PARAM, None)
    if manifest is None:
        if enabled:
            raise ValueError("QE_L2_MANIFEST_REQUIRED")
        return params, None
    if not isinstance(manifest, dict) or manifest.get("contract") != CONTRACT:
        raise ValueError("QE_L2_CONTRACT_INVALID")
    if any(params.get(k) for k in ("enable_sector_hmm", "enable_hmm_risk_gate", "sector_risk_overlay_enabled")):
        raise ValueError("QE_L2_OTHER_OVERLAY_FORBIDDEN")
    if (manifest.get("start"), manifest.get("end"), manifest.get("decision_days")) != ("2026-04-01", "2026-09-30", 125):
        raise ValueError("QE_L2_WINDOW_INVALID")
    if manifest.get("policy") != "qe_l2_risk_entry_gate_v1" or manifest.get("threshold") != 0.20:
        raise ValueError("QE_L2_POLICY_INVALID")
    if not {"risk", "mapping", "mapping_receipt", "authority", "prediction", "calendar", "pit"}.issubset(manifest.get("assets", {})):
        raise ValueError("QE_L2_ASSETS_MISSING")
    if manifest["assets"]["risk"].get("sha256") != APPROVED_RISK_SHA256:
        raise ValueError("QE_L2_UNAPPROVED_RISK_ASSET")
    return params, {"enabled": enabled, "manifest": deepcopy(manifest)}


def apply_study(result, study, *, scripts_dir):
    """Route only an explicit study; legacy composition remains byte-for-byte."""
    if study is None:
        return result
    approved = deepcopy(study["manifest"].get("dataset_binding"))
    observed = deepcopy(result.get("direct_v2_dataset_binding"))
    if not isinstance(approved, dict) or not isinstance(observed, dict):
        raise ValueError("QE_L2_EXECUTION_BINDING_MISSING")
    # Coverage receipts sign requested windows; all physical/PIT pins stay fixed.
    for binding in (approved, observed):
        binding.get("selection_pins", {}).pop("coverage_receipt_sha256", None)
    if approved != observed:
        raise ValueError("QE_L2_EXECUTION_BINDING_MISMATCH")
    files = result["experiment_files"]
    conf = yaml.safe_load(files["conf.yaml"])
    if conf["qlib_init"]["provider_uri"] != {
        "day": approved["provider_uri_day"], "1min": approved["provider_uri_1min"]
    } or conf.get("market") != approved["selection_pins"]["instrument_name"]:
        raise ValueError("QE_L2_EXECUTION_PROVIDER_MISMATCH")
    port = conf["port_analysis_config"]
    strategy = port["strategy"]
    if (strategy["class"], strategy["module_path"]) != (
        "SuspendFilterScoreWeightedTopkStrategyV2", "qe_suspend_filter_score_weighted_strategy"
    ):
        raise ValueError("QE_L2_BASE_STRATEGY_INVALID")
    if any(strategy["kwargs"].get(k) for k in ("enable_sector_hmm", "enable_hmm_risk_gate", "sector_risk_overlay_enabled")):
        raise ValueError("QE_L2_OTHER_OVERLAY_FORBIDDEN")
    if str(conf["task"]["dataset"]["kwargs"]["segments"]["test"][0]) != "2026-03-31":
        raise ValueError("QE_L2_FIRST_SIGNAL_DATE_MISSING")
    if str(port["backtest"]["end_time"]) != "2026-09-30":
        raise ValueError("QE_L2_TERMINAL_DATE_MISMATCH")
    port["backtest"]["start_time"] = "2026-04-01"
    if port["backtest"]["account"] != 100_000_000 or port["backtest"]["exchange_kwargs"]["freq"] != "1min":
        raise ValueError("QE_L2_ACCOUNT_OR_FREQUENCY_INVALID")
    executor = port["executor"]
    inner = executor["kwargs"]["inner_executor"]
    if executor["class"] != "NestedExecutor" or inner["class"] != "SimulatorExecutor":
        raise ValueError("QE_L2_EXECUTOR_INVALID")
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    inner.update({"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"})
    if study["enabled"]:
        strategy.update({"class": "L2RiskEntryGateScoreWeightedTopkStrategyV2", "module_path": "qe_l2_risk_consumer"})
        strategy["kwargs"].update({"qe_l2_risk_enabled": True, "qe_l2_manifest_file": MANIFEST_FILE})
    files["conf.yaml"] = yaml.safe_dump(conf, sort_keys=False, allow_unicode=True)
    files[MANIFEST_FILE] = json.dumps(study["manifest"], ensure_ascii=False, sort_keys=True, indent=2)
    for name in ("qe_l2_risk_consumer.py", "qe_terminal_interval_executor.py"):
        files[name] = (Path(scripts_dir) / name).read_text(encoding="utf-8")
    for key in ("wsl_command", "wsl_command_core"):
        command = result.get(key)
        if not command:
            continue
        command, count = re.subn(r"(?<!\S)python qrun_limit_minute\.py",
                                f"python qe_l2_risk_consumer.py {MANIFEST_FILE} --config conf.yaml"
                                " --prediction frozen_prediction.pkl && python qrun_limit_minute.py", command)
        if count != 1:
            raise ValueError("QE_L2_RUNNER_COMMAND_INVALID")
        result[key] = command
    return result
