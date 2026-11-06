"""Versioned QE stock-probability admission and optional Composer integration."""
from __future__ import annotations

import base64
import ast
from copy import deepcopy
import json
from pathlib import Path
import re

import yaml

from scripts.qe_stock_probability_gate import (
    MANIFEST_FILE, RISK_FILE, require, sha256, validate_manifest,
)

STUDY_PARAM = "qe_stock_probability_study"
ENABLED_PARAM = "qe_stock_probability_enabled"


def bind_buy_candidate_hook(source):
    """Add only an identity extension to the actual selected catalog source."""
    tree = ast.parse(source)
    classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ScoreWeightedTopkStrategyV2"]
    require(len(classes) == 1, "BASE_SOURCE_UNSUPPORTED")
    cls = classes[0]
    methods = {n.name: n for n in cls.body if isinstance(n, ast.FunctionDef)}
    decision = methods.get("generate_trade_decision")
    require(decision is not None, "BASE_DECISION_MISSING")
    calls = [n for n in ast.walk(decision) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and n.func.attr == "_prepare_buy_candidates"]
    if "_prepare_buy_candidates" in methods:
        hook = methods["_prepare_buy_candidates"]
        body = [n for n in hook.body if not (isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant))]
        require(len(calls) == 1 and len(body) == 1 and isinstance(body[0], ast.Return)
                and isinstance(body[0].value, ast.Name) and body[0].value.id == "buy_candidates",
                "BASE_HOOK_NOT_IDENTITY")
        return source
    require(not calls, "BASE_HOOK_CONFLICT")
    anchors = [n for n in decision.body if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call)
               and isinstance(n.value.func, ast.Attribute) and n.value.func.attr == "sort"
               and isinstance(n.value.func.value, ast.Name) and n.value.func.value.id == "buy_candidates"]
    require(len(anchors) == 1, "BASE_BUY_ANCHOR_UNSUPPORTED")
    lines = source.splitlines(keepends=True)
    call = "        buy_candidates = self._prepare_buy_candidates(ranked, current_holdings, buy_candidates)\n"
    lines.insert(anchors[0].end_lineno, call)
    method = ("    def _prepare_buy_candidates(self, ranked, current_holdings, buy_candidates):\n"
              "        return buy_candidates\n\n")
    lines.insert(decision.lineno - 1, method)
    result = "".join(lines)
    # Prove that removing exactly the extension restores the original AST.
    observed = ast.parse(result)
    target = next(n for n in observed.body if isinstance(n, ast.ClassDef) and n.name == cls.name)
    target.body = [n for n in target.body if not isinstance(n, ast.FunctionDef) or n.name != "_prepare_buy_candidates"]
    body = next(n for n in target.body if isinstance(n, ast.FunctionDef) and n.name == "generate_trade_decision")
    body.body = [n for n in body.body if not (isinstance(n, ast.Assign) and isinstance(n.value, ast.Call)
                 and isinstance(n.value.func, ast.Attribute) and n.value.func.attr == "_prepare_buy_candidates")]
    require(ast.dump(observed) == ast.dump(tree), "BASE_AST_CHANGED")
    return result


def extract_stock_probability(custom_params):
    params = dict(custom_params or {})
    enabled = params.pop(ENABLED_PARAM, False)
    require(type(enabled) is bool, "ENABLED_INVALID")
    study = params.pop(STUDY_PARAM, None)
    if study is None:
        require(not enabled, "MANIFEST_REQUIRED")
        return params, None
    require(isinstance(study, dict) and set(study) == {"manifest", "risk_source"}, "STUDY_INVALID")
    validate_manifest(study["manifest"])
    require(not any(params.get(k) for k in ("enable_sector_hmm", "enable_hmm_risk_gate",
        "sector_risk_overlay_enabled", "qe_l2_study_manifest", "qe_l2_continuous_manifest")), "OTHER_OVERLAY_FORBIDDEN")
    source = Path(study["risk_source"])
    require(source.is_file() and sha256(source) == study["manifest"]["risk"]["sha256"], "RISK_HASH_MISMATCH")
    return params, {"enabled": enabled, **deepcopy(study)}


def apply_stock_probability(result, study, *, scripts_dir):
    if study is None:
        return result
    m = study["manifest"]
    validate_manifest(m)
    expected, actual = deepcopy(m["dataset_binding"]), deepcopy(result.get("direct_v2_dataset_binding"))
    require(isinstance(actual, dict), "EXECUTION_BINDING_MISSING")
    # Only the requested-window coverage receipt may differ; physical and PIT pins may not.
    for binding in (expected, actual):
        binding.get("selection_pins", {}).pop("coverage_receipt_sha256", None)
    require(expected == actual, "EXECUTION_BINDING_MISMATCH")
    files = result["experiment_files"]
    conf = yaml.safe_load(files["conf.yaml"])
    port = conf["port_analysis_config"]
    strategy = port["strategy"]
    classes = {
        ("ScoreWeightedTopkStrategyV2", "custom_strategy"): "StockProbabilityScoreWeightedTopkStrategyV2",
        ("SuspendFilterScoreWeightedTopkStrategyV2", "qe_suspend_filter_score_weighted_strategy"):
            "StockProbabilitySuspendFilterScoreWeightedTopkStrategyV2",
    }
    key = (strategy["class"], strategy["module_path"])
    require(key in classes, "BASE_STRATEGY_UNSUPPORTED")
    require(not any(strategy["kwargs"].get(k) for k in ("enable_sector_hmm", "enable_hmm_risk_gate",
                                                       "sector_risk_overlay_enabled")), "OTHER_OVERLAY_FORBIDDEN")
    require(str(port["backtest"]["start_time"]) == m["start"]
            and str(port["backtest"]["end_time"]) == m["end"], "EXECUTION_WINDOW_MISMATCH")
    require(conf["qlib_init"]["provider_uri"] == {"day": expected["provider_uri_day"],
            "1min": expected["provider_uri_1min"]}
            and conf.get("market") == expected["selection_pins"]["instrument_name"], "EXECUTION_PROVIDER_MISMATCH")
    # The risk reader verifies source bytes again immediately before execution.
    risk_bytes = Path(study["risk_source"]).read_bytes()
    import hashlib
    require(hashlib.sha256(risk_bytes).hexdigest() == m["risk"]["sha256"], "RISK_HASH_MISMATCH")
    require(not any(name in files for name in (MANIFEST_FILE, RISK_FILE, RISK_FILE + ".b64")), "FILE_COLLISION")
    files[MANIFEST_FILE] = json.dumps(m, sort_keys=True, ensure_ascii=False, indent=2)
    files[RISK_FILE + ".b64"] = base64.b64encode(risk_bytes).decode("ascii")
    scripts = Path(scripts_dir)
    for name in ("qe_stock_probability_gate.py", "qe_stock_probability_gate_strategy.py"):
        require(name not in files, "FILE_COLLISION")
        files[name] = (scripts / name).read_text(encoding="utf-8")
    require("custom_strategy.py" in files, "SELECTED_BASE_SOURCE_MISSING")
    files["custom_strategy.py"] = bind_buy_candidate_hook(files["custom_strategy.py"])
    if study["enabled"]:
        strategy.update({"class": classes[key], "module_path": "qe_stock_probability_gate_strategy"})
        strategy["kwargs"].update({ENABLED_PARAM: True, "qe_stock_probability_manifest_file": MANIFEST_FILE})
        files["conf.yaml"] = yaml.safe_dump(conf, sort_keys=False, allow_unicode=True)
    for key in ("wsl_command", "wsl_command_core"):
        if not result.get(key):
            continue
        command, count = re.subn(r"(?<!\S)python qrun_limit_minute\.py",
            f"python qe_stock_probability_gate.py {MANIFEST_FILE} --config conf.yaml"
            " --prediction frozen_prediction.pkl && python qrun_limit_minute.py", result[key])
        require(count == 1, "REPLAY_COMMAND_INVALID")
        result[key] = command
    return result
