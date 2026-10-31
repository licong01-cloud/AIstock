"""Optional, exact-source natural-entry budget hook and formal Composer binding."""

from copy import deepcopy
import ast
import base64
import hashlib
import json
from pathlib import Path
import re
import yaml
from scripts.qe_l2_natural_entry_admission import BASE_SOURCE_SHA, validate_admission
from scripts.qe_l2_risk_admission import canonical_sha, physical_binding
from .l2_relative_consumer import EXECUTION_PORT_SHA

PARAM = "qe_l2_natural_entry_manifest"
MANIFEST = "qe_l2_natural_entry_manifest.json"
WORKERS = (
    "qe_l2_natural_entry_admission.py",
    "qe_l2_natural_entry_consumer.py",
    "qe_l2_natural_entry_strategy.py",
    "qe_l2_relative_admission.py",
    "qe_l2_relative_consumer.py",
    "qe_l2_continuous_consumer.py",
    "qe_l2_continuous_admission.py",
    "qe_l2_risk_consumer.py",
    "qe_l2_risk_admission.py",
    "qe_terminal_interval_executor.py",
)


def extract_natural_entry(params):
    params = dict(params or {})
    manifest = params.pop(PARAM, None)
    if manifest is None:
        return params, None
    if any(
        params.get(k)
        for k in (
            "qe_terminal_replay",
            "qe_l2_relative_manifest",
            "qe_l2_continuous_manifest",
            "qe_l2_study_manifest",
            "qe_stock_probability_study",
            "qe_l2_risk_enabled",
            "enable_sector_hmm",
            "enable_hmm_risk_gate",
            "sector_risk_overlay_enabled",
        )
    ):
        raise ValueError("QE_L2_NATURAL_OTHER_OVERLAY_FORBIDDEN")
    validate_admission(manifest)
    return params, deepcopy(manifest)


def bind_natural_entry_hook(source):
    source = source.replace("\r\n", "\n")
    digest = hashlib.sha256(source.encode()).hexdigest()
    if digest != BASE_SOURCE_SHA:
        raise ValueError("QE_L2_NATURAL_BASE_SOURCE_DRIFT")
    tree = ast.parse(source)
    original = ast.dump(tree, include_attributes=False)
    classes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ScoreWeightedTopkStrategyV2"]
    methods = [n for n in classes[0].body if isinstance(n, ast.FunctionDef) and n.name == "generate_trade_decision"]
    method = methods[0]
    sites = [i for i, n in enumerate(method.body) if isinstance(n, ast.Assign) and ast.unparse(n) == "buy_orders = []"]
    if len(sites) != 1:
        raise ValueError("QE_L2_NATURAL_BUDGET_SITE")
    insertion = ast.parse("""
_qe_base_budgets = {sid: min(total_account_value * weight_map[sid], self.max_single_order_value) for sid in actual_buys}
_qe_entry_budgets = self._qe_adjust_natural_entry(
    decision_date=trade_start_time,
    ordered_targets=[str(s) for s in ranked.index if s in weight_map],
    natural_new_buys=list(actual_buys), base_budgets_cny=_qe_base_budgets,
    nav_cny=total_account_value, held_before=current_holdings)
""").body
    buy_loops = [n for n in method.body if isinstance(n, ast.For) and ast.unparse(n.iter) == "actual_buys"]
    if len(buy_loops) != 1:
        raise ValueError("QE_L2_NATURAL_BUY_LOOP")
    buy_loop = buy_loops[0]
    pairs = [
        i
        for i in range(len(buy_loop.body) - 1)
        if ast.unparse(buy_loop.body[i]) == "target_value = total_account_value * w"
        and ast.unparse(buy_loop.body[i + 1]) == "target_value = min(target_value, self.max_single_order_value)"
    ]
    if len(pairs) != 1:
        raise ValueError("QE_L2_NATURAL_TARGET_VALUE_SITE")
    pos = pairs[0]
    originals = deepcopy(buy_loop.body[pos : pos + 2])
    buy_loop.body[pos : pos + 2] = ast.parse("target_value = _qe_entry_budgets[sid]").body
    method.body[sites[0] : sites[0]] = insertion
    adapted = ast.unparse(ast.fix_missing_locations(tree)) + "\n"
    # Remove only the two new statements and restore only the original value.
    del method.body[sites[0] : sites[0] + len(insertion)]
    buy_loop.body[pos : pos + 1] = originals
    if ast.dump(tree, include_attributes=False) != original:
        raise ValueError("QE_L2_NATURAL_BASE_AST_DRIFT")
    return adapted, {
        "original_source_sha256": digest,
        "base_ast_restored": True,
        "adapted_source_sha256": hashlib.sha256(adapted.encode()).hexdigest(),
        "budget_hooks": 1,
        "new_order_generators": 0,
    }


def apply_natural_entry(result, manifest, *, scripts_dir):
    if manifest is None:
        return result
    validate_admission(manifest)
    m = deepcopy(manifest)
    binding = m["dataset_binding"]
    observed = result.get("direct_v2_dataset_binding")
    if not isinstance(observed, dict) or physical_binding(binding) != physical_binding(observed):
        raise ValueError("QE_L2_NATURAL_EXECUTION_BINDING")
    if set(m["assets"]) != {"loss", "prediction", "mapping", "mapping_receipt", "authority", "calendar", "pit"}:
        raise ValueError("QE_L2_NATURAL_ASSET_FIELDS")
    if (
        m["assets"]["calendar"]["sha256"] != binding["day_pins"]["calendar_sha256"]
        or m["assets"]["pit"]["sha256"] != binding["selection_pins"]["instruments_sha256"]
    ):
        raise ValueError("QE_L2_NATURAL_TRANSPORT_PIN")
    files = result["experiment_files"]
    for key, ref in m["assets"].items():
        if key == "prediction":
            ref["path"] = "frozen_prediction.pkl"
            continue
        if key in ("calendar", "pit") or (key == "loss" and m["arm"] == "B0"):
            continue
        raw = Path(ref["path"]).read_bytes()
        if hashlib.sha256(raw).hexdigest() != ref["sha256"]:
            raise ValueError("QE_L2_NATURAL_TRANSPORT_HASH: " + key)
        dest = "qe_l2_natural_entry_" + key + Path(ref["path"]).suffix
        files[dest + ".b64"] = base64.b64encode(raw).decode("ascii")
        ref["path"] = dest
    conf = yaml.safe_load(files["conf.yaml"])
    if (
        conf["qlib_init"]["provider_uri"] != {"day": binding["provider_uri_day"], "1min": binding["provider_uri_1min"]}
        or conf["market"] != binding["selection_pins"]["instrument_name"]
    ):
        raise ValueError("QE_L2_NATURAL_PROVIDER")
    port = conf["port_analysis_config"]
    strategy, backtest, executor = port["strategy"], port["backtest"], port["executor"]
    if (strategy["class"], strategy["module_path"]) != (
        "SuspendFilterScoreWeightedTopkStrategyV2",
        "qe_suspend_filter_score_weighted_strategy",
    ):
        raise ValueError("QE_L2_NATURAL_BASE_STRATEGY")
    if (
        (m["start"], m["end"], m["decision_days"]) != ("2026-04-01", "2026-09-30", 125)
        or str(conf["task"]["dataset"]["kwargs"]["segments"]["test"][0]) != "2026-03-31"
        or str(backtest["end_time"]) != "2026-09-30"
        or backtest["account"] != 100_000_000
        or backtest["exchange_kwargs"]["freq"] != "1min"
        or executor["class"] != "NestedExecutor"
        or executor["kwargs"]["inner_executor"]["class"] != "SimulatorExecutor"
    ):
        raise ValueError("QE_L2_NATURAL_EXECUTION_CONTRACT")
    files["custom_strategy.py"], receipt = bind_natural_entry_hook(files["custom_strategy.py"])
    files["qe_l2_natural_entry_base_adapter.json"] = json.dumps(receipt, sort_keys=True)
    for name in WORKERS:
        files[name] = (Path(scripts_dir) / name).read_text(encoding="utf-8")
    code = {
        name: hashlib.sha256(content.encode()).hexdigest()
        for name, content in sorted(files.items())
        if name.endswith(".py") and name != "prepare_factors.py"
    }
    backtest["start_time"] = "2026-04-01"
    executor.update({"class": "BoundedTerminalNestedExecutor", "module_path": "qe_terminal_interval_executor"})
    executor["kwargs"]["inner_executor"].update(
        {"class": "BoundedTerminalSimulatorExecutor", "module_path": "qe_terminal_interval_executor"}
    )
    # Verify original C1 trading parameters against the reviewed execution pin.
    check = deepcopy(port)
    check["strategy"].update(
        {"class": "L2RelativeScoreWeightedTopkStrategyV2", "module_path": "qe_l2_relative_strategy"}
    )
    check["strategy"]["kwargs"]["qe_l2_relative_manifest_file"] = "qe_l2_relative_manifest.json"
    if canonical_sha(json.loads(json.dumps(check, default=lambda x: x.isoformat()))) != EXECUTION_PORT_SHA:
        raise ValueError("QE_L2_NATURAL_BASE_EXECUTION_DRIFT")
    strategy.update(
        {"class": "L2NaturalEntryScoreWeightedTopkStrategyV2", "module_path": "qe_l2_natural_entry_strategy"}
    )
    strategy["kwargs"]["qe_l2_natural_entry_manifest_file"] = MANIFEST
    execution_contract = json.loads(
        json.dumps(
            {k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}, default=lambda x: x.isoformat()
        )
    )
    m["execution_sha256"] = canonical_sha({"files": code, "contract": execution_contract})
    m["execution_files_sha256"] = code
    m["execution_contract"] = execution_contract
    files["conf.yaml"] = yaml.safe_dump(conf, sort_keys=False, allow_unicode=True)
    files[MANIFEST] = json.dumps(m, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
    for key in ("wsl_command", "wsl_command_core"):
        if result.get(key):
            command, count = re.subn(
                r"(?<!\S)python qrun_limit_minute\.py",
                f"python qe_l2_natural_entry_consumer.py {MANIFEST} --config conf.yaml --prediction frozen_prediction.pkl && python qrun_limit_minute.py",
                result[key],
            )
            if count != 1:
                raise ValueError("QE_L2_NATURAL_RUNNER")
            result[key] = command
    return result
