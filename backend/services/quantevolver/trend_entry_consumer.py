"""Opt-in post-decision trend consumer; the catalog strategy remains immutable."""
from copy import deepcopy
import ast
import base64
import hashlib
import json
from pathlib import Path

import yaml

from scripts.qe_trend_entry_admission import (
    BASE_SOURCE_SHA, MANIFEST_FILE, MAX_ASSET_BYTES, PANEL_FILE, canonical_sha,
    physical_binding, require, seal, sha256, validate_manifest,
)

PARAM = "qe_trend_entry_manifest"
WORKERS = ("qe_trend_entry_admission.py", "qe_trend_entry_strategy.py")
BASE_DEPENDENCIES = {
    "score_weighted_strategy.py": "407c3ef80ff8dade6a5807b5ce8935c3015dcda0ccce35d6cd837f979f86bc53",
    "qe_suspend_filter_score_weighted_strategy.py": "46aa3aaa8ef2b7bf24b2e5f9a405c7f1c970f887e93feb633addc076d3c98cd3",
    "tail_twap_strategy.py": "7ff0d383106244b1d9e99ae2445db78bf6e259f781973458dae832d4a81f0e4c",
}


def extract_trend_entry(params):
    params = dict(params or {})
    manifest = params.pop(PARAM, None)
    asset_path = params.pop("qe_trend_entry_panel_path", None)
    if manifest is None:
        require(asset_path is None, "ORPHAN_ASSET_PATH")
        return params, None
    forbidden = ("qe_terminal_replay", "qe_l2_natural_entry_manifest", "qe_l2_relative_manifest",
                 "qe_l2_continuous_manifest", "qe_l2_study_manifest", "qe_stock_probability_study",
                 "qe_l2_risk_enabled", "enable_sector_hmm", "enable_hmm_risk_gate", "sector_risk_overlay_enabled")
    require(not any(params.get(key) for key in forbidden), "OTHER_OVERLAY_FORBIDDEN")
    validate_manifest(manifest)
    require(isinstance(asset_path, str) and bool(asset_path), "ASSET_PATH_REQUIRED")
    return params, (deepcopy(manifest), asset_path)


def bind_trend_entry_hooks(source):
    source = source.replace("\r\n", "\n")
    require(hashlib.sha256(source.encode()).hexdigest() == BASE_SOURCE_SHA, "BASE_SOURCE_DRIFT")
    tree = ast.parse(source)
    before = ast.dump(tree, include_attributes=False)
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "ScoreWeightedTopkStrategyV2")
    method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "generate_trade_decision")
    sites = []
    # Insert exactly three observational calls. Original calculations and return stay intact.
    def insert_after(body, predicate, code):
        found = [i for i, n in enumerate(body) if predicate(n)]
        require(len(found) == 1, "AST_SITE")
        node = ast.parse(code).body[0]
        body.insert(found[0] + 1, node)
        sites.append((body, node))

    insert_after(method.body,
                 lambda n: isinstance(n, ast.Assign) and ast.unparse(n.targets[0]) == "all_pred_scores",
                 "self._qe_check_trend_prediction(all_pred_scores)")
    buy_loop = next(n for n in method.body if isinstance(n, ast.For) and ast.unparse(n.iter) == "actual_buys")
    formed = [n for n in buy_loop.body if isinstance(n, ast.If) and ast.unparse(n.test) == "buy_amount > 0"]
    require(len(formed) == 1, "FORMED_ORDER_SITE")
    insert_after(formed[0].body, lambda n: isinstance(n, ast.Expr),
                 "self._qe_record_trend_slot(sid, buy_amount, buy_price, factor, target_value, amount_unit)")
    capture = ast.parse("""self._qe_capture_trend_context(
        ranked, scores, current_holdings, valid_holdings, actual_sells, actual_buys,
        all_sells, trade_step, trade_start_time, trade_end_time,
        self._diag_stats['threshold'])""").body[0]
    require(isinstance(method.body[-1], ast.Return)
            and ast.unparse(method.body[-1].value) == "TradeDecisionWO(sell_orders + buy_orders, self)", "FINAL_RETURN")
    method.body.insert(len(method.body) - 1, capture)
    sites.append((method.body, capture))
    adapted = ast.unparse(ast.fix_missing_locations(tree)) + "\n"
    for body, node in reversed(sites):
        body.remove(node)
    require(ast.dump(tree, include_attributes=False) == before, "BASE_AST_DRIFT")
    return adapted, {"original_sha256": BASE_SOURCE_SHA, "restored_ast_equal": True,
                     "observation_hooks": 3, "adapted_sha256": hashlib.sha256(adapted.encode()).hexdigest()}


def apply_trend_entry(result, study, *, scripts_dir):
    if study is None:
        return result
    manifest, asset_path = study
    m = deepcopy(validate_manifest(manifest))
    require(physical_binding(m["dataset_binding"]) == physical_binding(result.get("direct_v2_dataset_binding", {})),
            "COMPOSER_BINDING")
    asset = Path(asset_path)
    require(asset.stat().st_size <= MAX_ASSET_BYTES and sha256(asset) == m["panel"]["sha256"], "TRANSPORT_HASH")
    files = result["experiment_files"]
    conf = yaml.safe_load(files["conf.yaml"])
    port = conf["port_analysis_config"]
    require(canonical_sha(json.loads(json.dumps(port, default=str))) == m["original_port_sha256"], "ORIGINAL_EXECUTION_DRIFT")
    b = m["dataset_binding"]
    require(conf["qlib_init"]["provider_uri"] == {"day": b["provider_uri_day"], "1min": b["provider_uri_1min"]}
            and conf["market"] == b["selection_pins"]["instrument_name"], "PROVIDER")
    strategy, backtest = port["strategy"], port["backtest"]
    require((strategy["class"], strategy["module_path"]) ==
            ("SuspendFilterScoreWeightedTopkStrategyV2", "qe_suspend_filter_score_weighted_strategy"), "STRATEGY")
    kw = strategy["kwargs"]
    require(kw.get("topk") == 20 and kw.get("min_n_drop") == 0 and kw.get("enable_dynamic_ndrop") is True
            and backtest["account"] == 10_000_000, "BASE_POLICY")
    require(str(backtest["start_time"])[:10] == m["account"]["start"]
            and str(backtest["end_time"])[:10] == m["account"]["end"], "BACKTEST_WINDOW")
    inner = port["executor"]["kwargs"]["inner_strategy"]
    require((inner["class"], inner["module_path"], inner["kwargs"].get("unfilled_handler")) ==
            ("TailTWAPWithLimitStrategy", "tail_twap_strategy", "TAIL_BOOST"), "INNER_EXECUTION")
    for name, digest in BASE_DEPENDENCIES.items():
        require(name in files and hashlib.sha256(files[name].replace("\r\n", "\n").encode()).hexdigest() == digest,
                "BASE_DEPENDENCY_DRIFT:" + name)
    files["custom_strategy.py"], m["source_adaptation"] = bind_trend_entry_hooks(files["custom_strategy.py"])
    for name in WORKERS:
        files[name] = (Path(scripts_dir) / name).read_text(encoding="utf-8")
    files[PANEL_FILE + ".b64"] = base64.b64encode(asset.read_bytes()).decode("ascii")
    strategy.update({"class": "TrendEntryScoreWeightedTopkStrategyV2", "module_path": "qe_trend_entry_strategy"})
    kw["qe_trend_entry_manifest_file"] = MANIFEST_FILE
    # PortAnaRecord may be a YAML alias or an independent copy. Both must consume the same contract.
    for record in conf.get("task", {}).get("record", []):
        if record.get("class") == "PortAnaRecord":
            record["kwargs"]["config"] = deepcopy(port)
    m["execution_files_sha256"] = {
        name: hashlib.sha256(files[name].replace("\r\n", "\n").encode()).hexdigest()
        for name in (*BASE_DEPENDENCIES, *WORKERS, "custom_strategy.py")
    }
    m["execution_contract_sha256"] = canonical_sha(json.loads(json.dumps(
        {k: conf[k] for k in ("port_analysis_config", "qlib_init", "market")}, default=str)))
    files["conf.yaml"] = yaml.safe_dump(conf, allow_unicode=True, sort_keys=False)
    files[MANIFEST_FILE] = json.dumps(seal(m), ensure_ascii=False, indent=2, allow_nan=False)
    return result
