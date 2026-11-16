"""Save ordinary Qlib account artifacts without inventing pred.pkl or a model."""
from __future__ import annotations

import json
from pathlib import Path

from qe_absolute_targets import MANIFEST_FILE, RESULT_FILE, SCHEMA, file_sha256, require


def run_absolute_account(config, recorder):
    import pandas as pd
    from qlib.backtest import backtest
    from qlib.contrib.evaluate import indicator_analysis, risk_analysis
    from qlib.utils import flatten_dict

    contract = json.loads(Path(MANIFEST_FILE).read_text(encoding="utf-8"))
    require(config.get("absolute_target_replay") == contract, "QE_ABSOLUTE_TARGET_CONFIG_DRIFT")
    require(not config.get("task", {}).get("model") and not config.get("task", {}).get("dataset"),
            "QE_ABSOLUTE_TARGET_MODEL_IN_RUNNER")
    port = config["port_analysis_config"]
    portfolios, indicators = backtest(executor=port["executor"], strategy=port["strategy"], **port["backtest"])
    require("1day" in portfolios, "QE_ABSOLUTE_TARGET_DAILY_ACCOUNT_MISSING")
    report, positions = portfolios["1day"]
    w = contract["window"]
    days = [str(d.date()) for d in report.index]
    require(len(days) == w["account_days"] and days[0] == w["account_start"] and days[-1] == w["account_end"]
            and len(set(days)) == len(days), "QE_ABSOLUTE_TARGET_ACCOUNT_BOUNDARY")
    actions = [json.loads(line) for line in Path("qe_absolute_target_actions.jsonl").read_text(encoding="utf-8").splitlines()]
    require([r["trade_date"] for r in actions] == days, "QE_ABSOLUTE_TARGET_CONSUMPTION_BOUNDARY")
    # Persist actual account observations before derived analysis. A later
    # analysis error must not discard a completed, potentially costly replay.
    raw = {}
    for freq, (frame, held) in portfolios.items():
        raw[f"report_normal_{freq}.pkl"] = frame
        raw[f"positions_normal_{freq}.pkl"] = held
    for freq, (frame, obj) in indicators.items():
        raw[f"indicators_normal_{freq}.pkl"] = frame
        raw[f"indicators_normal_{freq}_obj.pkl"] = obj
    recorder.save_objects(artifact_path="portfolio_analysis", **raw)
    artifacts, indicator_availability = {}, {}
    for freq, (frame, held) in portfolios.items():
        analysis = pd.concat({
            "excess_return_without_cost": risk_analysis(frame["return"] - frame["bench"], freq=freq),
            "excess_return_with_cost": risk_analysis(frame["return"] - frame["bench"] - frame["cost"], freq=freq),
        })
        artifacts[f"port_analysis_{freq}.pkl"] = analysis
        recorder.log_metrics(**{f"{freq}.{k}": v for k, v in flatten_dict(analysis["risk"].unstack().T.to_dict()).items()})
    for freq, (frame, obj) in indicators.items():
        if frame.empty:
            # Disabled inner metrics and genuine no-order periods have no
            # observations. Preserve missingness; never fabricate count/rates.
            artifacts[f"indicator_analysis_{freq}.pkl"] = pd.DataFrame({"value": pd.Series(dtype=float)})
            indicator_availability[freq] = {"available": False, "reason": "NO_ORDER_OBSERVATIONS"}
            continue
        require({"count", "deal_amount", "value", "ffr", "pa", "pos"} <= set(frame.columns),
                "QE_ABSOLUTE_TARGET_INDICATOR_COLUMNS")
        analysis = indicator_analysis(frame)
        indicator_availability[freq] = {"available": True, "reason": "OBSERVED_ORDER_INDICATORS"}
        artifacts[f"indicator_analysis_{freq}.pkl"] = analysis
        recorder.log_metrics(**{f"{freq}.{k}": v for k, v in analysis["value"].to_dict().items()})
    recorder.save_objects(artifact_path="portfolio_analysis", **artifacts)
    result = {"schema_version": SCHEMA, "status": "COMPLETE_ACCOUNT_AND_TARGET_CONSUMPTION",
              "arm": contract["arm"], "targets_sha256": file_sha256("absolute_targets.parquet"),
              "dataset": contract["dataset"], "window": w, "account_days": len(days),
              "target_days_consumed": len(actions), "new_fit_calls": 0, "new_predict_calls": 0,
              "IC": None, "RankIC": None, "label_status": "NOT_APPLICABLE",
              "actions_sha256": file_sha256("qe_absolute_target_actions.jsonl"),
              "indicator_availability": indicator_availability,
              "actual_exposure_equals_target": "NOT_ASSUMED", "known_at": "UNVERIFIED"}
    require(result["targets_sha256"] == contract["targets"]["sha256"], "QE_ABSOLUTE_TARGET_POSTRUN_HASH")
    Path(RESULT_FILE).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    recorder.save_objects(absolute_target_result=result, config=config)
    return result
