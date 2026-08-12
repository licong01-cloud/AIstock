from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import resource as _resource
except ModuleNotFoundError:
    _resource = None

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH, FEATURE_SCHEMA_PAYLOAD
from backend.services.advisory_model_first.meta_label_bundle import (
    find_meta_label_bundle_for_request,
    publish_meta_label_bundle,
)
from backend.services.advisory_model_first.meta_label_contracts import FrozenAdvisoryMetaLabelTrainingRequestV1
from backend.services.advisory_model_first.meta_label_evaluation import evaluate_meta_label_validation_blocks
from backend.services.advisory_model_first.meta_label_features import build_meta_label_feature_matrix
from backend.services.advisory_model_first.meta_label_training import train_final_meta_label, train_meta_label_trial
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.policy_cpcv import calculate_policy_pbo
from backend.services.advisory_model_first.policy_dataset_bundle import load_policy_dataset_bundle
from backend.services.advisory_model_first.qe_file_source import (
    STATIC_FACTOR_COLUMNS,
    all_qlib_instruments,
    initialize_qlib,
    load_qlib_daily,
    load_static_factors,
    load_suspend_rows,
    load_trading_calendar,
    validate_factor_file_schemas,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


class MetaLabelProgress:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def add(self, stage: str, started: float, **details: Any) -> None:
        peak = _peak_rss()
        row = {"stage": stage, "wall_seconds": round(time.monotonic() - started, 3), "elapsed_seconds": round(time.monotonic() - self.started, 3), "peak_rss_bytes": peak, **details}
        self.stages.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
        if peak > self.limit:
            raise AdvisoryModelFirstError("meta-label training exceeded RSS limit", reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED", context=row)

    def report(self) -> dict[str, Any]:
        return {"schema_version": "advisory_meta_label_resource_v1", "peak_rss_bytes": _peak_rss(), "limit_bytes": self.limit, "total_wall_seconds": round(time.monotonic() - self.started, 3), "stages": self.stages}


def run_meta_label_pipeline(request_path: str | Path) -> dict[str, Any]:
    request = FrozenAdvisoryMetaLabelTrainingRequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    environment = _verify_environment(request)
    progress = MetaLabelProgress(request.resource_max_rss_bytes)
    started = time.monotonic()
    manifest = load_policy_dataset_bundle(request.policy_dataset_bundle_root, expected_bundle_id=request.policy_dataset_bundle_id)
    manifest_path = Path(request.policy_dataset_bundle_root) / "manifest.json"
    if _sha256(manifest_path) != request.policy_dataset_manifest_file_sha256:
        raise AdvisoryModelFirstError("P0-C manifest hash differs from request", reason_code="ADVISORY_META_LABEL_SOURCE_INVALID")
    for key in ("program_id", "binding_version_id", "package_id", "manifest_sha256", "shadow_policy_sha256", "cost_policy_sha256", "split_policy_sha256"):
        if manifest.get(key) != getattr(request, key):
            raise AdvisoryModelFirstError("P0-C identity differs from meta-label request", reason_code="ADVISORY_META_LABEL_SOURCE_INVALID", context={"field": key})
    existing = find_meta_label_bundle_for_request(request)
    if existing is not None:
        bundle_id, bundle_path, bundle_manifest = existing
        winner = json.loads((bundle_path / "winner_receipt.json").read_text(encoding="utf-8"))
        pbo = json.loads((bundle_path / "pbo_receipt.json").read_text(encoding="utf-8"))
        resource = json.loads((bundle_path / "resource_report.json").read_text(encoding="utf-8"))
        trial_metrics = pd.read_parquet(bundle_path / "cpcv_trial_metrics.parquet")
        return {
            "status": "EXISTING_BUNDLE",
            "request_id": request.request_id,
            "bundle_id": bundle_id,
            "bundle_path": str(bundle_path),
            "manifest": bundle_manifest,
            "winner": winner,
            "pbo": pbo,
            "trial_path_count": len(trial_metrics),
            "resource_report": resource,
            "activated": False,
        }
    root = Path(request.policy_dataset_bundle_root)
    rankings = pd.read_parquet(root / "candidate_rankings.parquet")
    labels = pd.read_parquet(root / "candidate_episode_labels.parquet")
    cpcv = json.loads((root / "cpcv_paths.json").read_text(encoding="utf-8"))
    policy = transition_policy_from_payload(json.loads((root / "shadow_policy.json").read_text(encoding="utf-8")))
    cost = AdvisoryPolicyCostV1.model_validate_json((root / "cost_policy.json").read_text(encoding="utf-8"))
    policy_source_request = json.loads((root / "request.json").read_text(encoding="utf-8"))
    paths = [item for item in cpcv["paths"] if item["status"] == "READY"]
    if len(paths) != 28:
        raise AdvisoryModelFirstError("meta-label requires all 28 READY CPCV paths", reason_code="ADVISORY_META_LABEL_SOURCE_INVALID", context={"ready_paths": len(paths)})
    progress.add("source_readback", started, label_rows=len(labels), ranking_rows=len(rankings), path_count=len(paths))

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    file_history_start = "2023-09-01"
    hmm_history_start = "2023-12-01"
    cutoff = pd.Timestamp(rankings.loc[rankings["is_candidate_decision"], "decision_as_of_trade_date"].max()).date().isoformat()
    data_end = pd.Timestamp(policy_source_request["data_cutoff"]).date().isoformat()
    calendar = load_trading_calendar(file_history_start, data_end)
    symbols = sorted(rankings.loc[rankings["is_candidate_decision"], "instrument"].unique())
    candidate_daily = load_qlib_daily(symbols, start=file_history_start, end=data_end)
    candidate_static = load_static_factors(request.factor_data_root, columns=STATIC_FACTOR_COLUMNS, start=file_history_start, end=data_end, instruments=symbols)
    market_daily = load_qlib_daily(all_qlib_instruments(), start=file_history_start, end=data_end, fields=("$close", "$limit_up"))
    benchmark = load_qlib_daily([cost.benchmark_instrument], start=file_history_start, end=data_end, fields=("$open", "$close"))
    static_all = load_static_factors(request.factor_data_root, columns=("l2_code_id", "sw2_close", "sw2_amount"), start=file_history_start, end=data_end)
    suspend = load_suspend_rows(request.suspend_data_root, start=file_history_start, end=data_end, instruments=symbols)
    schema_receipt = validate_factor_file_schemas(
        request.factor_data_root, data_cutoff=request.factor_data_cutoff
    )
    feature_result = build_meta_label_feature_matrix(rankings=rankings, block_by_date=cpcv["block_by_date"], candidate_daily=candidate_daily, candidate_static=candidate_static, market_daily=market_daily, benchmark_daily=benchmark, suspend_rows=suspend, static_all=static_all, trading_calendar=calendar, hmm_history_start=hmm_history_start, runtime_cutoff=cutoff)
    progress.add("features", started, feature_rows=len(feature_result.features), available_dates=int((feature_result.coverage["status"] == "available").sum()))

    trial_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    trial_predictions: dict[tuple[str, int, str], pd.DataFrame] = {}
    baseline_by_path: dict[str, float] = {}
    hmm_baseline_by_path: dict[str, float] = {}
    random_baseline_by_path: dict[str, float] = {}
    candidate20_by_path: dict[str, float] = {}
    started = time.monotonic()
    for path in paths:
        validation_blocks = tuple(int(value) for value in path["validation_blocks"])
        selection_priority = rankings.loc[
            rankings["is_candidate_decision"] & rankings["decision_as_of_trade_date"].isin(pd.to_datetime(path["validation_dates"])) & (rankings["selection_effective_rank"] <= 20),
            ["decision_as_of_trade_date", "instrument", "selection_effective_rank"],
        ].rename(columns={"selection_effective_rank": "entry_priority_rank"})
        baseline_metrics, _, _ = evaluate_meta_label_validation_blocks(rankings=rankings, predictions=selection_priority, validation_blocks=validation_blocks, block_by_date=cpcv["block_by_date"], daily=candidate_daily, benchmark_daily=benchmark, suspend_rows=suspend, trading_calendar=calendar, policy=policy, policy_sha256=request.shadow_policy_sha256, cost_policy=cost, request_id=f"{request.request_id}_selection_{path['path_id']}")
        baseline_by_path[path["path_id"]] = baseline_metrics[request.primary_metric]
        validation_features = feature_result.features[
            pd.to_datetime(feature_result.features["decision_as_of_trade_date"]).dt.normalize().isin(
                pd.to_datetime(path["validation_dates"])
            )
        ].copy()
        hmm_priority = _rank_priority(
            validation_features,
            score_column="hmm_bull_posterior",
            descending=True,
        )
        hmm_metrics, _, _ = evaluate_meta_label_validation_blocks(rankings=rankings, predictions=hmm_priority, validation_blocks=validation_blocks, block_by_date=cpcv["block_by_date"], daily=candidate_daily, benchmark_daily=benchmark, suspend_rows=suspend, trading_calendar=calendar, policy=policy, policy_sha256=request.shadow_policy_sha256, cost_policy=cost, request_id=f"{request.request_id}_hmm_{path['path_id']}")
        hmm_baseline_by_path[path["path_id"]] = hmm_metrics[request.primary_metric]
        random_priority = validation_features[
            ["decision_as_of_trade_date", "instrument", "selection_effective_rank"]
        ].copy()
        random_priority["random_score"] = [
            int(
                canonical_json_sha256(
                    {
                        "seed": request.seed_roster[0],
                        "date": pd.Timestamp(date).date().isoformat(),
                        "instrument": symbol,
                    }
                )[:16],
                16,
            )
            for date, symbol in zip(
                random_priority["decision_as_of_trade_date"],
                random_priority["instrument"],
                strict=True,
            )
        ]
        random_priority = _rank_priority(random_priority, score_column="random_score", descending=True)
        random_metrics, _, _ = evaluate_meta_label_validation_blocks(rankings=rankings, predictions=random_priority, validation_blocks=validation_blocks, block_by_date=cpcv["block_by_date"], daily=candidate_daily, benchmark_daily=benchmark, suspend_rows=suspend, trading_calendar=calendar, policy=policy, policy_sha256=request.shadow_policy_sha256, cost_policy=cost, request_id=f"{request.request_id}_random_{path['path_id']}")
        random_baseline_by_path[path["path_id"]] = random_metrics[request.primary_metric]
        validation_label_rows = labels[
            pd.to_datetime(labels["decision_as_of_trade_date"]).dt.normalize().isin(
                pd.to_datetime(path["validation_dates"])
            )
            & (labels["label_status"] == "MATURED")
        ]
        candidate20_by_path[path["path_id"]] = float(
            validation_label_rows.groupby("decision_as_of_trade_date")["net_excess_return_bps"].mean().mean()
        )
        for family in request.family_specs:
            for seed in request.seed_roster:
                trial_id = f"{family.family_id}_{seed}"
                result = train_meta_label_trial(features=feature_result.features, labels=labels, train_dates=pd.to_datetime(path["train_dates"]), validation_dates=pd.to_datetime(path["validation_dates"]), family=family, seed=seed)
                policy_metrics, _, _ = evaluate_meta_label_validation_blocks(rankings=rankings, predictions=result.validation_predictions, validation_blocks=validation_blocks, block_by_date=cpcv["block_by_date"], daily=candidate_daily, benchmark_daily=benchmark, suspend_rows=suspend, trading_calendar=calendar, policy=policy, policy_sha256=request.shadow_policy_sha256, cost_policy=cost, request_id=f"{request.request_id}_{trial_id}_{path['path_id']}")
                trial_predictions[(family.family_id, seed, path["path_id"])] = result.validation_predictions
                trial_rows.append({"trial_id": trial_id, "family_id": family.family_id, "seed": seed, "path_id": path["path_id"], "validation_blocks": list(validation_blocks), **result.metrics, **{f"policy_{key}": value for key, value in policy_metrics.items() if key != "block_metrics"}, "selection_baseline_mean_daily_net_excess_return_bps": baseline_metrics[request.primary_metric], "policy_lift_bps": policy_metrics[request.primary_metric] - baseline_metrics[request.primary_metric], "best_iteration": result.best_iteration})
                for item in policy_metrics["block_metrics"]:
                    block_rows.append({"trial_id": trial_id, "family_id": family.family_id, "seed": seed, "path_id": path["path_id"], **item})
    trial_metrics = pd.DataFrame(trial_rows)
    raw_blocks = pd.DataFrame(block_rows)
    block_scores = raw_blocks.groupby(["trial_id", "family_id", "seed", "block_id"], as_index=False)[request.primary_metric].mean()
    pbo = calculate_policy_pbo(block_scores[["trial_id", "block_id", request.primary_metric]], group_count=8, metric_column=request.primary_metric)
    summary = trial_metrics.groupby(["family_id", "seed"], as_index=False).agg(mean_daily_net_excess_return_bps=("policy_mean_daily_net_excess_return_bps", "mean"), mean_policy_lift_bps=("policy_lift_bps", "mean"), path_win_rate=("policy_lift_bps", lambda values: float((values > 0).mean())), mean_roc_auc=("roc_auc", "mean"), mean_brier=("brier", "mean"))
    winner = summary.sort_values([request.primary_metric, "family_id", "seed"], ascending=[False, True, True]).iloc[0]
    family = next(item for item in request.family_specs if item.family_id == winner.family_id)
    winner_iterations = trial_metrics[(trial_metrics["family_id"] == winner.family_id) & (trial_metrics["seed"] == winner.seed)]["best_iteration"]
    final_rounds = int(np.median(winner_iterations))
    final = train_final_meta_label(features=feature_result.features, labels=labels, family=family, seed=int(winner.seed), boost_rounds=final_rounds)
    progress.add("trials_and_final", started, trial_path_count=len(trial_metrics), winner_family=winner.family_id, winner_seed=int(winner.seed), final_rounds=final_rounds)

    feature_schema = {**FEATURE_SCHEMA_PAYLOAD, "feature_schema_hash": FEATURE_SCHEMA_HASH, "trained_feature_names": list(final.feature_names), "categorical_vocabulary": {key: list(value) for key, value in final.categorical_vocabulary.items()}}
    winner_receipt = {"schema_version": "advisory_meta_label_winner_v1", "family_id": str(winner.family_id), "seed": int(winner.seed), "primary_metric": request.primary_metric, "primary_metric_value": float(winner[request.primary_metric]), "mean_policy_lift_bps": float(winner.mean_policy_lift_bps), "path_win_rate": float(winner.path_win_rate), "mean_roc_auc": float(winner.mean_roc_auc), "mean_brier": float(winner.mean_brier), "final_boost_rounds": final_rounds, "tie_break": request.tie_break}
    baseline = {"schema_version": "advisory_meta_label_baselines_v1", "selection_path_primary_metric": baseline_by_path, "selection_mean_primary_metric": float(np.mean(list(baseline_by_path.values()))), "hmm_top5_path_primary_metric": hmm_baseline_by_path, "hmm_top5_mean_primary_metric": float(np.mean(list(hmm_baseline_by_path.values()))), "random_top5_path_primary_metric": random_baseline_by_path, "random_top5_mean_primary_metric": float(np.mean(list(random_baseline_by_path.values()))), "candidate20_equal_path_primary_metric": candidate20_by_path, "candidate20_equal_mean_primary_metric": float(np.mean(list(candidate20_by_path.values()))), "candidate20_equal_semantics": "CANDIDATE_COUNTERFACTUAL_MEAN_NOT_FIVE_SLOT_PORTFOLIO", "m5a_status": "HISTORICAL_REFERENCE_NOT_USED_FOR_SELECTION_OR_MATCHED_POLICY"}
    resource = progress.report()
    bundle_id, bundle_path, bundle_manifest = publish_meta_label_bundle(request=request, booster=final.booster, feature_schema=feature_schema, runtime_hmm_models=feature_result.runtime_hmm_models, runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable), walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt, trial_metrics=trial_metrics, block_scores=block_scores, pbo_receipt=pbo, winner_receipt=winner_receipt, baseline_comparison=baseline, training_log={"environment": environment, "schema_receipt": schema_receipt.__dict__, "trial_summary": summary.to_dict("records")}, resource_report=resource)
    return {"status": "TRAINED", "request_id": request.request_id, "bundle_id": bundle_id, "bundle_path": str(bundle_path), "manifest": bundle_manifest, "winner": winner_receipt, "pbo": pbo, "trial_path_count": len(trial_metrics), "resource_report": progress.report(), "activated": False}


def _verify_environment(request: FrozenAdvisoryMetaLabelTrainingRequestV1) -> dict[str, Any]:
    if os.name == "nt" or "microsoft" not in platform.release().lower() or os.getenv("CONDA_DEFAULT_ENV") != "rdagent-gpu":
        raise AdvisoryModelFirstError("meta-label training requires WSL rdagent-gpu", reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL")
    git = ["git.exe", "-C", request.repository_root_windows]
    commit = subprocess.run(
        [*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True
    ).stdout.strip().lower()
    if commit != request.repository_commit:
        raise AdvisoryModelFirstError("meta-label repository commit mismatch", reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH", context={"expected": request.repository_commit, "actual": commit})
    tracked_changes = subprocess.run(
        [*git, "status", "--porcelain", "--untracked-files=no"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if tracked_changes:
        raise AdvisoryModelFirstError(
            "meta-label repository has uncommitted tracked changes",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"changed_file_count": len(tracked_changes.splitlines())},
        )
    return {"conda_env": os.getenv("CONDA_DEFAULT_ENV"), "repository_commit": commit, "python": platform.python_version(), "lightgbm": importlib.metadata.version("lightgbm")}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _peak_rss() -> int:
    if _resource is None:
        return 0
    return int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss) * (1 if platform.system() == "Darwin" else 1024)


def _rank_priority(frame: pd.DataFrame, *, score_column: str, descending: bool) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "instrument",
        "selection_effective_rank",
        score_column,
    }
    if not required.issubset(frame.columns):
        raise AdvisoryModelFirstError(
            "baseline priority input is incomplete",
            reason_code="ADVISORY_META_LABEL_EVALUATION_INVALID",
            context={"missing_columns": sorted(required - set(frame.columns))},
        )
    result = frame[
        ["decision_as_of_trade_date", "instrument", "selection_effective_rank", score_column]
    ].copy()
    result["_missing"] = result[score_column].isna()
    result = result.sort_values(
        ["decision_as_of_trade_date", "_missing", score_column, "selection_effective_rank", "instrument"],
        ascending=[True, True, not descending, True, True],
    )
    result["entry_priority_rank"] = result.groupby("decision_as_of_trade_date").cumcount().add(1)
    return result.drop(columns="_missing")
