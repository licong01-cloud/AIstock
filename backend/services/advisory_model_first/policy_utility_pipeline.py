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
from backend.services.advisory_model_first.feature_schema_v2 import build_feature_schema_payload
from backend.services.advisory_model_first.meta_label_bundle import load_meta_label_bundle
from backend.services.advisory_model_first.meta_label_evaluation import evaluate_meta_label_validation_blocks
from backend.services.advisory_model_first.meta_label_features import build_meta_label_feature_matrix
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1, transition_policy_from_payload
from backend.services.advisory_model_first.policy_cpcv import calculate_policy_pbo
from backend.services.advisory_model_first.policy_dataset_bundle import load_policy_dataset_bundle
from backend.services.advisory_model_first.policy_utility_bundle import (
    find_policy_utility_bundle_for_request,
    publish_policy_utility_bundle,
)
from backend.services.advisory_model_first.policy_utility_contracts import (
    ExactMetaLabelReferenceV1,
    FrozenAdvisoryPolicyUtilityTrainingRequestV2,
)
from backend.services.advisory_model_first.policy_utility_training import (
    FinalPolicyUtilityTrainingResult,
    train_final_policy_utility,
    train_policy_utility_trial,
)
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


class PolicyUtilityProgress:
    def __init__(self, limit: int) -> None:
        self.limit = limit
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def add(self, stage: str, started: float, **details: Any) -> None:
        peak = _peak_rss()
        row = {
            "stage": stage,
            "wall_seconds": round(time.monotonic() - started, 3),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "peak_rss_bytes": peak,
            **details,
        }
        self.stages.append(row)
        print(json.dumps(row, sort_keys=True), flush=True)
        if peak > self.limit:
            raise AdvisoryModelFirstError(
                "policy utility training exceeded RSS limit",
                reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
                context=row,
            )

    def report(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_policy_utility_resource_v1",
            "peak_rss_bytes": _peak_rss(),
            "limit_bytes": self.limit,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "stages": self.stages,
        }


def run_policy_utility_pipeline(request_path: str | Path) -> dict[str, Any]:
    try:
        request = FrozenAdvisoryPolicyUtilityTrainingRequestV2.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "policy utility frozen request cannot be read or validated",
            reason_code="ADVISORY_POLICY_UTILITY_REQUEST_INVALID",
        ) from exc
    environment = _verify_environment(request)
    progress = PolicyUtilityProgress(request.resource_max_rss_bytes)
    started = time.monotonic()
    _verify_policy_dataset(request)
    legacy_p0d = _load_reference(request, request.legacy_p0d_reference)
    legacy_p0e = _load_reference(request, request.legacy_p0e_reference)
    existing = find_policy_utility_bundle_for_request(request)
    if existing is not None:
        bundle_id, bundle_path, bundle_manifest = existing
        return {
            "status": "EXISTING_BUNDLE",
            "request_id": request.request_id,
            "bundle_id": bundle_id,
            "bundle_path": str(bundle_path),
            "manifest": bundle_manifest,
            "winner": _read_json(bundle_path / "winner_receipt.json"),
            "pbo": _read_json(bundle_path / "pbo_receipt.json"),
            "advancement": _read_json(bundle_path / "advancement_receipt.json"),
            "trial_path_count": len(pd.read_parquet(bundle_path / "cpcv_trial_metrics.parquet")),
            "resource_report": _read_json(bundle_path / "resource_report.json"),
            "activated": False,
        }
    root = Path(request.policy_dataset_bundle_root)
    rankings = pd.read_parquet(root / "candidate_rankings.parquet")
    labels = pd.read_parquet(root / "candidate_episode_labels.parquet")
    _verify_policy_source_coverage(request, rankings, labels)
    cpcv = _read_json(root / "cpcv_paths.json")
    policy = transition_policy_from_payload(_read_json(root / "shadow_policy.json"))
    cost = AdvisoryPolicyCostV1.model_validate_json((root / "cost_policy.json").read_text(encoding="utf-8"))
    policy_source_request = _read_json(root / "request.json")
    paths = [item for item in cpcv["paths"] if item["status"] == "READY"]
    if len(paths) != 28 or len({item["path_id"] for item in paths}) != 28:
        raise AdvisoryModelFirstError(
            "policy utility requires all 28 unique READY CPCV paths",
            reason_code="ADVISORY_POLICY_UTILITY_INCOMPLETE_CPCV",
            context={"ready_paths": len(paths)},
        )
    _verify_training_cutoffs(request, labels)
    progress.add("source_readback", started, label_rows=len(labels), ranking_rows=len(rankings), path_count=len(paths))

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    file_history_start = "2023-09-01"
    hmm_history_start = "2023-12-01"
    runtime_cutoff = request.latest_training_decision_trade_date
    data_end = pd.Timestamp(policy_source_request["data_cutoff"]).date().isoformat()
    calendar = load_trading_calendar(file_history_start, data_end)
    symbols = sorted(rankings.loc[rankings["is_candidate_decision"], "instrument"].unique())
    candidate_daily = load_qlib_daily(symbols, start=file_history_start, end=data_end)
    candidate_static = load_static_factors(
        request.factor_data_root,
        columns=STATIC_FACTOR_COLUMNS,
        start=file_history_start,
        end=data_end,
        instruments=symbols,
    )
    market_daily = load_qlib_daily(
        all_qlib_instruments(), start=file_history_start, end=data_end, fields=("$close", "$limit_up")
    )
    benchmark = load_qlib_daily(
        [cost.benchmark_instrument], start=file_history_start, end=data_end, fields=("$open", "$close")
    )
    static_all = load_static_factors(
        request.factor_data_root,
        columns=("l2_code_id", "sw2_close", "sw2_amount"),
        start=file_history_start,
        end=data_end,
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start=file_history_start,
        end=data_end,
        instruments=symbols,
        full_day_only=True,
    )
    _verify_bound_data_identities(request, calendar)
    schema_receipt = validate_factor_file_schemas(request.factor_data_root, data_cutoff=request.factor_data_cutoff)
    feature_result = build_meta_label_feature_matrix(
        rankings=rankings,
        block_by_date=cpcv["block_by_date"],
        candidate_daily=candidate_daily,
        candidate_static=candidate_static,
        market_daily=market_daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        static_all=static_all,
        trading_calendar=calendar,
        hmm_history_start=hmm_history_start,
        runtime_cutoff=runtime_cutoff,
        feature_schema_version=request.feature_schema_version,
    )
    _verify_feature_v2_coverage(request, feature_result.features, rankings)
    progress.add(
        "features",
        started,
        feature_rows=len(feature_result.features),
        available_dates=int((feature_result.coverage["status"] == "available").sum()),
    )

    trial_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    baseline_by_path: dict[str, dict[str, float]] = {}
    path_failure: dict[str, Any] | None = None
    started = time.monotonic()
    # Arm outermost is intentional: only one objective's boosters are resident
    # at a time, preserving the frozen sequential-under-8GB execution contract.
    for arm in request.arm_specs:
        for path in paths:
            path_id = str(path["path_id"])
            validation_blocks = tuple(int(value) for value in path["validation_blocks"])
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path["validation_dates"])).normalize()
            if path_id not in baseline_by_path:
                selection_priority = rankings.loc[
                    rankings["is_candidate_decision"]
                    & pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize().isin(validation_dates)
                    & (rankings["selection_effective_rank"] <= 20),
                    ["decision_as_of_trade_date", "instrument", "selection_effective_rank"],
                ].rename(columns={"selection_effective_rank": "entry_priority_rank"})
                baseline_metrics, _, _ = _evaluate(
                    rankings,
                    selection_priority,
                    validation_blocks,
                    cpcv,
                    candidate_daily,
                    benchmark,
                    suspend,
                    calendar,
                    policy,
                    request,
                    cost,
                    f"selection_{path_id}",
                )
                baseline_by_path[path_id] = _paired_metrics(baseline_metrics)
            baseline_metrics = baseline_by_path[path_id]
            for family in request.family_specs:
                for seed in request.seed_roster:
                    trial_id = f"{arm.arm_id}_{family.family_id}_{seed}"
                    try:
                        result = train_policy_utility_trial(
                            features=feature_result.features,
                            labels=labels,
                            train_dates=pd.to_datetime(path["train_dates"]),
                            validation_dates=validation_dates,
                            family=family,
                            seed=seed,
                            arm=arm,
                        )
                        policy_metrics, policy_daily, policy_episodes = _evaluate(
                            rankings,
                            result.validation_predictions,
                            validation_blocks,
                            cpcv,
                            candidate_daily,
                            benchmark,
                            suspend,
                            calendar,
                            policy,
                            request,
                            cost,
                            f"{trial_id}_{path_id}",
                        )
                    except AdvisoryModelFirstError as exc:
                        if exc.reason_code not in {
                            "ADVISORY_POLICY_UTILITY_PATH_NOT_COMPUTABLE",
                            "ADVISORY_POLICY_UTILITY_TOP20_INVALID",
                        }:
                            raise
                        path_failure = {
                            "arm_id": arm.arm_id,
                            "path_id": path_id,
                            "family_id": family.family_id,
                            "seed": seed,
                            **exc.as_dict(),
                        }
                        break
                    transform = result.transform
                    trial_rows.append(
                        {
                            "trial_id": trial_id,
                            "arm_id": arm.arm_id,
                            "training_objective": arm.training_objective,
                            "family_id": family.family_id,
                            "seed": seed,
                            "path_id": path_id,
                            "validation_blocks": list(validation_blocks),
                            **result.metrics,
                            **{
                                f"policy_{key}": value
                                for key, value in policy_metrics.items()
                                if key != "block_metrics"
                            },
                            **_episode_metrics(policy_daily, policy_episodes, target_count=policy.target_count),
                            "selection_baseline_mean_daily_net_excess_return_bps": baseline_metrics[
                                request.primary_metric
                            ],
                            "policy_lift_bps": policy_metrics[request.primary_metric]
                            - baseline_metrics[request.primary_metric],
                            "transform_location_bps": (transform.location_bps if transform is not None else None),
                            "transform_scale_bps": (transform.scale_bps if transform is not None else None),
                            "best_iteration": result.best_iteration,
                            "outcome_weight_scale_bps": (
                                result.outcome_weighting_receipt["scale_bps"]
                                if result.outcome_weighting_receipt is not None
                                else None
                            ),
                            "outcome_weight_normalization_divisor": (
                                result.outcome_weighting_receipt["normalization_divisor"]
                                if result.outcome_weighting_receipt is not None
                                else None
                            ),
                        }
                    )
                    for item in policy_metrics["block_metrics"]:
                        block_rows.append(
                            {
                                "trial_id": trial_id,
                                "arm_id": arm.arm_id,
                                "family_id": family.family_id,
                                "seed": seed,
                                "path_id": path_id,
                                **item,
                            }
                        )
                if path_failure is not None:
                    break
            if path_failure is not None:
                break
        if path_failure is not None:
            break
    trial_metrics = pd.DataFrame(trial_rows)
    if path_failure is not None:
        return _publish_incomplete_experiment(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            baseline_by_path=baseline_by_path,
            legacy_p0d=legacy_p0d,
            legacy_p0e=legacy_p0e,
            path_failure=path_failure,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    if len(trial_metrics) != request.expected_trial_path_count:
        raise AdvisoryModelFirstError(
            "policy utility trial roster is incomplete",
            reason_code="ADVISORY_POLICY_UTILITY_ARM_ROSTER_INVALID",
            context={"trial_path_count": len(trial_metrics)},
        )
    raw_blocks = pd.DataFrame(block_rows)
    block_scores = raw_blocks.groupby(["trial_id", "arm_id", "family_id", "seed", "block_id"], as_index=False)[
        request.primary_metric
    ].mean()
    pbo = calculate_policy_pbo(
        block_scores[["trial_id", "block_id", request.primary_metric]],
        group_count=8,
        metric_column=request.primary_metric,
    )
    summary = trial_metrics.groupby(["arm_id", "family_id", "seed"], as_index=False).agg(
        mean_daily_net_excess_return_bps=("policy_mean_daily_net_excess_return_bps", "mean"),
        mean_daily_net_return_bps=("policy_mean_daily_net_return_bps", "mean"),
        mean_maximum_drawdown=("policy_maximum_drawdown", "mean"),
        mean_turnover_fraction=("policy_mean_turnover_fraction", "mean"),
        completed_episode_hit_rate=("policy_completed_episode_hit_rate", "mean"),
    )
    winners: dict[str, pd.Series] = {}
    winner_rows_by_arm: dict[str, pd.DataFrame] = {}
    finals: dict[str, FinalPolicyUtilityTrainingResult] = {}
    for arm in request.arm_specs:
        arm_summary = summary[summary["arm_id"] == arm.arm_id]
        winner = arm_summary.sort_values(
            [request.primary_metric, "family_id", "seed"], ascending=[False, True, True]
        ).iloc[0]
        winners[arm.arm_id] = winner
        winner_rows = trial_metrics[
            (trial_metrics["arm_id"] == arm.arm_id)
            & (trial_metrics["family_id"] == winner.family_id)
            & (trial_metrics["seed"] == winner.seed)
        ]
        winner_rows_by_arm[arm.arm_id] = winner_rows
        family = next(item for item in request.family_specs if item.family_id == winner.family_id)
        finals[arm.arm_id] = train_final_policy_utility(
            features=feature_result.features,
            labels=labels,
            family=family,
            seed=int(winner.seed),
            boost_rounds=int(np.median(winner_rows["best_iteration"])),
            arm=arm,
        )
    progress.add(
        "trials_and_final",
        started,
        trial_path_count=len(trial_metrics),
        winner_by_arm={
            arm_id: {"family_id": str(row.family_id), "seed": int(row.seed)} for arm_id, row in winners.items()
        },
    )

    p0d_rows = winner_rows_by_arm["ARM_P0D_V2_BINARY_PARITY"]
    p0f_rows = winner_rows_by_arm["ARM_P0F_V2_HUBER_UTILITY"]
    p0d_comparison = compare_policy_arm_rows(
        candidate_rows=p0f_rows,
        reference_rows=p0d_rows,
        reference_role="ARM_P0D_V2_BINARY_PARITY",
    )
    p0e_comparison = compare_policy_arm_rows(
        candidate_rows=p0f_rows,
        reference_rows=winner_rows_by_arm["ARM_P0E_V2_WEIGHTED_BINARY"],
        reference_role="ARM_P0E_V2_WEIGHTED_BINARY",
    )
    selection_lift = float(
        p0f_rows["policy_mean_daily_net_excess_return_bps"].mean()
        - np.mean([item[request.primary_metric] for item in baseline_by_path.values()])
    )
    advancement = build_policy_utility_advancement_receipt(
        p0d_comparison=p0d_comparison,
        candidate_minus_selection_mean_primary_metric_bps=selection_lift,
        candidate_path_ids=p0f_rows["path_id"].tolist(),
    )
    feature_schema = build_feature_schema_payload(
        market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
        suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
    )
    feature_schema.update(
        {
            "feature_schema_hash": request.feature_schema_hash,
            "trained_feature_names_by_arm": {arm_id: list(final.feature_names) for arm_id, final in finals.items()},
            "categorical_vocabulary_by_arm": {
                arm_id: {key: list(value) for key, value in final.categorical_vocabulary.items()}
                for arm_id, final in finals.items()
            },
        }
    )
    winner_receipt = {
        "schema_version": "advisory_policy_utility_winner_v2",
        "winner_by_arm": {
            arm_id: _winner_receipt(
                request,
                winners[arm_id],
                winner_rows_by_arm[arm_id],
                finals[arm_id],
                advancement,
                finals[arm_id].boost_rounds,
            )
            for arm_id in winners
        },
        "stage_b_confidence_arm_id": "ARM_P0D_V2_BINARY_PARITY",
        "advancement_candidate_arm_id": "ARM_P0F_V2_HUBER_UTILITY",
    }
    baseline = {
        "schema_version": "advisory_policy_utility_baselines_v1",
        "selection_path_metrics": baseline_by_path,
        "selection_mean_primary_metric": float(
            np.mean([item[request.primary_metric] for item in baseline_by_path.values()])
        ),
    }
    references = {
        "schema_version": "advisory_policy_utility_reference_comparison_v2",
        "p0d_v2_parity": p0d_comparison,
        "p0e_v2_diagnostic": p0e_comparison,
        "p0e_is_advancement_gate": False,
        "legacy_p0d_bundle_id": legacy_p0d["manifest"]["bundle_id"],
        "legacy_p0e_bundle_id": legacy_p0e["manifest"]["bundle_id"],
        "legacy_references_are_advancement_gate": False,
    }
    resource = progress.report()
    bundle_id, bundle_path, bundle_manifest = publish_policy_utility_bundle(
        request=request,
        arm_boosters={arm_id: final.booster for arm_id, final in finals.items()},
        feature_schema=feature_schema,
        transform_receipt={
            "schema_version": "advisory_policy_utility_transform_v2",
            "by_arm": {
                arm_id: (
                    {
                        "mode": "TRAIN_MEDIAN_MAD_AFFINE_V1",
                        "location_bps": final.transform.location_bps,
                        "scale_bps": final.transform.scale_bps,
                    }
                    if final.transform is not None
                    else {"mode": "BINARY_PROBABILITY_NO_LABEL_TRANSFORM"}
                )
                for arm_id, final in finals.items()
            },
            "fit_scope": "ALL_EXACT_P0_C_MATURED_ROWS_FINAL_REFIT",
        },
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        pbo_receipt=pbo,
        winner_receipt=winner_receipt,
        baseline_comparison=baseline,
        reference_comparison=references,
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt.__dict__,
            "trial_summary": summary.to_dict("records"),
            "exact_feature_coverage": {
                "row_count": len(feature_result.features),
                "decision_date_count": feature_result.features["decision_as_of_trade_date"].nunique(),
                "candidates_per_date": request.expected_candidates_per_date,
                "dropped_candidate_count": 0,
            },
            "model_information_cutoff_trade_date": request.model_information_cutoff_trade_date,
            "experiment_lineage": list(request.experiment_lineage),
            "independent_oos_evidence": False,
        },
        resource_report=resource,
    )
    return {
        "status": advancement["experiment_status"],
        "request_id": request.request_id,
        "bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": bundle_manifest,
        "winner": winner_receipt,
        "pbo": pbo,
        "advancement": advancement,
        "trial_path_count": len(trial_metrics),
        "resource_report": resource,
        "activated": False,
    }


def _publish_incomplete_experiment(
    *,
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    feature_result: Any,
    trial_metrics: pd.DataFrame,
    block_rows: list[dict[str, Any]],
    baseline_by_path: dict[str, dict[str, float]],
    legacy_p0d: dict[str, Any],
    legacy_p0e: dict[str, Any],
    path_failure: dict[str, Any],
    environment: dict[str, Any],
    schema_receipt: dict[str, Any],
    progress: PolicyUtilityProgress,
) -> dict[str, Any]:
    block_scores = pd.DataFrame(block_rows)
    advancement = {
        "schema_version": "advisory_policy_utility_advancement_v2",
        "experiment_status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "advanced_to_stage_b": False,
        "checks": {"exact_504_trial_paths": False},
        "path_failure": path_failure,
        "completed_trial_path_count": len(trial_metrics),
        "stage_b_guard": "DENY_INCOMPLETE_CPCV",
        "pbo_is_gate": False,
        "candidate_diagnostics_are_gate": False,
        "historical_replay_is_gate": False,
    }
    winner = {
        "schema_version": "advisory_policy_utility_winner_v2",
        "status": "NOT_SELECTED_INCOMPLETE_CPCV",
        "winner_by_arm": {},
    }
    references = {
        "schema_version": "advisory_policy_utility_reference_comparison_v2",
        "status": "NOT_COMPUTABLE_INCOMPLETE_CPCV",
        "legacy_p0d_bundle_id": legacy_p0d["manifest"]["bundle_id"],
        "legacy_p0e_bundle_id": legacy_p0e["manifest"]["bundle_id"],
        "legacy_references_are_advancement_gate": False,
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_policy_utility_bundle(
        request=request,
        arm_boosters=None,
        feature_schema={
            **build_feature_schema_payload(
                market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
                suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
            ),
            "feature_schema_hash": request.feature_schema_hash,
            "trained_feature_names_by_arm": {},
            "categorical_vocabulary_by_arm": {},
            "status": "NO_WINNER_INCOMPLETE_CPCV",
        },
        transform_receipt={
            "schema_version": "advisory_policy_utility_transform_v2",
            "status": "NO_FINAL_REFIT_INCOMPLETE_CPCV",
        },
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        pbo_receipt={"status": "NOT_COMPUTABLE_INCOMPLETE_CPCV"},
        winner_receipt=winner,
        baseline_comparison={
            "schema_version": "advisory_policy_utility_baselines_v1",
            "selection_path_metrics_completed_before_stop": baseline_by_path,
        },
        reference_comparison=references,
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt,
            "path_failure": path_failure,
            "experiment_lineage": list(request.experiment_lineage),
            "independent_oos_evidence": False,
        },
        resource_report=resource,
    )
    return {
        "status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "request_id": request.request_id,
        "bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "winner": winner,
        "pbo": {"status": "NOT_COMPUTABLE_INCOMPLETE_CPCV"},
        "advancement": advancement,
        "trial_path_count": len(trial_metrics),
        "resource_report": resource,
        "activated": False,
    }


def compare_policy_utility_reference(
    *, winner_rows: pd.DataFrame, reference: dict[str, Any], reference_role: str
) -> dict[str, Any]:
    reference_winner = reference["winner"]
    reference_rows = reference["trial_metrics"]
    reference_rows = reference_rows[
        (reference_rows["family_id"] == str(reference_winner["family_id"]))
        & (reference_rows["seed"].astype(int) == int(reference_winner["seed"]))
    ][
        [
            "path_id",
            "policy_mean_daily_net_excess_return_bps",
            "policy_maximum_drawdown",
            "policy_mean_turnover_fraction",
        ]
    ].rename(
        columns={
            "policy_mean_daily_net_excess_return_bps": "reference_primary_metric_bps",
            "policy_maximum_drawdown": "reference_maximum_drawdown",
            "policy_mean_turnover_fraction": "reference_mean_turnover_fraction",
        }
    )
    candidate = winner_rows[
        [
            "path_id",
            "policy_mean_daily_net_excess_return_bps",
            "policy_maximum_drawdown",
            "policy_mean_turnover_fraction",
        ]
    ].rename(
        columns={
            "policy_mean_daily_net_excess_return_bps": "candidate_primary_metric_bps",
            "policy_maximum_drawdown": "candidate_maximum_drawdown",
            "policy_mean_turnover_fraction": "candidate_mean_turnover_fraction",
        }
    )
    if (
        len(candidate) != 28
        or len(reference_rows) != 28
        or candidate["path_id"].duplicated().any()
        or reference_rows["path_id"].duplicated().any()
        or set(candidate["path_id"]) != set(reference_rows["path_id"])
    ):
        raise AdvisoryModelFirstError(
            "policy utility reference comparison does not contain exact 28 paths",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_INVALID",
        )
    paired = candidate.merge(reference_rows, on="path_id", how="inner", validate="one_to_one").sort_values("path_id")
    paired["candidate_minus_reference_bps"] = (
        paired["candidate_primary_metric_bps"] - paired["reference_primary_metric_bps"]
    )
    paired["maximum_drawdown_difference"] = paired["candidate_maximum_drawdown"] - paired["reference_maximum_drawdown"]
    paired["mean_turnover_fraction_difference"] = (
        paired["candidate_mean_turnover_fraction"] - paired["reference_mean_turnover_fraction"]
    )
    lifts = paired["candidate_minus_reference_bps"]
    return {
        "schema_version": "advisory_policy_utility_paired_reference_v1",
        "reference_role": reference_role,
        "reference_bundle_id": reference["manifest"]["bundle_id"],
        "reference_family_id": reference_winner["family_id"],
        "reference_seed": int(reference_winner["seed"]),
        "path_count": len(paired),
        "candidate_minus_reference_mean_primary_metric_bps": float(lifts.mean()),
        "candidate_path_win_rate": float((lifts > 0.0).mean()),
        "mean_maximum_drawdown_difference": float(paired["maximum_drawdown_difference"].mean()),
        "mean_turnover_fraction_difference": float(paired["mean_turnover_fraction_difference"].mean()),
        "path_comparisons": paired.to_dict("records"),
    }


def compare_policy_arm_rows(
    *,
    candidate_rows: pd.DataFrame,
    reference_rows: pd.DataFrame,
    reference_role: str,
) -> dict[str, Any]:
    required = {
        "path_id",
        "policy_mean_daily_net_excess_return_bps",
        "policy_maximum_drawdown",
        "policy_mean_turnover_fraction",
    }
    if not required.issubset(candidate_rows) or not required.issubset(reference_rows):
        raise AdvisoryModelFirstError(
            "policy utility parity comparison schema is incomplete",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_NOT_PARITY",
        )
    candidate = candidate_rows[list(required)].rename(
        columns={
            "policy_mean_daily_net_excess_return_bps": "candidate_primary_metric_bps",
            "policy_maximum_drawdown": "candidate_maximum_drawdown",
            "policy_mean_turnover_fraction": "candidate_mean_turnover_fraction",
        }
    )
    reference = reference_rows[list(required)].rename(
        columns={
            "policy_mean_daily_net_excess_return_bps": "reference_primary_metric_bps",
            "policy_maximum_drawdown": "reference_maximum_drawdown",
            "policy_mean_turnover_fraction": "reference_mean_turnover_fraction",
        }
    )
    if (
        len(candidate) != 28
        or len(reference) != 28
        or candidate["path_id"].duplicated().any()
        or reference["path_id"].duplicated().any()
        or set(candidate["path_id"]) != set(reference["path_id"])
    ):
        raise AdvisoryModelFirstError(
            "policy utility parity comparison does not contain exact same 28 paths",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_NOT_PARITY",
        )
    paired = candidate.merge(reference, on="path_id", validate="one_to_one").sort_values("path_id")
    paired["candidate_minus_reference_bps"] = (
        paired["candidate_primary_metric_bps"] - paired["reference_primary_metric_bps"]
    )
    paired["maximum_drawdown_difference"] = paired["candidate_maximum_drawdown"] - paired["reference_maximum_drawdown"]
    paired["turnover_fraction_difference"] = (
        paired["candidate_mean_turnover_fraction"] - paired["reference_mean_turnover_fraction"]
    )
    lifts = paired["candidate_minus_reference_bps"]
    return {
        "schema_version": "advisory_policy_utility_paired_arm_v2",
        "reference_role": reference_role,
        "path_count": len(paired),
        "candidate_minus_reference_mean_primary_metric_bps": float(lifts.mean()),
        "candidate_path_win_rate": float((lifts > 0).mean()),
        "tie_path_count": int((lifts == 0).sum()),
        "mean_maximum_drawdown_difference": float(paired["maximum_drawdown_difference"].mean()),
        "mean_turnover_fraction_difference": float(paired["turnover_fraction_difference"].mean()),
        "path_comparisons": paired.to_dict("records"),
    }


def build_policy_utility_advancement_receipt(
    *,
    p0d_comparison: dict[str, Any],
    candidate_minus_selection_mean_primary_metric_bps: float,
    candidate_path_ids: list[str],
) -> dict[str, Any]:
    if p0d_comparison.get("reference_role") != "ARM_P0D_V2_BINARY_PARITY":
        raise AdvisoryModelFirstError(
            "policy utility advancement reference is not the same-coverage P0-D v2 arm",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_NOT_PARITY",
        )
    complete = len(candidate_path_ids) == 28 and len(set(candidate_path_ids)) == 28
    checks = {
        "candidate_minus_p0d_mean_primary_gt_zero": bool(
            p0d_comparison["candidate_minus_reference_mean_primary_metric_bps"] > 0.0
        ),
        "candidate_vs_p0d_path_win_rate_gt_half": bool(p0d_comparison["candidate_path_win_rate"] > 0.5),
        "candidate_minus_selection_mean_primary_gt_zero": bool(candidate_minus_selection_mean_primary_metric_bps > 0.0),
        "paired_mean_maximum_drawdown_difference_gte_zero": bool(
            p0d_comparison["mean_maximum_drawdown_difference"] >= 0.0
        ),
        "paired_mean_turnover_fraction_difference_lte_zero": bool(
            p0d_comparison["mean_turnover_fraction_difference"] <= 0.0
        ),
        "exact_28_unique_paths": complete,
    }
    advanced = all(checks.values())
    status = (
        "ADVANCED_TO_STAGE_B"
        if advanced
        else ("NEGATIVE_STOP_NOT_ADVANCED" if complete else "NEGATIVE_STOP_INCOMPLETE_CPCV")
    )
    return {
        "schema_version": "advisory_policy_utility_advancement_v1",
        "experiment_status": status,
        "advanced_to_stage_b": advanced,
        "checks": checks,
        "candidate_minus_selection_mean_primary_metric_bps": candidate_minus_selection_mean_primary_metric_bps,
        "p0d_paired_comparison": {key: value for key, value in p0d_comparison.items() if key != "path_comparisons"},
        "stage_b_guard": "ALLOW_ONLY_IF_ADVANCED_TO_STAGE_B",
        "pbo_is_gate": False,
        "candidate_diagnostics_are_gate": False,
        "historical_replay_is_gate": False,
    }


def _verify_feature_v2_coverage(
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    features: pd.DataFrame,
    rankings: pd.DataFrame,
) -> None:
    keys = ["decision_as_of_trade_date", "instrument"]
    if not set(keys).issubset(features) or features.duplicated(keys).any():
        raise AdvisoryModelFirstError(
            "feature v2 identities are missing or duplicated",
            reason_code="ADVISORY_FEATURE_V2_COVERAGE_INVALID",
        )
    actual = features[keys].copy()
    actual["decision_as_of_trade_date"] = pd.to_datetime(actual["decision_as_of_trade_date"]).dt.normalize()
    actual["instrument"] = actual["instrument"].astype(str).str.upper()
    counts = actual.groupby("decision_as_of_trade_date").size()
    expected = rankings.loc[
        rankings["is_candidate_decision"] & (rankings["selection_effective_rank"] <= 20), keys
    ].copy()
    expected["decision_as_of_trade_date"] = pd.to_datetime(expected["decision_as_of_trade_date"]).dt.normalize()
    expected["instrument"] = expected["instrument"].astype(str).str.upper()
    expected_has_duplicates = expected.duplicated(keys).any()
    expected = expected.drop_duplicates(keys)
    actual_identity = set(actual.itertuples(index=False, name=None))
    expected_identity = set(expected.itertuples(index=False, name=None))
    valid = (
        len(actual) == request.expected_candidate_row_count
        and len(counts) == request.expected_decision_date_count
        and counts.eq(request.expected_candidates_per_date).all()
        and actual_identity == expected_identity
        and len(expected_identity) == request.expected_candidate_row_count
        and not expected_has_duplicates
    )
    if not valid:
        missing = sorted(expected_identity - actual_identity)[:10]
        extra = sorted(actual_identity - expected_identity)[:10]
        raise AdvisoryModelFirstError(
            "feature v2 does not preserve the exact P0-C Selection Top20 coverage",
            reason_code="ADVISORY_FEATURE_V2_COVERAGE_INVALID",
            context={
                "actual_row_count": len(actual),
                "actual_decision_date_count": len(counts),
                "minimum_candidates_per_date": int(counts.min()) if len(counts) else 0,
                "maximum_candidates_per_date": int(counts.max()) if len(counts) else 0,
                "missing_identity_count": len(expected_identity - actual_identity),
                "extra_identity_count": len(actual_identity - expected_identity),
                "missing_samples": [f"{date.date()}:{symbol}" for date, symbol in missing],
                "extra_samples": [f"{date.date()}:{symbol}" for date, symbol in extra],
            },
        )


def _verify_policy_source_coverage(
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    rankings: pd.DataFrame,
    labels: pd.DataFrame,
) -> None:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    required_rankings = {*keys, "is_candidate_decision", "selection_effective_rank"}
    required_labels = {*keys, "label_status"}
    if not required_rankings.issubset(rankings) or not required_labels.issubset(labels):
        raise AdvisoryModelFirstError(
            "P0-C ranking or label schema is incomplete",
            reason_code="ADVISORY_POLICY_UTILITY_DATASET_INVALID",
        )
    candidates = rankings.loc[
        rankings["is_candidate_decision"] & (rankings["selection_effective_rank"] <= 20), keys
    ].copy()
    label_identity = labels[keys].copy()
    for frame in (candidates, label_identity):
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
        frame["target_trade_date"] = pd.to_datetime(frame["target_trade_date"]).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
    candidate_identity = set(candidates.itertuples(index=False, name=None))
    labels_identity = set(label_identity.itertuples(index=False, name=None))
    valid = (
        len(candidates) == request.expected_candidate_row_count
        and len(labels) == request.expected_candidate_row_count
        and not candidates.duplicated(keys).any()
        and not label_identity.duplicated(keys).any()
        and candidate_identity == labels_identity
    )
    if not valid:
        raise AdvisoryModelFirstError(
            "P0-C rankings and labels do not have exact one-to-one candidate coverage",
            reason_code="ADVISORY_POLICY_UTILITY_DATASET_INVALID",
            context={
                "ranking_candidate_row_count": len(candidates),
                "label_row_count": len(labels),
                "missing_label_identity_count": len(candidate_identity - labels_identity),
                "extra_label_identity_count": len(labels_identity - candidate_identity),
            },
        )


def _verify_bound_data_identities(
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    calendar: pd.DatetimeIndex,
) -> None:
    normalized = pd.DatetimeIndex(pd.to_datetime(calendar)).normalize()
    actual_calendar = {
        "sha256": _calendar_identity_sha256(normalized),
        "row_count": len(normalized),
    }
    expected_calendar = request.market_calendar_identity
    suspend_path = Path(request.suspend_data_root) / "suspend_d.parquet"
    try:
        suspend_dates = pd.read_parquet(suspend_path, columns=["trade_date"])["trade_date"]
    except (OSError, ValueError, KeyError) as exc:
        raise AdvisoryModelFirstError(
            "bound suspend sidecar identity cannot be read",
            reason_code="ADVISORY_POLICY_UTILITY_DATASET_INVALID",
        ) from exc
    actual_suspend = {"sha256": _sha256(suspend_path), "row_count": len(suspend_dates)}
    expected_suspend = request.suspend_sidecar_identity
    mismatches: dict[str, Any] = {}
    if (
        actual_calendar["sha256"] != expected_calendar.sha256
        or actual_calendar["row_count"] != expected_calendar.row_count
    ):
        mismatches["market_calendar"] = {
            "expected_sha256": expected_calendar.sha256,
            "actual_sha256": actual_calendar["sha256"],
            "expected_row_count": expected_calendar.row_count,
            "actual_row_count": actual_calendar["row_count"],
        }
    if actual_suspend["sha256"] != expected_suspend.sha256 or actual_suspend["row_count"] != expected_suspend.row_count:
        mismatches["suspend_sidecar"] = {
            "expected_sha256": expected_suspend.sha256,
            "actual_sha256": actual_suspend["sha256"],
            "expected_row_count": expected_suspend.row_count,
            "actual_row_count": actual_suspend["row_count"],
        }
    if mismatches:
        raise AdvisoryModelFirstError(
            "calendar or suspend sidecar differs from the frozen request",
            reason_code="ADVISORY_POLICY_UTILITY_DATASET_INVALID",
            context={"mismatches": mismatches},
        )


def _calendar_identity_sha256(calendar: pd.DatetimeIndex) -> str:
    return canonical_json_sha256({"market_sessions": [item.date().isoformat() for item in calendar]})


def _verify_policy_dataset(request: FrozenAdvisoryPolicyUtilityTrainingRequestV2) -> dict[str, Any]:
    manifest = load_policy_dataset_bundle(
        request.policy_dataset_bundle_root, expected_bundle_id=request.policy_dataset_bundle_id
    )
    manifest_path = Path(request.policy_dataset_bundle_root) / "manifest.json"
    if _sha256(manifest_path) != request.policy_dataset_manifest_file_sha256:
        raise AdvisoryModelFirstError(
            "P0-C manifest hash differs from policy utility request",
            reason_code="ADVISORY_POLICY_UTILITY_SOURCE_INVALID",
        )
    for key in (
        "program_id",
        "binding_version_id",
        "package_id",
        "manifest_sha256",
        "shadow_policy_sha256",
        "cost_policy_sha256",
        "split_policy_sha256",
    ):
        if manifest.get(key) != getattr(request, key):
            raise AdvisoryModelFirstError(
                "P0-C identity differs from policy utility request",
                reason_code="ADVISORY_POLICY_UTILITY_SOURCE_INVALID",
                context={"field": key},
            )
    return manifest


def _load_reference(
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2, reference: ExactMetaLabelReferenceV1
) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or _sha256(manifest_path) != reference.manifest_file_sha256:
        raise AdvisoryModelFirstError(
            "policy utility reference manifest differs from request",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_INVALID",
            context={"role": reference.role},
        )
    loaded = load_meta_label_bundle(root, expected_bundle_id=reference.bundle_id, load_booster=False)
    expected = {
        "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
        "program_id": request.program_id,
        "binding_version_id": request.binding_version_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "shadow_policy_sha256": request.shadow_policy_sha256,
        "model_role": "meta_label_take_skip_confidence",
    }
    mismatches = {
        key: {"expected": value, "actual": loaded["manifest"].get(key)}
        for key, value in expected.items()
        if loaded["manifest"].get(key) != value
    }
    if mismatches:
        raise AdvisoryModelFirstError(
            "policy utility reference identity differs from request",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_INVALID",
            context={"role": reference.role, "mismatches": mismatches},
        )
    try:
        winner = _read_json(root / "winner_receipt.json")
        trial_metrics = pd.read_parquet(root / "cpcv_trial_metrics.parquet")
    except (OSError, ValueError, KeyError) as exc:
        raise AdvisoryModelFirstError(
            "policy utility reference evidence cannot be read",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_INVALID",
            context={"role": reference.role},
        ) from exc
    training_objective = loaded["manifest"].get("training_objective")
    if reference.role == "LEGACY_P0_D_LINEAGE" and training_objective is not None:
        raise AdvisoryModelFirstError(
            "P0-D primary reference is not the unweighted binary baseline",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_INVALID",
        )
    if reference.role == "LEGACY_P0_E_LINEAGE" and training_objective != "OUTCOME_MAGNITUDE_WEIGHTED_BINARY_V1":
        raise AdvisoryModelFirstError(
            "P0-E diagnostic reference is not the outcome-weighted binary model",
            reason_code="ADVISORY_POLICY_UTILITY_REFERENCE_INVALID",
        )
    return {
        "manifest": loaded["manifest"],
        "winner": winner,
        "trial_metrics": trial_metrics,
    }


def _verify_training_cutoffs(request: FrozenAdvisoryPolicyUtilityTrainingRequestV2, labels: pd.DataFrame) -> None:
    matured = labels[labels["label_status"] == "MATURED"].copy()
    if matured.empty or "label_information_end" not in matured:
        raise AdvisoryModelFirstError(
            "policy utility MATURED labels or information cutoff are missing",
            reason_code="ADVISORY_POLICY_UTILITY_SOURCE_INVALID",
        )
    latest_decision = pd.to_datetime(matured["decision_as_of_trade_date"]).max().date().isoformat()
    latest_observation = pd.to_datetime(matured["label_information_end"]).max().date().isoformat()
    if (
        latest_decision != request.latest_training_decision_trade_date
        or latest_observation != request.latest_training_label_observation_trade_date
    ):
        raise AdvisoryModelFirstError(
            "policy utility label cutoffs differ from frozen request",
            reason_code="ADVISORY_POLICY_UTILITY_SOURCE_INVALID",
            context={"latest_decision": latest_decision, "latest_observation": latest_observation},
        )


def _evaluate(
    rankings: pd.DataFrame,
    predictions: pd.DataFrame,
    validation_blocks: tuple[int, ...],
    cpcv: dict[str, Any],
    candidate_daily: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspend: pd.DataFrame,
    calendar: Any,
    policy: Any,
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    cost: AdvisoryPolicyCostV1,
    suffix: str,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    return evaluate_meta_label_validation_blocks(
        rankings=rankings,
        predictions=predictions,
        validation_blocks=validation_blocks,
        block_by_date=cpcv["block_by_date"],
        daily=candidate_daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost_policy=cost,
        request_id=f"{request.request_id}_{suffix}",
    )


def _paired_metrics(metrics: dict[str, Any]) -> dict[str, float]:
    return {
        "mean_daily_net_excess_return_bps": float(metrics["mean_daily_net_excess_return_bps"]),
        "maximum_drawdown": float(metrics["maximum_drawdown"]),
        "mean_turnover_fraction": float(metrics["mean_turnover_fraction"]),
    }


def _episode_metrics(daily: pd.DataFrame, episodes: pd.DataFrame, *, target_count: int) -> dict[str, Any]:
    completed = episodes[episodes["status"] == "EXITED"] if not episodes.empty else episodes
    return {
        "policy_completed_episode_hit_rate": float((completed["net_return_bps"] > 0).mean())
        if len(completed)
        else None,
        "policy_mean_completed_episode_return_bps": float(completed["net_return_bps"].mean())
        if len(completed)
        else None,
        "policy_median_completed_episode_return_bps": float(completed["net_return_bps"].median())
        if len(completed)
        else None,
        "policy_active_slot_coverage": float(daily["active_count"].sum() / (len(daily) * target_count)),
        "policy_cash_day_count": int((daily["cash_slot_count"] > 0).sum()),
        "policy_entry_count": int(daily["entered_count"].sum()),
        "policy_exit_count": int(daily["exited_count"].sum()),
    }


def _winner_receipt(
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    winner: pd.Series,
    winner_rows: pd.DataFrame,
    final: FinalPolicyUtilityTrainingResult,
    advancement: dict[str, Any],
    final_rounds: int,
) -> dict[str, Any]:
    return {
        "schema_version": "advisory_policy_utility_arm_winner_v2",
        "arm_id": final.arm_id,
        "family_id": str(winner.family_id),
        "seed": int(winner.seed),
        "primary_metric": request.primary_metric,
        "primary_metric_value": float(winner[request.primary_metric]),
        "mean_daily_net_return_bps": float(winner.mean_daily_net_return_bps),
        "mean_maximum_drawdown": float(winner.mean_maximum_drawdown),
        "mean_turnover_fraction": float(winner.mean_turnover_fraction),
        "completed_episode_hit_rate": _optional_float(winner.completed_episode_hit_rate),
        "final_boost_rounds": final_rounds,
        "final_transform_location_bps": (final.transform.location_bps if final.transform is not None else None),
        "final_transform_scale_bps": (final.transform.scale_bps if final.transform is not None else None),
        "path_count": int(winner_rows["path_id"].nunique()),
        "tie_break": request.tie_break,
        "training_objective": next(
            item.training_objective for item in request.arm_specs if item.arm_id == final.arm_id
        ),
        "advancement_status": advancement["experiment_status"],
    }


def _rank_baseline(frame: pd.DataFrame, score_column: str) -> pd.DataFrame:
    result = frame[["decision_as_of_trade_date", "instrument", "selection_effective_rank", score_column]].copy()
    result["_missing"] = result[score_column].isna()
    result = result.sort_values(
        ["decision_as_of_trade_date", "_missing", score_column, "selection_effective_rank", "instrument"],
        ascending=[True, True, False, True, True],
    )
    result["entry_priority_rank"] = result.groupby("decision_as_of_trade_date").cumcount().add(1)
    return result.drop(columns="_missing")


def _optional_float(value: Any) -> float | None:
    numeric = float(value)
    return numeric if np.isfinite(numeric) else None


def _verify_environment(request: FrozenAdvisoryPolicyUtilityTrainingRequestV2) -> dict[str, Any]:
    if (
        os.name == "nt"
        or "microsoft" not in platform.release().lower()
        or os.getenv("CONDA_DEFAULT_ENV") != "rdagent-gpu"
    ):
        raise AdvisoryModelFirstError(
            "policy utility training requires WSL rdagent-gpu",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        )
    git = ["git.exe", "-C", request.repository_root_windows]
    commit = (
        subprocess.run([*git, "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip().lower()
    )
    if commit != request.repository_commit:
        raise AdvisoryModelFirstError(
            "policy utility repository commit mismatch",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"expected": request.repository_commit, "actual": commit},
        )
    tracked = subprocess.run(
        [*git, "status", "--porcelain", "--untracked-files=no"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if tracked:
        raise AdvisoryModelFirstError(
            "policy utility repository has uncommitted tracked changes",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"changed_file_count": len(tracked.splitlines())},
        )
    return {
        "conda_env": os.getenv("CONDA_DEFAULT_ENV"),
        "repository_commit": commit,
        "python": platform.python_version(),
        "lightgbm": importlib.metadata.version("lightgbm"),
    }


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


# Stable public orchestration helpers for later Stage-A challengers.  Keep the
# original private names for compatibility with P0-D..P0-J while new pipelines
# depend only on these explicit shared signatures.
evaluate_policy_validation_blocks = _evaluate
policy_episode_metrics = _episode_metrics
paired_policy_metrics = _paired_metrics
read_policy_json = _read_json
sha256_policy_file = _sha256
verify_policy_bound_data_identities = _verify_bound_data_identities
verify_policy_environment = _verify_environment
verify_policy_feature_v2_coverage = _verify_feature_v2_coverage
verify_policy_dataset = _verify_policy_dataset
verify_policy_source_coverage = _verify_policy_source_coverage
verify_policy_training_cutoffs = _verify_training_cutoffs
