from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.grouped_rank_output_constraint_bundle import (
    find_grouped_rank_bundle_for_request,
    publish_grouped_rank_bundle,
)
from backend.services.advisory_model_first.grouped_rank_output_constraint_contracts import (
    ExactGroupedRankReferenceV1,
    FrozenAdvisoryGroupedRankTrainingRequestV1,
)
from backend.services.advisory_model_first.grouped_rank_output_constraint_training import (
    COMBINED_SCORE_COLUMN,
    LIABILITY_SCORE_COLUMN,
    RETURN_SCORE_COLUMN,
    build_inner_fold_specs,
    combine_grouped_rank_predictions,
    grouped_rank_candidate_metrics,
    eligible_constraint_dates,
    fit_final_grouped_rank_models,
    fit_oof_price_scale,
    score_final_grouped_rank_models,
    select_minimum_feasible_oof_price,
    train_grouped_rank_oof,
)
from backend.services.advisory_model_first.dual_head_output_constraint_bundle import load_dual_head_bundle
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import build_feature_schema_payload
from backend.services.advisory_model_first.meta_label_features import build_meta_label_feature_matrix
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1, transition_policy_from_payload
from backend.services.advisory_model_first.policy_cpcv import calculate_policy_pbo
from backend.services.advisory_model_first.policy_utility_bundle import load_policy_utility_bundle
from backend.services.advisory_model_first.policy_utility_contracts import approved_policy_utility_families
from backend.services.advisory_model_first.policy_utility_pipeline import (
    PolicyUtilityProgress,
    _episode_metrics,
    _evaluate,
    _paired_metrics,
    _read_json,
    _sha256,
    _verify_bound_data_identities,
    _verify_environment,
    _verify_feature_v2_coverage,
    _verify_policy_dataset,
    _verify_policy_source_coverage,
    _verify_training_cutoffs,
    build_policy_utility_advancement_receipt,
    compare_policy_arm_rows,
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
from backend.services.advisory_model_first.turnover_constrained_utility_bundle import (
    load_turnover_constrained_utility_bundle,
)
from backend.services.advisory_model_first.turnover_constrained_utility_pipeline import (
    _evaluate_constraint_blocks,
)
from backend.services.advisory_model_first.turnover_constrained_utility_training import (
    train_fixed_p0d_reference_predictions,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


ARM_ID = "ARM_P0I_V1_GROUPED_RANK_OUTPUT_CONSTRAINED_UTILITY"


def run_grouped_rank_output_constraint_pipeline(request_path: str | Path) -> dict[str, Any]:
    try:
        request = FrozenAdvisoryGroupedRankTrainingRequestV1.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "grouped-rank frozen request cannot be read or validated",
            reason_code="ADVISORY_GROUPED_RANK_REQUEST_INVALID",
        ) from exc
    environment = _verify_environment(request)
    progress = PolicyUtilityProgress(request.resource_max_rss_bytes)
    started = time.monotonic()
    _verify_policy_dataset(request)
    p0d_reference = _load_reference(request, request.exact_p0d_reference)
    p0f_reference = _load_reference(request, request.exact_p0f_reference)
    p0g_reference = _load_reference(request, request.exact_p0g_reference)
    p0h_reference = _load_reference(request, request.exact_p0h_reference)
    existing = find_grouped_rank_bundle_for_request(request)
    if existing is not None:
        bundle_id, bundle_path, manifest = existing
        return {
            "status": "EXISTING_BUNDLE",
            "request_id": request.request_id,
            "bundle_id": bundle_id,
            "bundle_path": str(bundle_path),
            "manifest": manifest,
            "advancement": _read_json(bundle_path / "advancement_receipt.json"),
            "trial_path_count": len(pd.read_parquet(bundle_path / "cpcv_trial_metrics.parquet")),
            "activated": False,
        }
    root = Path(request.policy_dataset_bundle_root)
    rankings = pd.read_parquet(root / "candidate_rankings.parquet")
    labels = pd.read_parquet(root / "candidate_episode_labels.parquet")
    _verify_policy_source_coverage(request, rankings, labels)
    _verify_label_status_identity(request, labels)
    eligible_dates, coverage_receipt = eligible_constraint_dates(
        labels,
        expected_decision_date_count=request.expected_decision_date_count,
        expected_constraint_decision_date_count=request.expected_constraint_decision_date_count,
    )
    cpcv = _read_json(root / "cpcv_paths.json")
    policy = transition_policy_from_payload(_read_json(root / "shadow_policy.json"))
    cost = AdvisoryPolicyCostV1.model_validate_json((root / "cost_policy.json").read_text(encoding="utf-8"))
    policy_source_request = _read_json(root / "request.json")
    paths = [item for item in cpcv["paths"] if item["status"] == "READY"]
    _verify_cpcv_identity(request, paths, cpcv["block_by_date"])
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
        all_qlib_instruments(),
        start=file_history_start,
        end=data_end,
        fields=("$close", "$limit_up"),
    )
    benchmark = load_qlib_daily(
        [cost.benchmark_instrument],
        start=file_history_start,
        end=data_end,
        fields=("$open", "$close"),
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
    schema_receipt = validate_factor_file_schemas(
        request.factor_data_root,
        data_cutoff=request.factor_data_cutoff,
    )
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

    p0d_rows = _reference_winner_rows(p0d_reference, request.exact_p0d_reference, paths)
    p0f_rows = _reference_winner_rows(p0f_reference, request.exact_p0f_reference, paths)
    p0g_rows = _reference_winner_rows(p0g_reference, request.exact_p0g_reference, paths)
    p0h_rows = _reference_winner_rows(p0h_reference, request.exact_p0h_reference, paths)
    p0d_family = _p0d_family(request.exact_p0d_reference.winner_family_id)
    trial_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    constraint_receipts: list[dict[str, Any]] = []
    p0d_budget_receipts: list[dict[str, Any]] = []
    baseline_by_path: dict[str, dict[str, float]] = {}
    path_failure: dict[str, Any] | None = None
    started = time.monotonic()

    for path in paths:
        path_id = str(path["path_id"])
        train_dates = pd.DatetimeIndex(pd.to_datetime(path["train_dates"])).normalize()
        validation_dates = pd.DatetimeIndex(pd.to_datetime(path["validation_dates"])).normalize()
        validation_blocks = tuple(int(value) for value in path["validation_blocks"])
        calibration_dates = pd.DatetimeIndex(sorted(set(train_dates) & set(eligible_dates))).normalize()
        try:
            folds = build_inner_fold_specs(
                labels=labels,
                outer_train_dates=train_dates,
                eligible_dates=eligible_dates,
                block_by_date=cpcv["block_by_date"],
                trading_calendar=calendar,
                embargo_trading_days=request.inner_embargo_trading_days,
            )
            p0d_predictions = _train_p0d_oof(
                features=feature_result.features,
                labels=labels,
                folds=folds,
                family=p0d_family,
                seed=request.exact_p0d_reference.winner_seed,
                boost_rounds=request.exact_p0d_reference.winner_boost_rounds,
            )
            _verify_prediction_dates(p0d_predictions, calibration_dates)
            p0d_metrics = _evaluate_constraint_blocks(
                rankings=rankings,
                entry_priorities=p0d_predictions,
                calibration_dates=calibration_dates,
                block_by_date=cpcv["block_by_date"],
                candidate_daily=candidate_daily,
                benchmark=benchmark,
                suspend=suspend,
                calendar=calendar,
                policy=policy,
                policy_sha256=request.shadow_policy_sha256,
                cost=cost,
                request_id=f"{request.request_id}_{path_id}_p0d_oof_budget",
            )
        except AdvisoryModelFirstError as exc:
            path_failure = {"path_id": path_id, **exc.as_dict()}
            break
        p0d_budget_receipts.append(
            {
                "path_id": path_id,
                "calibration_decision_count": len(calibration_dates),
                "calibration_dates_sha256": canonical_json_sha256(
                    [value.date().isoformat() for value in calibration_dates]
                ),
                "p0d_oof_mean_turnover_fraction": float(p0d_metrics["mean_turnover_fraction"]),
                "inner_fold_count": len([fold for fold in folds if fold.score_dates]),
            }
        )
        selection_priority = rankings.loc[
            rankings["is_candidate_decision"]
            & pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize().isin(validation_dates)
            & (rankings["selection_effective_rank"] <= 20),
            ["decision_as_of_trade_date", "instrument", "selection_effective_rank"],
        ].rename(columns={"selection_effective_rank": "entry_priority_rank"})
        selection_metrics, _, _ = _evaluate(
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
        baseline_by_path[path_id] = _paired_metrics(selection_metrics)
        for family in request.family_specs:
            for seed in request.seed_roster:
                trial_id = f"{ARM_ID}_{family.family_id}_{seed}"
                try:
                    oof = train_grouped_rank_oof(
                        features=feature_result.features,
                        labels=labels,
                        folds=folds,
                        family=family,
                        seed=seed,
                        liability_clip_min=request.liability_clip_min,
                        liability_clip_max=request.liability_clip_max,
                    )
                    _verify_prediction_dates(oof.predictions, calibration_dates)
                    scale = fit_oof_price_scale(
                        oof.predictions,
                        multipliers=request.shadow_price_multipliers,
                    )

                    def evaluate_price(price: float) -> float:
                        priorities = combine_grouped_rank_predictions(oof.predictions, shadow_price=price)
                        metrics = _evaluate_constraint_blocks(
                            rankings=rankings,
                            entry_priorities=priorities,
                            calibration_dates=calibration_dates,
                            block_by_date=cpcv["block_by_date"],
                            candidate_daily=candidate_daily,
                            benchmark=benchmark,
                            suspend=suspend,
                            calendar=calendar,
                            policy=policy,
                            policy_sha256=request.shadow_policy_sha256,
                            cost=cost,
                            request_id=f"{request.request_id}_{path_id}_{family.family_id}_{seed}_{price:.12g}",
                        )
                        return float(metrics["mean_turnover_fraction"])

                    price_selection = select_minimum_feasible_oof_price(
                        scale=scale,
                        p0d_oof_turnover_budget=float(p0d_metrics["mean_turnover_fraction"]),
                        evaluate_turnover=evaluate_price,
                    )
                    return_rounds = max(1, int(np.median(oof.return_best_iterations)))
                    liability_rounds = max(1, int(np.median(oof.liability_best_iterations)))
                    models = fit_final_grouped_rank_models(
                        features=feature_result.features,
                        labels=labels,
                        train_dates=train_dates,
                        family=family,
                        seed=seed,
                        return_boost_rounds=return_rounds,
                        liability_boost_rounds=liability_rounds,
                    )
                    validation_predictions = score_final_grouped_rank_models(
                        features=feature_result.features,
                        models=models,
                        score_dates=validation_dates,
                        liability_clip_min=request.liability_clip_min,
                        liability_clip_max=request.liability_clip_max,
                    )
                    combined = combine_grouped_rank_predictions(
                        validation_predictions,
                        shadow_price=price_selection.shadow_price_score_per_fraction,
                    )
                    diagnostic_rows = _attach_labels(combined, labels)
                    candidate_metrics = grouped_rank_candidate_metrics(diagnostic_rows)
                    policy_metrics, policy_daily, policy_episodes = _evaluate(
                        rankings,
                        combined,
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
                    path_failure = {
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        **exc.as_dict(),
                    }
                    break
                constraint_receipts.append(
                    {
                        "trial_id": trial_id,
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        "calibration_decision_count": len(calibration_dates),
                        "calibration_dates_sha256": canonical_json_sha256(
                            [value.date().isoformat() for value in calibration_dates]
                        ),
                        "return_scale_score": scale.return_scale_score,
                        "liability_scale": scale.liability_scale,
                        "base_price_score_per_fraction": scale.base_price_score_per_fraction,
                        "return_boost_rounds": return_rounds,
                        "liability_boost_rounds": liability_rounds,
                        "inner_folds": list(oof.fold_receipts),
                        **price_selection.__dict__,
                    }
                )
                trial_rows.append(
                    {
                        "trial_id": trial_id,
                        "arm_id": ARM_ID,
                        "return_training_objective": request.return_training_objective,
                        "liability_training_objective": request.liability_training_objective,
                        "family_id": family.family_id,
                        "seed": seed,
                        "path_id": path_id,
                        "validation_blocks": list(validation_blocks),
                        **candidate_metrics,
                        **{f"policy_{key}": value for key, value in policy_metrics.items() if key != "block_metrics"},
                        **_episode_metrics(policy_daily, policy_episodes, target_count=policy.target_count),
                        "selection_baseline_mean_daily_net_excess_return_bps": baseline_by_path[path_id][
                            request.primary_metric
                        ],
                        "policy_lift_bps": policy_metrics[request.primary_metric]
                        - baseline_by_path[path_id][request.primary_metric],
                        "shadow_price_score_per_fraction": price_selection.shadow_price_score_per_fraction,
                        "p0d_oof_turnover_budget": price_selection.p0d_oof_turnover_budget,
                        "p0i_oof_turnover": price_selection.p0i_oof_turnover,
                        "oof_constraint_slack": price_selection.constraint_slack,
                        "return_best_iteration": return_rounds,
                        "liability_best_iteration": liability_rounds,
                    }
                )
                for item in policy_metrics["block_metrics"]:
                    block_rows.append(
                        {
                            "trial_id": trial_id,
                            "arm_id": ARM_ID,
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

    trial_metrics = pd.DataFrame(trial_rows)
    constraint_payload = {
        "schema_version": "advisory_grouped_rank_inner_oof_constraint_v1",
        "coverage": coverage_receipt,
        "p0d_path_budgets": p0d_budget_receipts,
        "trial_constraints": constraint_receipts,
        "failure": path_failure,
    }
    if path_failure is not None:
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            baseline_by_path=baseline_by_path,
            constraint_receipt=constraint_payload,
            references=(p0d_reference, p0f_reference, p0g_reference, p0h_reference),
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    if len(trial_metrics) != request.expected_outer_trial_path_count:
        raise AdvisoryModelFirstError(
            "grouped-rank outer trial roster is incomplete",
            reason_code="ADVISORY_GROUPED_RANK_OUTER_ROSTER_INVALID",
            context={"trial_path_count": len(trial_metrics)},
        )
    raw_blocks = pd.DataFrame(block_rows)
    block_scores = raw_blocks.groupby(
        ["trial_id", "arm_id", "family_id", "seed", "block_id"],
        as_index=False,
    )[request.primary_metric].mean()
    pbo = calculate_policy_pbo(
        block_scores[["trial_id", "block_id", request.primary_metric]],
        group_count=request.expected_cpcv_block_count,
        metric_column=request.primary_metric,
    )
    summary = trial_metrics.groupby(["family_id", "seed"], as_index=False).agg(
        mean_daily_net_excess_return_bps=("policy_mean_daily_net_excess_return_bps", "mean"),
        mean_daily_net_return_bps=("policy_mean_daily_net_return_bps", "mean"),
        mean_maximum_drawdown=("policy_maximum_drawdown", "mean"),
        mean_turnover_fraction=("policy_mean_turnover_fraction", "mean"),
        completed_episode_hit_rate=("policy_completed_episode_hit_rate", "mean"),
    )
    winner = summary.sort_values(
        [request.primary_metric, "family_id", "seed"],
        ascending=[False, True, True],
    ).iloc[0]
    winner_rows = trial_metrics[
        (trial_metrics["family_id"] == winner.family_id) & (trial_metrics["seed"] == winner.seed)
    ].copy()
    p0d_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0d_rows,
        reference_role="ARM_P0D_V2_BINARY_PARITY",
    )
    p0f_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0f_rows,
        reference_role="ARM_P0F_V2_HUBER_UTILITY",
    )
    p0g_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0g_rows,
        reference_role="ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
    )
    p0h_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0h_rows,
        reference_role="ARM_P0H_V1_DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY",
    )
    selection_lift = float(
        winner_rows["policy_mean_daily_net_excess_return_bps"].mean()
        - np.mean([item[request.primary_metric] for item in baseline_by_path.values()])
    )
    advancement = build_policy_utility_advancement_receipt(
        p0d_comparison=p0d_comparison,
        candidate_minus_selection_mean_primary_metric_bps=selection_lift,
        candidate_path_ids=winner_rows["path_id"].tolist(),
    )

    all_dates = pd.DatetimeIndex(pd.to_datetime(labels["decision_as_of_trade_date"].unique())).normalize()
    final_folds = build_inner_fold_specs(
        labels=labels,
        outer_train_dates=all_dates,
        eligible_dates=eligible_dates,
        block_by_date=cpcv["block_by_date"],
        trading_calendar=calendar,
        embargo_trading_days=request.inner_embargo_trading_days,
    )
    family = next(item for item in request.family_specs if item.family_id == winner.family_id)
    final_oof = train_grouped_rank_oof(
        features=feature_result.features,
        labels=labels,
        folds=final_folds,
        family=family,
        seed=int(winner.seed),
        liability_clip_min=request.liability_clip_min,
        liability_clip_max=request.liability_clip_max,
    )
    final_p0d_oof = _train_p0d_oof(
        features=feature_result.features,
        labels=labels,
        folds=final_folds,
        family=p0d_family,
        seed=request.exact_p0d_reference.winner_seed,
        boost_rounds=request.exact_p0d_reference.winner_boost_rounds,
    )
    final_p0d_metrics = _evaluate_constraint_blocks(
        rankings=rankings,
        entry_priorities=final_p0d_oof,
        calibration_dates=eligible_dates,
        block_by_date=cpcv["block_by_date"],
        candidate_daily=candidate_daily,
        benchmark=benchmark,
        suspend=suspend,
        calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost=cost,
        request_id=f"{request.request_id}_final_p0d_oof_budget",
    )
    final_scale = fit_oof_price_scale(
        final_oof.predictions,
        multipliers=request.shadow_price_multipliers,
    )

    def evaluate_final_price(price: float) -> float:
        priorities = combine_grouped_rank_predictions(final_oof.predictions, shadow_price=price)
        metrics = _evaluate_constraint_blocks(
            rankings=rankings,
            entry_priorities=priorities,
            calibration_dates=eligible_dates,
            block_by_date=cpcv["block_by_date"],
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            policy_sha256=request.shadow_policy_sha256,
            cost=cost,
            request_id=f"{request.request_id}_final_{price:.12g}",
        )
        return float(metrics["mean_turnover_fraction"])

    final_price = select_minimum_feasible_oof_price(
        scale=final_scale,
        p0d_oof_turnover_budget=float(final_p0d_metrics["mean_turnover_fraction"]),
        evaluate_turnover=evaluate_final_price,
    )
    final_return_rounds = max(1, int(np.median(final_oof.return_best_iterations)))
    final_liability_rounds = max(1, int(np.median(final_oof.liability_best_iterations)))
    final_models = fit_final_grouped_rank_models(
        features=feature_result.features,
        labels=labels,
        train_dates=all_dates,
        family=family,
        seed=int(winner.seed),
        return_boost_rounds=final_return_rounds,
        liability_boost_rounds=final_liability_rounds,
    )
    progress.add(
        "trials_and_final",
        started,
        trial_path_count=len(trial_metrics),
        winner={"family_id": str(winner.family_id), "seed": int(winner.seed)},
    )
    feature_schema = build_feature_schema_payload(
        market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
        suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
    )
    feature_schema.update(
        {
            "feature_schema_hash": request.feature_schema_hash,
            "trained_feature_names": list(final_models.feature_names),
            "categorical_vocabulary": {
                key: list(value) for key, value in final_models.categorical_vocabulary.items()
            },
            "prediction_columns": [RETURN_SCORE_COLUMN, LIABILITY_SCORE_COLUMN, COMBINED_SCORE_COLUMN],
            "entry_priority_score_kind": "GROUPED_RANK_OUTPUT_CONSTRAINED_PERCENTILE_V1",
        }
    )
    winner_receipt = {
        "schema_version": "advisory_grouped_rank_winner_v1",
        "arm_id": ARM_ID,
        "family_id": str(winner.family_id),
        "seed": int(winner.seed),
        "path_count": len(winner_rows),
        "primary_metric": request.primary_metric,
        "primary_metric_value": float(winner[request.primary_metric]),
        "mean_daily_net_return_bps": float(winner.mean_daily_net_return_bps),
        "mean_maximum_drawdown": float(winner.mean_maximum_drawdown),
        "mean_turnover_fraction": float(winner.mean_turnover_fraction),
        "completed_episode_hit_rate": float(winner.completed_episode_hit_rate),
        "final_return_boost_rounds": final_return_rounds,
        "final_liability_boost_rounds": final_liability_rounds,
        "final_shadow_price_score_per_fraction": final_price.shadow_price_score_per_fraction,
        "advancement_status": advancement["experiment_status"],
    }
    constraint_payload["final"] = {
        "return_scale_score": final_scale.return_scale_score,
        "liability_scale": final_scale.liability_scale,
        "base_price_score_per_fraction": final_scale.base_price_score_per_fraction,
        "return_boost_rounds": final_return_rounds,
        "liability_boost_rounds": final_liability_rounds,
        "inner_folds": list(final_oof.fold_receipts),
        **final_price.__dict__,
    }
    baseline = {
        "schema_version": "advisory_grouped_rank_baselines_v1",
        "selection_path_metrics": baseline_by_path,
        "selection_mean_primary_metric": float(
            np.mean([item[request.primary_metric] for item in baseline_by_path.values()])
        ),
    }
    references = {
        "schema_version": "advisory_grouped_rank_reference_comparison_v1",
        "p0d_v2_advancement": p0d_comparison,
        "p0f_v2_diagnostic": p0f_comparison,
        "p0g_v1_diagnostic": p0g_comparison,
        "p0h_v1_diagnostic": p0h_comparison,
        "p0f_is_advancement_gate": False,
        "p0g_is_advancement_gate": False,
        "p0h_is_advancement_gate": False,
        "p0d_bundle_id": request.exact_p0d_reference.bundle_id,
        "p0f_bundle_id": request.exact_p0f_reference.bundle_id,
        "p0g_bundle_id": request.exact_p0g_reference.bundle_id,
        "p0h_bundle_id": request.exact_p0h_reference.bundle_id,
    }
    diagnostics = {
        "schema_version": "advisory_grouped_rank_candidate_diagnostics_v1",
        "winner_trial_summary": _diagnostic_summary(winner_rows),
        "all_trial_summary": _diagnostic_summary(trial_metrics),
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_grouped_rank_bundle(
        request=request,
        return_booster=final_models.return_booster,
        liability_booster=final_models.liability_booster,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        inner_oof_constraint_receipt=constraint_payload,
        transform_receipt={
            "schema_version": "advisory_grouped_rank_transform_v1",
            "return": {
                "kind": "WITHIN_DECISION_DATE_AVERAGE_TIE_PERCENTILE_V1",
                "minimum": 0.0,
                "maximum": 1.0,
            },
            "liability": {
                "location": final_models.liability_transform.location_bps,
                "scale": final_models.liability_transform.scale_bps,
                "clip_min": request.liability_clip_min,
                "clip_max": request.liability_clip_max,
            },
            "shadow_price_score_per_fraction": final_price.shadow_price_score_per_fraction,
            "fit_scope": "ALL_P0_C_MATURED_ROWS_WITH_8_BLOCK_OOF_STATE",
        },
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        candidate_diagnostics=diagnostics,
        pbo_receipt=pbo,
        winner_receipt=winner_receipt,
        baseline_comparison=baseline,
        reference_comparison=references,
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt.__dict__,
            "trial_summary": summary.to_dict("records"),
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
        "manifest": manifest,
        "winner": winner_receipt,
        "pbo": pbo,
        "advancement": advancement,
        "trial_path_count": len(trial_metrics),
        "resource_report": resource,
        "activated": False,
    }


def _train_p0d_oof(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    folds: Sequence[Any],
    family: Any,
    seed: int,
    boost_rounds: int,
) -> pd.DataFrame:
    parts = []
    for fold in folds:
        if not fold.score_dates:
            continue
        parts.append(
            train_fixed_p0d_reference_predictions(
                features=features,
                labels=labels,
                train_dates=fold.train_dates,
                score_dates=fold.score_dates,
                family=family,
                seed=seed,
                boost_rounds=boost_rounds,
            )
        )
    if not parts:
        raise AdvisoryModelFirstError(
            "exact P0-D OOF produced no predictions",
            reason_code="ADVISORY_GROUPED_RANK_P0D_OOF_INVALID",
        )
    result = pd.concat(parts, ignore_index=True)
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "exact P0-D OOF predictions contain duplicates",
            reason_code="ADVISORY_GROUPED_RANK_P0D_OOF_INVALID",
        )
    return result


def _verify_prediction_dates(predictions: pd.DataFrame, expected_dates: Sequence[pd.Timestamp]) -> None:
    actual = set(pd.DatetimeIndex(pd.to_datetime(predictions["decision_as_of_trade_date"])).normalize())
    expected = set(pd.DatetimeIndex(pd.to_datetime(list(expected_dates))).normalize())
    counts = predictions.groupby(pd.to_datetime(predictions["decision_as_of_trade_date"]).dt.normalize()).size()
    if actual != expected or counts.empty or not counts.eq(20).all():
        raise AdvisoryModelFirstError(
            "grouped-rank/P0-D OOF prediction dates differ from exact calibration dates",
            reason_code="ADVISORY_GROUPED_RANK_OOF_DATE_MISMATCH",
        )


def _attach_labels(predictions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    columns = keys + ["label_status", "net_excess_return_bps", "holding_trading_days"]
    attached = predictions.merge(labels[columns], on=keys, how="left", validate="one_to_one")
    liability = np.where(
        attached["label_status"] == "MATURED",
        2.0 / (5.0 * pd.to_numeric(attached["holding_trading_days"], errors="coerce")),
        np.nan,
    )
    attached["turnover_liability_fraction_per_day"] = liability
    return attached


def _load_reference(
    request: FrozenAdvisoryGroupedRankTrainingRequestV1,
    reference: ExactGroupedRankReferenceV1,
) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or _sha256(manifest_path) != reference.manifest_file_sha256:
        raise AdvisoryModelFirstError(
            "grouped-rank reference manifest differs from request",
            reason_code="ADVISORY_GROUPED_RANK_REFERENCE_MISMATCH",
            context={"role": reference.role},
        )
    if reference.role == "P0G_V1_REFERENCE":
        loaded = load_turnover_constrained_utility_bundle(
            root,
            expected_bundle_id=reference.bundle_id,
            load_booster=False,
        )
        winner = _read_json(root / "winner_receipt.json")
        feature_schema = _read_json(root / "feature_schema.json")
    elif reference.role == "P0H_V1_REFERENCE":
        loaded = load_dual_head_bundle(
            root,
            expected_bundle_id=reference.bundle_id,
            load_boosters=False,
        )
        winner = _read_json(root / "winner_receipt.json")
        feature_schema = _read_json(root / "dual_head_feature_schema.json")
    else:
        loaded = load_policy_utility_bundle(
            root,
            expected_bundle_id=reference.bundle_id,
            load_booster=False,
        )
        winner = _read_json(root / "winner_receipt.json")["winner_by_arm"].get(reference.arm_id)
        feature_schema = _read_json(root / "utility_feature_schema.json")
    expected = {
        "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
        "program_id": request.program_id,
        "binding_version_id": request.binding_version_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "shadow_policy_sha256": request.shadow_policy_sha256,
        "feature_schema_hash": request.feature_schema_hash,
    }
    mismatches = {
        key: {"expected": value, "actual": loaded["manifest"].get(key)}
        for key, value in expected.items()
        if loaded["manifest"].get(key) != value
    }
    if mismatches:
        raise AdvisoryModelFirstError(
            "grouped-rank reference identity differs from request",
            reason_code="ADVISORY_GROUPED_RANK_REFERENCE_MISMATCH",
            context={"role": reference.role, "mismatches": mismatches},
        )
    if not winner or (
        winner.get("family_id") != reference.winner_family_id
        or int(winner.get("seed", -1)) != reference.winner_seed
        or _winner_objective(winner, reference.role) != reference.winner_training_objective
        or _winner_rounds(winner, reference.role) != reference.winner_boost_rounds
    ):
        raise AdvisoryModelFirstError(
            "grouped-rank reference winner identity differs from request",
            reason_code="ADVISORY_GROUPED_RANK_REFERENCE_MISMATCH",
            context={"role": reference.role},
        )
    return {
        "root": root,
        "loaded": loaded,
        "winner": winner,
        "trial_metrics": pd.read_parquet(root / "cpcv_trial_metrics.parquet"),
        "feature_schema": feature_schema,
    }


def _winner_objective(winner: dict[str, Any], role: str) -> str:
    if role == "P0G_V1_REFERENCE":
        return "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"
    if role == "P0H_V1_REFERENCE":
        return "P0H_DUAL_HEAD_OUTPUT_CONSTRAINT_V1"
    return str(winner.get("training_objective"))


def _winner_rounds(winner: dict[str, Any], role: str) -> int:
    if role == "P0H_V1_REFERENCE":
        return int(winner.get("final_return_boost_rounds", 0))
    return int(winner.get("final_boost_rounds", 0))


def _reference_winner_rows(
    reference: dict[str, Any],
    specification: ExactGroupedRankReferenceV1,
    paths: list[dict[str, Any]],
) -> pd.DataFrame:
    rows = reference["trial_metrics"]
    selected = rows[
        (rows["arm_id"] == specification.arm_id)
        & (rows["family_id"] == specification.winner_family_id)
        & (rows["seed"] == specification.winner_seed)
    ].copy()
    expected_paths = {str(item["path_id"]) for item in paths}
    if len(selected) != len(expected_paths) or set(selected["path_id"]) != expected_paths:
        raise AdvisoryModelFirstError(
            "grouped-rank reference does not contain exact 28 winner paths",
            reason_code="ADVISORY_GROUPED_RANK_REFERENCE_MISMATCH",
            context={"role": specification.role},
        )
    return selected


def _p0d_family(family_id: str):
    matches = [item for item in approved_policy_utility_families() if item.family_id == family_id]
    if len(matches) != 1:
        raise AdvisoryModelFirstError(
            "exact P0-D winner family is not approved",
            reason_code="ADVISORY_GROUPED_RANK_REFERENCE_MISMATCH",
        )
    return matches[0]


def _verify_label_status_identity(
    request: FrozenAdvisoryGroupedRankTrainingRequestV1,
    labels: pd.DataFrame,
) -> None:
    actual = {str(key): int(value) for key, value in labels["label_status"].value_counts().items()}
    matured = int((labels["label_status"] == "MATURED").sum())
    if (
        len(labels) != request.expected_candidate_row_count
        or matured != request.expected_matured_row_count
        or actual != request.expected_label_status_counts
    ):
        raise AdvisoryModelFirstError(
            "grouped-rank label status identity differs from frozen P0-C",
            reason_code="ADVISORY_GROUPED_RANK_LABEL_INVALID",
            context={"actual": actual, "expected": request.expected_label_status_counts},
        )


def _verify_cpcv_identity(
    request: FrozenAdvisoryGroupedRankTrainingRequestV1,
    paths: list[dict[str, Any]],
    block_by_date: dict[str, int],
) -> None:
    if (
        len(paths) != request.expected_outer_path_count
        or len({item["path_id"] for item in paths}) != len(paths)
        or set(block_by_date.values()) != set(range(request.expected_cpcv_block_count))
        or any(
            len(item["validation_blocks"]) != request.expected_outer_validation_block_count
            for item in paths
        )
    ):
        raise AdvisoryModelFirstError(
            "grouped-rank CPCV identity differs from frozen P0-C",
            reason_code="ADVISORY_GROUPED_RANK_OUTER_ROSTER_INVALID",
        )


def _diagnostic_summary(rows: pd.DataFrame) -> dict[str, Any]:
    columns = [
        "return_mae",
        "return_rmse",
        "return_daily_spearman_mean",
        "return_daily_spearman_null_count",
        "liability_mae",
        "liability_rmse",
        "liability_daily_spearman_mean",
        "liability_daily_spearman_null_count",
        "liability_clip_low_count",
        "liability_clip_high_count",
        "top5_vs_rest_raw_return_spread_bps",
    ]
    return {
        "row_count": len(rows),
        "means": {
            column: (float(pd.to_numeric(rows[column], errors="coerce").mean()) if column in rows else None)
            for column in columns
        },
    }


def _publish_incomplete(
    *,
    request: FrozenAdvisoryGroupedRankTrainingRequestV1,
    feature_result: Any,
    trial_metrics: pd.DataFrame,
    block_rows: list[dict[str, Any]],
    baseline_by_path: dict[str, dict[str, float]],
    constraint_receipt: dict[str, Any],
    references: tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]],
    environment: dict[str, Any],
    schema_receipt: dict[str, Any],
    progress: PolicyUtilityProgress,
) -> dict[str, Any]:
    advancement = {
        "schema_version": "advisory_policy_utility_advancement_v1",
        "experiment_status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "advanced_to_stage_b": False,
        "stage_b_guard": "DENY_INCOMPLETE_CPCV",
        "failure": constraint_receipt.get("failure"),
    }
    block_scores = pd.DataFrame(block_rows)
    if not block_scores.empty:
        block_scores = block_scores.groupby(
            ["trial_id", "arm_id", "family_id", "seed", "block_id"],
            as_index=False,
        )[request.primary_metric].mean()
    feature_schema = build_feature_schema_payload(
        market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
        suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
    )
    feature_schema.update({"feature_schema_hash": request.feature_schema_hash, "trained_feature_names": []})
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_grouped_rank_bundle(
        request=request,
        return_booster=None,
        liability_booster=None,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        inner_oof_constraint_receipt=constraint_receipt,
        transform_receipt={"schema_version": "advisory_grouped_rank_transform_v1", "status": "INCOMPLETE"},
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        candidate_diagnostics={"schema_version": "advisory_grouped_rank_candidate_diagnostics_v1", "status": "INCOMPLETE"},
        pbo_receipt={"schema_version": "advisory_policy_pbo_v1", "status": "NOT_COMPUTABLE"},
        winner_receipt={"schema_version": "advisory_grouped_rank_winner_v1", "status": "INCOMPLETE"},
        baseline_comparison={
            "schema_version": "advisory_grouped_rank_baselines_v1",
            "selection_path_metrics": baseline_by_path,
        },
        reference_comparison={
            "schema_version": "advisory_grouped_rank_reference_comparison_v1",
            "p0d_bundle_id": references[0]["loaded"]["manifest"]["bundle_id"],
            "p0f_bundle_id": references[1]["loaded"]["manifest"]["bundle_id"],
            "p0g_bundle_id": references[2]["loaded"]["manifest"]["bundle_id"],
            "p0h_bundle_id": references[3]["loaded"]["manifest"]["bundle_id"],
        },
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt,
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
        "manifest": manifest,
        "advancement": advancement,
        "trial_path_count": len(trial_metrics),
        "activated": False,
    }
