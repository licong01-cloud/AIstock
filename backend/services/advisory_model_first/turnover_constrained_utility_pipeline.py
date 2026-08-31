from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd

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
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.advisory_model_first.turnover_constrained_utility_bundle import (
    find_turnover_constrained_utility_bundle_for_request,
    publish_turnover_constrained_utility_bundle,
)
from backend.services.advisory_model_first.turnover_constrained_utility_contracts import (
    ExactTurnoverUtilityReferenceV1,
    FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1,
)
from backend.services.advisory_model_first.turnover_constrained_utility_training import (
    SCORE_COLUMN,
    add_turnover_constrained_targets,
    complete_matured_decision_dates,
    fit_shadow_price_scale,
    rank_turnover_utility_predictions,
    score_exact_p0d_reference_booster,
    select_minimum_feasible_shadow_price,
    train_final_turnover_constrained_utility,
    train_fixed_p0d_reference_predictions,
    train_turnover_constrained_utility_trial,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


ARM_ID = "ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY"


def run_turnover_constrained_utility_pipeline(request_path: str | Path) -> dict[str, Any]:
    try:
        request = FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "turnover utility frozen request cannot be read or validated",
            reason_code="ADVISORY_TURNOVER_UTILITY_REQUEST_INVALID",
        ) from exc
    environment = _verify_environment(request)
    progress = PolicyUtilityProgress(request.resource_max_rss_bytes)
    started = time.monotonic()
    _verify_policy_dataset(request)
    p0d_reference = _load_exact_reference(request, request.exact_p0d_reference, load_booster=True)
    p0f_reference = _load_exact_reference(request, request.exact_p0f_reference, load_booster=False)
    existing = find_turnover_constrained_utility_bundle_for_request(request)
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
    cpcv = _read_json(root / "cpcv_paths.json")
    policy = transition_policy_from_payload(_read_json(root / "shadow_policy.json"))
    cost = AdvisoryPolicyCostV1.model_validate_json((root / "cost_policy.json").read_text(encoding="utf-8"))
    policy_source_request = _read_json(root / "request.json")
    paths = [item for item in cpcv["paths"] if item["status"] == "READY"]
    if len(paths) != request.expected_cpcv_path_count or len({item["path_id"] for item in paths}) != len(paths):
        raise AdvisoryModelFirstError(
            "turnover utility requires all 28 unique READY CPCV paths",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
        )
    _verify_training_cutoffs(request, labels)
    complete_dates, coverage_receipt = complete_matured_decision_dates(
        labels,
        expected_candidates_per_date=request.expected_candidates_per_date,
    )
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

    p0d_rows = _reference_winner_rows(p0d_reference, request.exact_p0d_reference, paths)
    p0f_rows = _reference_winner_rows(p0f_reference, request.exact_p0f_reference, paths)
    p0d_family = _p0d_family(request.exact_p0d_reference.winner_family_id)
    trial_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    path_constraint_receipts: list[dict[str, Any]] = []
    baseline_by_path: dict[str, dict[str, float]] = {}
    path_failure: dict[str, Any] | None = None
    started = time.monotonic()

    for path in paths:
        path_id = str(path["path_id"])
        train_dates = pd.DatetimeIndex(pd.to_datetime(path["train_dates"])).normalize()
        validation_dates = pd.DatetimeIndex(pd.to_datetime(path["validation_dates"])).normalize()
        validation_blocks = tuple(int(value) for value in path["validation_blocks"])
        calibration_dates = pd.DatetimeIndex(sorted(set(train_dates) & set(complete_dates))).normalize()
        if calibration_dates.empty:
            path_failure = _failure(
                path_id,
                "ADVISORY_TURNOVER_UTILITY_CALIBRATION_COVERAGE_INVALID",
                "path has no exact-20 matured train calibration date",
            )
            break
        path_labels = labels[
            pd.to_datetime(labels["decision_as_of_trade_date"]).dt.normalize().isin(train_dates)
            & (labels["label_status"] == "MATURED")
        ].copy()
        try:
            p0d_path = p0d_rows[p0d_rows["path_id"] == path_id]
            if len(p0d_path) != 1:
                raise AdvisoryModelFirstError(
                    "exact P0-D reference path evidence is missing",
                    reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
                )
            p0d_predictions = train_fixed_p0d_reference_predictions(
                features=feature_result.features,
                labels=labels,
                train_dates=train_dates,
                score_dates=calibration_dates,
                family=p0d_family,
                seed=request.exact_p0d_reference.winner_seed,
                boost_rounds=int(p0d_path.iloc[0]["best_iteration"]),
            )
            p0d_train_metrics = _evaluate_constraint_blocks(
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
                request_id=f"{request.request_id}_{path_id}_p0d_train_budget",
            )
            scale_fit = fit_shadow_price_scale(
                path_labels,
                target_count=policy.target_count,
                multipliers=request.shadow_price_multipliers,
            )

            def oracle_turnover(price: float) -> float:
                priorities = _oracle_priorities(
                    labels=labels,
                    decision_dates=calibration_dates,
                    target_count=policy.target_count,
                    shadow_price=price,
                )
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
                    request_id=f"{request.request_id}_{path_id}_{format(price, '.12g')}",
                )
                return float(metrics["mean_turnover_fraction"])

            selection = select_minimum_feasible_shadow_price(
                scale_fit=scale_fit,
                p0d_train_turnover_budget=float(p0d_train_metrics["mean_turnover_fraction"]),
                evaluate_oracle_turnover=oracle_turnover,
            )
        except AdvisoryModelFirstError as exc:
            path_failure = {"path_id": path_id, **exc.as_dict()}
            break
        path_constraint_receipts.append(
            {
                "path_id": path_id,
                "calibration_decision_count": len(calibration_dates),
                "calibration_dates_sha256": canonical_json_sha256(
                    [value.date().isoformat() for value in calibration_dates]
                ),
                "utility_scale_bps": scale_fit.utility_scale_bps,
                "liability_scale": scale_fit.liability_scale,
                "shadow_price_base_bps_per_fraction": scale_fit.shadow_price_base_bps_per_fraction,
                **selection.__dict__,
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
                    result = train_turnover_constrained_utility_trial(
                        features=feature_result.features,
                        labels=labels,
                        train_dates=train_dates,
                        validation_dates=validation_dates,
                        family=family,
                        seed=seed,
                        target_count=policy.target_count,
                        shadow_price_bps_per_fraction=selection.shadow_price_bps_per_fraction,
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
                    path_failure = {
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        **exc.as_dict(),
                    }
                    break
                trial_rows.append(
                    {
                        "trial_id": trial_id,
                        "arm_id": ARM_ID,
                        "training_objective": request.training_objective,
                        "family_id": family.family_id,
                        "seed": seed,
                        "path_id": path_id,
                        "validation_blocks": list(validation_blocks),
                        **result.metrics,
                        **{f"policy_{key}": value for key, value in policy_metrics.items() if key != "block_metrics"},
                        **_episode_metrics(policy_daily, policy_episodes, target_count=policy.target_count),
                        "selection_baseline_mean_daily_net_excess_return_bps": baseline_by_path[path_id][
                            request.primary_metric
                        ],
                        "policy_lift_bps": policy_metrics[request.primary_metric]
                        - baseline_by_path[path_id][request.primary_metric],
                        "shadow_price_bps_per_fraction": selection.shadow_price_bps_per_fraction,
                        "train_turnover_budget": selection.p0d_train_turnover_budget,
                        "oracle_train_turnover": selection.oracle_train_turnover,
                        "train_constraint_slack": selection.constraint_slack,
                        "transform_location_bps": result.transform.location_bps,
                        "transform_scale_bps": result.transform.scale_bps,
                        "best_iteration": result.best_iteration,
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
    if path_failure is not None:
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            baseline_by_path=baseline_by_path,
            constraint_receipt={
                "schema_version": "advisory_turnover_utility_constraint_v1",
                "coverage": coverage_receipt,
                "paths": path_constraint_receipts,
                "failure": path_failure,
            },
            p0d_reference=p0d_reference,
            p0f_reference=p0f_reference,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    if len(trial_metrics) != request.expected_trial_path_count:
        raise AdvisoryModelFirstError(
            "turnover utility trial roster is incomplete",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
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
    summary = trial_metrics.groupby(["family_id", "seed"], as_index=False).agg(
        mean_daily_net_excess_return_bps=("policy_mean_daily_net_excess_return_bps", "mean"),
        mean_daily_net_return_bps=("policy_mean_daily_net_return_bps", "mean"),
        mean_maximum_drawdown=("policy_maximum_drawdown", "mean"),
        mean_turnover_fraction=("policy_mean_turnover_fraction", "mean"),
        completed_episode_hit_rate=("policy_completed_episode_hit_rate", "mean"),
    )
    winner = summary.sort_values(
        [request.primary_metric, "family_id", "seed"], ascending=[False, True, True]
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
    selection_lift = float(
        winner_rows["policy_mean_daily_net_excess_return_bps"].mean()
        - np.mean([item[request.primary_metric] for item in baseline_by_path.values()])
    )
    advancement = build_policy_utility_advancement_receipt(
        p0d_comparison=p0d_comparison,
        candidate_minus_selection_mean_primary_metric_bps=selection_lift,
        candidate_path_ids=winner_rows["path_id"].tolist(),
    )

    all_matured = labels[labels["label_status"] == "MATURED"].copy()
    final_scale = fit_shadow_price_scale(
        all_matured,
        target_count=policy.target_count,
        multipliers=request.shadow_price_multipliers,
    )
    final_p0d_predictions = score_exact_p0d_reference_booster(
        features=feature_result.features,
        booster=p0d_reference["loaded"]["arm_boosters"]["ARM_P0D_V2_BINARY_PARITY"],
        feature_names=p0d_reference["feature_schema"]["trained_feature_names_by_arm"][
            "ARM_P0D_V2_BINARY_PARITY"
        ],
        categorical_vocabulary=p0d_reference["feature_schema"]["categorical_vocabulary_by_arm"][
            "ARM_P0D_V2_BINARY_PARITY"
        ],
        score_dates=complete_dates,
    )
    final_p0d_metrics = _evaluate_constraint_blocks(
        rankings=rankings,
        entry_priorities=final_p0d_predictions,
        calibration_dates=complete_dates,
        block_by_date=cpcv["block_by_date"],
        candidate_daily=candidate_daily,
        benchmark=benchmark,
        suspend=suspend,
        calendar=calendar,
        policy=policy,
        policy_sha256=request.shadow_policy_sha256,
        cost=cost,
        request_id=f"{request.request_id}_final_p0d_budget",
    )

    def final_oracle_turnover(price: float) -> float:
        priorities = _oracle_priorities(
            labels=labels,
            decision_dates=complete_dates,
            target_count=policy.target_count,
            shadow_price=price,
        )
        metrics = _evaluate_constraint_blocks(
            rankings=rankings,
            entry_priorities=priorities,
            calibration_dates=complete_dates,
            block_by_date=cpcv["block_by_date"],
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            policy_sha256=request.shadow_policy_sha256,
            cost=cost,
            request_id=f"{request.request_id}_final_{format(price, '.12g')}",
        )
        return float(metrics["mean_turnover_fraction"])

    final_selection = select_minimum_feasible_shadow_price(
        scale_fit=final_scale,
        p0d_train_turnover_budget=float(final_p0d_metrics["mean_turnover_fraction"]),
        evaluate_oracle_turnover=final_oracle_turnover,
    )
    family = next(item for item in request.family_specs if item.family_id == winner.family_id)
    winner_rounds = int(np.median(winner_rows["best_iteration"]))
    final = train_final_turnover_constrained_utility(
        features=feature_result.features,
        labels=labels,
        family=family,
        seed=int(winner.seed),
        boost_rounds=winner_rounds,
        target_count=policy.target_count,
        shadow_price_bps_per_fraction=final_selection.shadow_price_bps_per_fraction,
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
            "trained_feature_names": list(final.feature_names),
            "categorical_vocabulary": {key: list(value) for key, value in final.categorical_vocabulary.items()},
            "prediction_column": SCORE_COLUMN,
            "entry_priority_score_kind": "TURNOVER_CONSTRAINED_POLICY_UTILITY_BPS",
        }
    )
    winner_receipt = {
        "schema_version": "advisory_turnover_utility_winner_v1",
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
        "final_boost_rounds": winner_rounds,
        "final_shadow_price_bps_per_fraction": final_selection.shadow_price_bps_per_fraction,
        "advancement_status": advancement["experiment_status"],
    }
    constraint_receipt = {
        "schema_version": "advisory_turnover_utility_constraint_v1",
        "coverage": coverage_receipt,
        "paths": path_constraint_receipts,
        "final": {
            "utility_scale_bps": final_scale.utility_scale_bps,
            "liability_scale": final_scale.liability_scale,
            "shadow_price_base_bps_per_fraction": final_scale.shadow_price_base_bps_per_fraction,
            **final_selection.__dict__,
        },
    }
    baseline = {
        "schema_version": "advisory_turnover_utility_baselines_v1",
        "selection_path_metrics": baseline_by_path,
        "selection_mean_primary_metric": float(
            np.mean([item[request.primary_metric] for item in baseline_by_path.values()])
        ),
    }
    references = {
        "schema_version": "advisory_turnover_utility_reference_comparison_v1",
        "p0d_v2_advancement": p0d_comparison,
        "p0f_v2_diagnostic": p0f_comparison,
        "p0f_is_advancement_gate": False,
        "p0d_bundle_id": request.exact_p0d_reference.bundle_id,
        "p0f_bundle_id": request.exact_p0f_reference.bundle_id,
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_turnover_constrained_utility_bundle(
        request=request,
        booster=final.booster,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        constraint_receipt=constraint_receipt,
        transform_receipt={
            "schema_version": "advisory_turnover_utility_transform_v1",
            "location_bps": final.transform.location_bps,
            "scale_bps": final.transform.scale_bps,
            "shadow_price_bps_per_fraction": final.shadow_price_bps_per_fraction,
            "fit_scope": "ALL_EXACT_P0_C_MATURED_ROWS_FINAL_REFIT",
        },
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
            "label_status_counts": coverage_receipt["label_status_counts"],
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


def _evaluate_constraint_blocks(
    *,
    rankings: pd.DataFrame,
    entry_priorities: pd.DataFrame,
    calibration_dates: Sequence[pd.Timestamp],
    block_by_date: dict[str, int],
    candidate_daily: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspend: pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
    policy: Any,
    policy_sha256: str,
    cost: AdvisoryPolicyCostV1,
    request_id: str,
) -> dict[str, Any]:
    dates = pd.DatetimeIndex(pd.to_datetime(list(calibration_dates))).normalize()
    priority_dates = set(pd.DatetimeIndex(pd.to_datetime(entry_priorities["decision_as_of_trade_date"])).normalize())
    if priority_dates != set(dates):
        raise AdvisoryModelFirstError(
            "P0-G/P0-D constraint calibration dates differ",
            reason_code="ADVISORY_TURNOVER_UTILITY_CALIBRATION_COVERAGE_INVALID",
        )
    grouped: dict[int, list[pd.Timestamp]] = {}
    for value in dates:
        block = block_by_date.get(value.date().isoformat())
        if block is None:
            raise AdvisoryModelFirstError(
                "constraint calibration date has no CPCV block",
                reason_code="ADVISORY_TURNOVER_UTILITY_BLOCK_LEAKAGE",
            )
        grouped.setdefault(int(block), []).append(value)
    daily_parts: list[pd.DataFrame] = []
    block_metrics: list[dict[str, Any]] = []
    for block, block_values in sorted(grouped.items()):
        block_dates = pd.DatetimeIndex(sorted(block_values)).normalize()
        block_priorities = entry_priorities[
            pd.to_datetime(entry_priorities["decision_as_of_trade_date"]).dt.normalize().isin(block_dates)
        ].copy()
        result = replay_shadow_portfolio(
            rankings=rankings,
            daily=candidate_daily,
            benchmark_daily=benchmark,
            suspend_rows=suspend,
            trading_calendar=calendar,
            policy=policy,
            policy_sha256=policy_sha256,
            cost_policy=cost,
            request_id=f"{request_id}_block_{block}",
            candidate_decision_dates=block_dates,
            entry_priorities=block_priorities,
        )
        daily_parts.append(result.daily)
        block_metrics.append(
            {
                "block_id": block,
                "calibration_decision_count": len(block_dates),
                "mean_turnover_fraction": float(result.daily["turnover_fraction"].mean()),
                "day_count": len(result.daily),
            }
        )
    if not daily_parts:
        raise AdvisoryModelFirstError(
            "constraint calibration produced no policy days",
            reason_code="ADVISORY_TURNOVER_UTILITY_CALIBRATION_COVERAGE_INVALID",
        )
    daily = pd.concat(daily_parts, ignore_index=True)
    return {
        "schema_version": "advisory_turnover_utility_train_constraint_evaluation_v1",
        "mean_turnover_fraction": float(daily["turnover_fraction"].mean()),
        "day_count": len(daily),
        "block_metrics": block_metrics,
    }


def _oracle_priorities(
    *,
    labels: pd.DataFrame,
    decision_dates: Sequence[pd.Timestamp],
    target_count: int,
    shadow_price: float,
) -> pd.DataFrame:
    dates = set(pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize())
    adjusted = add_turnover_constrained_targets(
        labels,
        target_count=target_count,
        shadow_price_bps_per_fraction=shadow_price,
    )
    adjusted["decision_as_of_trade_date"] = pd.to_datetime(adjusted["decision_as_of_trade_date"]).dt.normalize()
    rows = adjusted[adjusted["decision_as_of_trade_date"].isin(dates)].copy()
    if not (rows["label_status"] == "MATURED").all() or rows.groupby("decision_as_of_trade_date").size().ne(20).any():
        raise AdvisoryModelFirstError(
            "oracle constraint priorities are not exact-20 matured rows",
            reason_code="ADVISORY_TURNOVER_UTILITY_CALIBRATION_COVERAGE_INVALID",
        )
    rows = rows.rename(columns={"selection_rank": "selection_effective_rank"})
    rows[SCORE_COLUMN] = rows["turnover_constrained_policy_utility_bps"]
    return rank_turnover_utility_predictions(rows)


def _load_exact_reference(
    request: FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1,
    reference: ExactTurnoverUtilityReferenceV1,
    *,
    load_booster: bool,
) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    manifest_path = root / "manifest.json"
    if not manifest_path.is_file() or _sha256(manifest_path) != reference.manifest_file_sha256:
        raise AdvisoryModelFirstError(
            "turnover utility reference manifest differs from request",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
            context={"role": reference.role},
        )
    loaded = load_policy_utility_bundle(root, expected_bundle_id=reference.bundle_id, load_booster=load_booster)
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
            "turnover utility reference identity differs from request",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
            context={"role": reference.role, "mismatches": mismatches},
        )
    winner = _read_json(root / "winner_receipt.json")["winner_by_arm"].get(reference.arm_id)
    if not winner or (
        winner.get("family_id") != reference.winner_family_id
        or int(winner.get("seed", -1)) != reference.winner_seed
        or winner.get("training_objective") != reference.winner_training_objective
    ):
        raise AdvisoryModelFirstError(
            "turnover utility reference winner identity differs from request",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
            context={"role": reference.role},
        )
    return {
        "root": root,
        "loaded": loaded,
        "winner": winner,
        "trial_metrics": pd.read_parquet(root / "cpcv_trial_metrics.parquet"),
        "feature_schema": _read_json(root / "utility_feature_schema.json"),
    }


def _reference_winner_rows(
    reference: dict[str, Any],
    specification: ExactTurnoverUtilityReferenceV1,
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
            "turnover utility reference does not contain exact 28 winner paths",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
            context={"role": specification.role},
        )
    return selected


def _p0d_family(family_id: str):
    matches = [item for item in approved_policy_utility_families() if item.family_id == family_id]
    if len(matches) != 1:
        raise AdvisoryModelFirstError(
            "exact P0-D winner family is not approved",
            reason_code="ADVISORY_TURNOVER_UTILITY_REFERENCE_MISMATCH",
        )
    return matches[0]


def _verify_label_status_identity(
    request: FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1,
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
            "turnover utility label status identity differs from frozen P0-C",
            reason_code="ADVISORY_TURNOVER_UTILITY_LABEL_INVALID",
            context={"actual": actual, "expected": request.expected_label_status_counts},
        )


def _publish_incomplete(
    *,
    request: FrozenAdvisoryTurnoverConstrainedUtilityTrainingRequestV1,
    feature_result: Any,
    trial_metrics: pd.DataFrame,
    block_rows: list[dict[str, Any]],
    baseline_by_path: dict[str, dict[str, float]],
    constraint_receipt: dict[str, Any],
    p0d_reference: dict[str, Any],
    p0f_reference: dict[str, Any],
    environment: dict[str, Any],
    schema_receipt: dict[str, Any],
    progress: PolicyUtilityProgress,
) -> dict[str, Any]:
    advancement = {
        "schema_version": "advisory_turnover_utility_advancement_v1",
        "experiment_status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "advanced_to_stage_b": False,
        "stage_b_guard": "DENY_INCOMPLETE_CPCV",
        "failure": constraint_receipt.get("failure"),
    }
    block_scores = pd.DataFrame(block_rows)
    if not block_scores.empty:
        block_scores = block_scores.groupby(
            ["trial_id", "arm_id", "family_id", "seed", "block_id"], as_index=False
        )[request.primary_metric].mean()
    feature_schema = build_feature_schema_payload(
        market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
        suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
    )
    feature_schema.update({"feature_schema_hash": request.feature_schema_hash, "trained_feature_names": []})
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_turnover_constrained_utility_bundle(
        request=request,
        booster=None,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        constraint_receipt=constraint_receipt,
        transform_receipt={"schema_version": "advisory_turnover_utility_transform_v1", "status": "INCOMPLETE"},
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        pbo_receipt={"schema_version": "advisory_policy_pbo_v1", "status": "NOT_COMPUTABLE"},
        winner_receipt={"schema_version": "advisory_turnover_utility_winner_v1", "status": "INCOMPLETE"},
        baseline_comparison={
            "schema_version": "advisory_turnover_utility_baselines_v1",
            "selection_path_metrics": baseline_by_path,
        },
        reference_comparison={
            "schema_version": "advisory_turnover_utility_reference_comparison_v1",
            "p0d_bundle_id": p0d_reference["loaded"]["manifest"]["bundle_id"],
            "p0f_bundle_id": p0f_reference["loaded"]["manifest"]["bundle_id"],
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


def _failure(path_id: str, reason_code: str, message: str) -> dict[str, Any]:
    return {"path_id": path_id, "reason_code": reason_code, "message": message}
