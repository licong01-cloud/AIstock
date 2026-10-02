from __future__ import annotations

import gc
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.dual_head_output_constraint_bundle import (
    load_dual_head_bundle,
)
from backend.services.advisory_model_first.dual_head_output_constraint_training import (
    add_liability_target,
    build_inner_fold_specs,
    eligible_constraint_dates,
    fit_final_liability_head,
    score_final_liability_head,
    train_liability_head_oof,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import build_feature_schema_payload
from backend.services.advisory_model_first.meta_label_features import build_meta_label_feature_matrix
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_bundle import (
    find_p0g_anchored_liability_local_reranker_bundle_for_request,
    publish_p0g_anchored_liability_local_reranker_bundle,
)
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_contracts import (
    ExactP0DReferenceV1,
    ExactP0GAnchorReferenceV1,
    FrozenAdvisoryP0LTrainingRequestV1,
    P0LEvidenceReferenceV1,
)
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_training import (
    build_local_rerank_priorities,
    compare_policy_entries_and_completeness,
    local_rerank_candidate_metrics,
    select_minimum_feasible_gain,
)
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.policy_cpcv import calculate_policy_pbo
from backend.services.advisory_model_first.policy_utility_bundle import load_policy_utility_bundle
from backend.services.advisory_model_first.policy_utility_contracts import (
    approved_policy_utility_families,
)
from backend.services.advisory_model_first.policy_utility_pipeline import (
    PolicyUtilityProgress,
    build_policy_utility_advancement_receipt,
    compare_policy_arm_rows,
    evaluate_policy_validation_blocks,
    paired_policy_metrics,
    policy_episode_metrics,
    read_policy_json,
    sha256_policy_file,
    verify_policy_bound_data_identities,
    verify_policy_dataset,
    verify_policy_environment,
    verify_policy_feature_v2_coverage,
    verify_policy_source_coverage,
    verify_policy_training_cutoffs,
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
from backend.services.advisory_model_first.selection_liability_gate_bundle import (
    load_selection_liability_gate_bundle,
)
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.advisory_model_first.turnover_constrained_utility_bundle import (
    load_turnover_constrained_utility_bundle,
)
from backend.services.advisory_model_first.turnover_constrained_utility_contracts import (
    approved_turnover_constrained_utility_families,
)
from backend.services.advisory_model_first.turnover_constrained_utility_training import (
    SCORE_COLUMN,
    add_turnover_constrained_targets,
    complete_matured_decision_dates,
    fit_shadow_price_scale,
    rank_turnover_utility_predictions,
    score_final_turnover_constrained_utility,
    select_minimum_feasible_shadow_price,
    train_final_turnover_constrained_utility,
    train_fixed_p0d_reference_predictions,
    train_turnover_constrained_utility_trial,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


ARM_ID = "ARM_P0L_V1_P0G_ANCHORED_LIABILITY_LOCAL_RERANKER"


class P0LProgress(PolicyUtilityProgress):
    def add(self, stage: str, started: float, **details: Any) -> None:
        try:
            super().add(stage, started, **details)
        except AdvisoryModelFirstError as exc:
            if exc.reason_code != "ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED":
                raise
            raise _error(
                "P0-L Stage A exceeded the frozen 8 GiB RSS limit",
                "ADVISORY_P0L_RESOURCE_LIMIT_EXCEEDED",
                source=exc.as_dict(),
            ) from exc

    def report(self) -> dict[str, Any]:
        return {
            **super().report(),
            "schema_version": "advisory_p0l_resource_v1",
        }


def run_p0g_anchored_liability_local_reranker_pipeline(
    request_path: str | Path,
) -> dict[str, Any]:
    try:
        request = FrozenAdvisoryP0LTrainingRequestV1.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise _error("P0-L frozen request cannot be read", "ADVISORY_P0L_REQUEST_INVALID") from exc
    environment = verify_policy_environment(request)
    progress = P0LProgress(request.resource_max_rss_bytes)
    started = time.monotonic()
    verify_policy_dataset(request)
    p0d_reference = _load_p0d_reference(request, request.exact_p0d_reference)
    p0g_reference = _load_p0g_reference(request, request.exact_p0g_anchor_reference)
    evidence = {
        reference.role: _load_evidence(request, reference)
        for reference in (request.p0h_evidence_reference, request.p0k_evidence_reference)
    }
    existing = find_p0g_anchored_liability_local_reranker_bundle_for_request(request)
    if existing is not None:
        bundle_id, bundle_path, manifest = existing
        return {
            "status": "EXISTING_BUNDLE",
            "request_id": request.request_id,
            "bundle_id": bundle_id,
            "bundle_path": str(bundle_path),
            "manifest": manifest,
            "advancement": read_policy_json(bundle_path / "advancement_receipt.json"),
            "trial_path_count": len(pd.read_parquet(bundle_path / "cpcv_trial_metrics.parquet")),
            "activated": False,
        }
    root = Path(request.policy_dataset_bundle_root)
    rankings = pd.read_parquet(root / "candidate_rankings.parquet")
    labels = pd.read_parquet(root / "candidate_episode_labels.parquet")
    verify_policy_source_coverage(request, rankings, labels)
    _verify_label_identity(request, labels)
    eligible_dates, base_coverage = eligible_constraint_dates(
        labels,
        expected_decision_date_count=request.expected_decision_date_count,
        expected_constraint_decision_date_count=request.expected_constraint_decision_date_count,
    )
    anchor_price_dates, anchor_price_coverage = complete_matured_decision_dates(
        labels,
        expected_candidates_per_date=request.expected_candidates_per_date,
    )
    anchor_price_coverage = {
        **anchor_price_coverage,
        "role": "p0g_anchor_price_and_matched_p0d_turnover_only",
        "complete_matured_dates_sha256": _date_index_sha256(anchor_price_dates),
    }
    cpcv = read_policy_json(root / "cpcv_paths.json")
    paths = [item for item in cpcv["paths"] if item["status"] == "READY"]
    _verify_cpcv(request, paths, cpcv["block_by_date"])
    policy = transition_policy_from_payload(read_policy_json(root / "shadow_policy.json"))
    cost = AdvisoryPolicyCostV1.model_validate_json(
        (root / "cost_policy.json").read_text(encoding="utf-8")
    )
    source_request = read_policy_json(root / "request.json")
    verify_policy_training_cutoffs(request, labels)
    progress.add("source_readback", started, label_rows=len(labels), path_count=len(paths))

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    history_start = "2023-09-01"
    data_end = pd.Timestamp(source_request["data_cutoff"]).date().isoformat()
    calendar = load_trading_calendar(history_start, data_end)
    symbols = sorted(rankings.loc[rankings["is_candidate_decision"], "instrument"].unique())
    candidate_daily = load_qlib_daily(symbols, start=history_start, end=data_end)
    candidate_static = load_static_factors(
        request.factor_data_root,
        columns=STATIC_FACTOR_COLUMNS,
        start=history_start,
        end=data_end,
        instruments=symbols,
    )
    market_daily = load_qlib_daily(
        all_qlib_instruments(),
        start=history_start,
        end=data_end,
        fields=("$close", "$limit_up"),
    )
    benchmark = load_qlib_daily(
        [cost.benchmark_instrument], start=history_start, end=data_end, fields=("$open", "$close")
    )
    static_all = load_static_factors(
        request.factor_data_root,
        columns=("l2_code_id", "sw2_close", "sw2_amount"),
        start=history_start,
        end=data_end,
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start=history_start,
        end=data_end,
        instruments=symbols,
        full_day_only=True,
    )
    verify_policy_bound_data_identities(request, calendar)
    schema_receipt = validate_factor_file_schemas(
        request.factor_data_root, data_cutoff=request.factor_data_cutoff
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
        hmm_history_start="2023-12-01",
        runtime_cutoff=request.latest_training_decision_trade_date,
        feature_schema_version=request.feature_schema_version,
    )
    verify_policy_feature_v2_coverage(request, feature_result.features, rankings)
    progress.add("features", started, feature_rows=len(feature_result.features))

    p0d_rows = _reference_rows(p0d_reference, request.exact_p0d_reference, paths)
    p0g_rows = _reference_rows(p0g_reference, request.exact_p0g_anchor_reference, paths)
    p0d_family = _p0d_family(request.exact_p0d_reference.winner_family_id)
    p0g_family = _p0g_family(request.exact_p0g_anchor_reference.winner_family_id)
    trial_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    calibration_rows: list[dict[str, Any]] = []
    calibration_date_rows: list[dict[str, Any]] = []
    intervention_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    selection_by_path: dict[str, dict[str, float]] = {}
    anchor_by_path: dict[str, dict[str, float]] = {}
    failure: dict[str, Any] | None = None

    for path in paths:
        path_started = time.monotonic()
        path_id = str(path["path_id"])
        train_dates = pd.DatetimeIndex(pd.to_datetime(path["train_dates"])).normalize()
        validation_dates = pd.DatetimeIndex(pd.to_datetime(path["validation_dates"])).normalize()
        validation_blocks = tuple(int(value) for value in path["validation_blocks"])
        try:
            calibration_dates, anchor_price_calibration_dates, calibration_date_receipt = (
                _outer_calibration_date_contract(
                    train_dates=train_dates,
                    liability_eligible_dates=eligible_dates,
                    anchor_price_eligible_dates=anchor_price_dates,
                )
            )
            calibration_date_rows.append({"path_id": path_id, **calibration_date_receipt})
            folds = build_inner_fold_specs(
                labels=labels,
                outer_train_dates=train_dates,
                eligible_dates=eligible_dates,
                block_by_date=cpcv["block_by_date"],
                trading_calendar=calendar,
                embargo_trading_days=request.inner_embargo_trading_days,
            )
            anchor_oof, anchor_rounds, anchor_oof_receipts = _train_anchor_oof(
                features=feature_result.features,
                labels=labels,
                folds=folds,
                rankings=rankings,
                block_by_date=cpcv["block_by_date"],
                candidate_daily=candidate_daily,
                benchmark=benchmark,
                suspend=suspend,
                calendar=calendar,
                policy=policy,
                cost=cost,
                request=request,
                p0d_family=p0d_family,
                p0g_family=p0g_family,
                suffix=path_id,
            )
            _verify_prediction_dates(anchor_oof, calibration_dates)
            anchor_oof_eval = _evaluate_constraint_blocks(
                rankings=rankings,
                priorities=anchor_oof,
                dates=calibration_dates,
                block_by_date=cpcv["block_by_date"],
                candidate_daily=candidate_daily,
                benchmark=benchmark,
                suspend=suspend,
                calendar=calendar,
                policy=policy,
                cost=cost,
                request=request,
                suffix=f"{path_id}_anchor_oof",
            )
            p0d_oof = _train_p0d_oof(
                features=feature_result.features,
                labels=labels,
                folds=folds,
                family=p0d_family,
                seed=request.exact_p0d_reference.winner_seed,
                boost_rounds=request.exact_p0d_reference.winner_boost_rounds,
            )
            p0d_oof_eval = _evaluate_constraint_blocks(
                rankings=rankings,
                priorities=p0d_oof,
                dates=calibration_dates,
                block_by_date=cpcv["block_by_date"],
                candidate_daily=candidate_daily,
                benchmark=benchmark,
                suspend=suspend,
                calendar=calendar,
                policy=policy,
                cost=cost,
                request=request,
                suffix=f"{path_id}_p0d_oof",
            )
            anchor_price = _select_anchor_price(
                features=feature_result.features,
                labels=labels,
                train_dates=train_dates,
                calibration_dates=anchor_price_calibration_dates,
                rankings=rankings,
                block_by_date=cpcv["block_by_date"],
                candidate_daily=candidate_daily,
                benchmark=benchmark,
                suspend=suspend,
                calendar=calendar,
                policy=policy,
                cost=cost,
                request=request,
                p0d_family=p0d_family,
                suffix=path_id,
            )
            anchor_model = train_final_turnover_constrained_utility(
                features=feature_result.features,
                labels=_rows_on_dates(labels, train_dates),
                family=p0g_family,
                seed=request.exact_p0g_anchor_reference.winner_seed,
                boost_rounds=max(1, int(np.median(anchor_rounds))),
                target_count=request.target_count,
                shadow_price_bps_per_fraction=anchor_price["shadow_price"],
            )
            anchor_validation = score_final_turnover_constrained_utility(
                features=feature_result.features,
                model=anchor_model,
                score_dates=validation_dates,
            )
            anchor_metrics, anchor_daily, anchor_episodes = evaluate_policy_validation_blocks(
                rankings,
                anchor_validation,
                validation_blocks,
                cpcv,
                candidate_daily,
                benchmark,
                suspend,
                calendar,
                policy,
                request,
                cost,
                f"p0g_anchor_{path_id}",
            )
            selection_priorities = _selection_priorities(rankings, validation_dates)
            selection_metrics, _, _ = evaluate_policy_validation_blocks(
                rankings,
                selection_priorities,
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
        except AdvisoryModelFirstError as exc:
            failure = {"path_id": path_id, **exc.as_dict()}
            break
        selection_by_path[path_id] = paired_policy_metrics(selection_metrics)
        anchor_by_path[path_id] = paired_policy_metrics(anchor_metrics)

        for family in request.family_specs:
            for seed in request.seed_roster:
                trial_id = f"{ARM_ID}_{family.family_id}_{seed}"
                try:
                    liability_oof = train_liability_head_oof(
                        features=feature_result.features,
                        labels=labels,
                        folds=folds,
                        family=family,
                        seed=seed,
                        liability_clip_min=request.liability_clip_min,
                        liability_clip_max=request.liability_clip_max,
                    )
                    _verify_prediction_dates(liability_oof.predictions, calibration_dates)

                    def evaluate_gain(priorities: pd.DataFrame) -> Mapping[str, Any]:
                        candidate_eval = _evaluate_constraint_blocks(
                            rankings=rankings,
                            priorities=priorities,
                            dates=calibration_dates,
                            block_by_date=cpcv["block_by_date"],
                            candidate_daily=candidate_daily,
                            benchmark=benchmark,
                            suspend=suspend,
                            calendar=calendar,
                            policy=policy,
                            cost=cost,
                            request=request,
                            suffix=f"{path_id}_{family.family_id}_{seed}_gain",
                        )
                        return compare_policy_entries_and_completeness(
                            candidate_daily=candidate_eval["daily"],
                            candidate_episodes=candidate_eval["episodes"],
                            anchor_daily=anchor_oof_eval["daily"],
                            anchor_episodes=anchor_oof_eval["episodes"],
                            expected_dates=calibration_dates,
                            target_count=request.target_count,
                        )

                    selected = select_minimum_feasible_gain(
                        anchor_predictions=anchor_oof,
                        liability_predictions=liability_oof.predictions,
                        gain_roster=request.liability_rank_gain_roster,
                        p0d_oof_turnover_budget=float(p0d_oof_eval["mean_turnover_fraction"]),
                        anchor_metrics={
                            **anchor_oof_eval,
                            "actual_entry_change_count": 0,
                        },
                        evaluate=evaluate_gain,
                        target_count=request.target_count,
                    )
                    liability_rounds = max(1, int(np.median(liability_oof.best_iterations)))
                    liability_model = fit_final_liability_head(
                        features=feature_result.features,
                        labels=labels,
                        train_dates=train_dates,
                        family=family,
                        seed=seed,
                        boost_rounds=liability_rounds,
                    )
                    liability_validation = score_final_liability_head(
                        features=feature_result.features,
                        model=liability_model,
                        score_dates=validation_dates,
                        liability_clip_min=request.liability_clip_min,
                        liability_clip_max=request.liability_clip_max,
                    )
                    priorities = build_local_rerank_priorities(
                        anchor_validation,
                        liability_validation,
                        liability_rank_gain_required=selected.liability_rank_gain_required,
                        target_count=request.target_count,
                    )
                    policy_metrics, policy_daily, policy_episodes = evaluate_policy_validation_blocks(
                        rankings,
                        priorities.priorities,
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
                    intervention = compare_policy_entries_and_completeness(
                        candidate_daily=policy_daily,
                        candidate_episodes=policy_episodes,
                        anchor_daily=anchor_daily,
                        anchor_episodes=anchor_episodes,
                        expected_dates=validation_dates,
                        target_count=request.target_count,
                    )
                    diagnostics = local_rerank_candidate_metrics(
                        _attach_labels(liability_validation, labels),
                        changed_instruments={
                            (pd.Timestamp(item["decision_as_of_trade_date"]), str(item["upper_instrument"]))
                            for item in priorities.selected_swaps
                        }
                        | {
                            (pd.Timestamp(item["decision_as_of_trade_date"]), str(item["lower_instrument"]))
                            for item in priorities.selected_swaps
                        },
                    )
                except AdvisoryModelFirstError as exc:
                    failure = {
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        **exc.as_dict(),
                    }
                    break
                calibration_rows.append(
                    {
                        "trial_id": trial_id,
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        "anchor_inner_folds": anchor_oof_receipts,
                        "liability_inner_folds": list(liability_oof.fold_receipts),
                        "anchor_outer_shadow_price_bps_per_fraction": anchor_price["shadow_price"],
                        "anchor_outer_boost_rounds": anchor_model.boost_rounds,
                        "liability_boost_rounds": liability_rounds,
                        **selected.__dict__,
                    }
                )
                intervention_rows.append(
                    {
                        "trial_id": trial_id,
                        "path_id": path_id,
                        "priority_changed_decision_count": priorities.changed_decision_count,
                        "priority_changed_candidate_row_count": priorities.changed_candidate_row_count,
                        "top5_boundary_change_count": priorities.top5_boundary_change_count,
                        **intervention,
                    }
                )
                coverage_rows.append(
                    {
                        "trial_id": trial_id,
                        "path_id": path_id,
                        "daily_completeness_not_worse": bool(intervention["complete"]),
                        "active_slot_coverage": intervention["active_slot_coverage"],
                        "anchor_active_slot_coverage": intervention["anchor_active_slot_coverage"],
                        "cash_day_count": intervention["cash_day_count"],
                        "anchor_cash_day_count": intervention["anchor_cash_day_count"],
                    }
                )
                trial_rows.append(
                    {
                        "trial_id": trial_id,
                        "arm_id": ARM_ID,
                        "family_id": family.family_id,
                        "seed": seed,
                        "path_id": path_id,
                        "validation_blocks": list(validation_blocks),
                        **diagnostics,
                        **{f"policy_{key}": value for key, value in policy_metrics.items() if key != "block_metrics"},
                        **policy_episode_metrics(policy_daily, policy_episodes, target_count=policy.target_count),
                        "selection_baseline_mean_daily_net_excess_return_bps": selection_by_path[path_id][request.primary_metric],
                        "p0g_anchor_mean_daily_net_excess_return_bps": anchor_by_path[path_id][request.primary_metric],
                        "policy_lift_vs_selection_bps": policy_metrics[request.primary_metric] - selection_by_path[path_id][request.primary_metric],
                        "policy_lift_vs_p0g_anchor_bps": policy_metrics[request.primary_metric] - anchor_by_path[path_id][request.primary_metric],
                        "liability_rank_gain_required": selected.liability_rank_gain_required,
                        "p0d_oof_turnover_budget": selected.p0d_oof_turnover_budget,
                        "p0l_oof_turnover": selected.p0l_oof_turnover,
                        "oof_constraint_slack": selected.constraint_slack,
                        "outer_actual_entry_change_count": intervention["actual_entry_change_count"],
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
                del liability_oof, liability_model, liability_validation, policy_daily, policy_episodes
                gc.collect()
            if failure is not None:
                break
        if failure is not None:
            break
        try:
            progress.add("outer_path", path_started, path_id=path_id, completed_trials=6)
        except AdvisoryModelFirstError as exc:
            failure = {"path_id": path_id, **exc.as_dict()}
            break

    trial_metrics = pd.DataFrame(trial_rows)
    calibration_receipt = {
        "schema_version": "advisory_p0l_nested_calibration_v1",
        "base_coverage": base_coverage,
        "liability_constraint_coverage": base_coverage,
        "anchor_price_calibration_coverage": anchor_price_coverage,
        "outer_path_date_contracts": calibration_date_rows,
        "anchor_recomputed_once_per_outer_path": True,
        "outer_trials": calibration_rows,
        "failure": failure,
    }
    intervention_receipt = {
        "schema_version": "advisory_p0l_intervention_v1",
        "identity_control": request.identity_control,
        "gain_roster": list(request.liability_rank_gain_roster),
        "outer_trials": intervention_rows,
        "failure": failure,
    }
    coverage_receipt = {
        "schema_version": "advisory_p0l_coverage_v1",
        "base": base_coverage,
        "outer_trials": coverage_rows,
        "failure": failure,
    }
    if failure is not None:
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            calibration_receipt=calibration_receipt,
            intervention_receipt=intervention_receipt,
            coverage_receipt=coverage_receipt,
            p0d_reference=p0d_reference,
            p0g_reference=p0g_reference,
            evidence=evidence,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    _verify_complete_roster(request, trial_metrics)
    if int(trial_metrics["outer_actual_entry_change_count"].sum()) <= 0:
        failure = {
            "path_id": "FULL_OUTER_INTERVENTION",
            "reason_code": "ADVISORY_P0L_ANCHOR_IDENTITY_DEGENERATE",
            "message": "P0-L full outer result is identical to the P0-G anchor",
        }
        calibration_receipt["failure"] = failure
        intervention_receipt["failure"] = failure
        coverage_receipt["failure"] = failure
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            calibration_receipt=calibration_receipt,
            intervention_receipt=intervention_receipt,
            coverage_receipt=coverage_receipt,
            p0d_reference=p0d_reference,
            p0g_reference=p0g_reference,
            evidence=evidence,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )

    raw_blocks = pd.DataFrame(block_rows)
    block_scores = raw_blocks.groupby(
        ["trial_id", "arm_id", "family_id", "seed", "block_id"], as_index=False
    )[request.primary_metric].mean()
    pbo = _pbo_with_identity_diagnostic(
        block_scores,
        metric=request.primary_metric,
        group_count=request.expected_cpcv_block_count,
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
        (trial_metrics["family_id"] == winner.family_id)
        & (trial_metrics["seed"] == winner.seed)
    ].copy()
    if int(winner_rows["outer_actual_entry_change_count"].sum()) <= 0:
        failure = {
            "path_id": "WINNER_INTERVENTION",
            "reason_code": "ADVISORY_P0L_ANCHOR_IDENTITY_DEGENERATE",
            "message": "P0-L winner is identical to the P0-G anchor on all outer paths",
        }
        calibration_receipt["failure"] = failure
        intervention_receipt["failure"] = failure
        coverage_receipt["failure"] = failure
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            calibration_receipt=calibration_receipt,
            intervention_receipt=intervention_receipt,
            coverage_receipt=coverage_receipt,
            p0d_reference=p0d_reference,
            p0g_reference=p0g_reference,
            evidence=evidence,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    p0d_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0d_rows,
        reference_role="ARM_P0D_V2_BINARY_PARITY",
    )
    p0g_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0g_rows,
        reference_role="ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
    )
    selection_lift = float(
        winner_rows["policy_mean_daily_net_excess_return_bps"].mean()
        - np.mean([item[request.primary_metric] for item in selection_by_path.values()])
    )
    advancement = build_policy_utility_advancement_receipt(
        p0d_comparison=p0d_comparison,
        candidate_minus_selection_mean_primary_metric_bps=selection_lift,
        candidate_path_ids=winner_rows["path_id"].tolist(),
    )

    final_started = time.monotonic()
    try:
        final_model, final_gain, final_oof, final_anchor = _fit_final_winner(
            request=request,
            family=next(item for item in request.family_specs if item.family_id == winner.family_id),
            seed=int(winner.seed),
            features=feature_result.features,
            labels=labels,
            eligible_dates=eligible_dates,
            block_by_date=cpcv["block_by_date"],
            rankings=rankings,
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            cost=cost,
            p0d_family=p0d_family,
            p0g_family=p0g_family,
        )
        progress.add(
            "final_refit",
            final_started,
            winner={"family_id": str(winner.family_id), "seed": int(winner.seed)},
        )
    except AdvisoryModelFirstError as exc:
        failure = {"path_id": "FINAL_REFIT", **exc.as_dict()}
        calibration_receipt["failure"] = failure
        intervention_receipt["failure"] = failure
        coverage_receipt["failure"] = failure
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            calibration_receipt=calibration_receipt,
            intervention_receipt=intervention_receipt,
            coverage_receipt=coverage_receipt,
            p0d_reference=p0d_reference,
            p0g_reference=p0g_reference,
            evidence=evidence,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    feature_schema = build_feature_schema_payload(
        market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
        suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
    )
    feature_schema.update(
        {
            "feature_schema_hash": request.feature_schema_hash,
            "trained_feature_names": list(final_model.feature_names),
            "categorical_vocabulary": {
                key: list(value) for key, value in final_model.categorical_vocabulary.items()
            },
            "prediction_columns": ["predicted_turnover_liability_fraction_per_day"],
            "entry_priority_score_kind": "P0G_ANCHORED_RELATIVE_LIABILITY_LOCAL_RERANK_V1",
            "model_role": request.model_role,
        }
    )
    transform_receipt = {
        "schema_version": "advisory_p0l_liability_transform_v1",
        "liability_location": final_model.transform.location_bps,
        "liability_scale": final_model.transform.scale_bps,
        "liability_clip": [request.liability_clip_min, request.liability_clip_max],
    }
    winner_receipt = {
        "schema_version": "advisory_p0l_winner_v1",
        "arm_id": ARM_ID,
        "family_id": str(winner.family_id),
        "seed": int(winner.seed),
        "path_count": len(winner_rows),
        "primary_metric": request.primary_metric,
        "primary_metric_value": float(winner[request.primary_metric]),
        "mean_daily_net_return_bps": float(winner.mean_daily_net_return_bps),
        "mean_maximum_drawdown": float(winner.mean_maximum_drawdown),
        "mean_turnover_fraction": float(winner.mean_turnover_fraction),
        "completed_episode_hit_rate": _optional_float(winner.completed_episode_hit_rate),
        "final_liability_boost_rounds": final_model.boost_rounds,
        "final_liability_rank_gain_required": final_gain.liability_rank_gain_required,
        "tie_break": request.tie_break,
        "training_objective": request.liability_training_objective,
        "model_role": request.model_role,
        "advancement_status": advancement["experiment_status"],
    }
    calibration_receipt["final"] = {
        "family_id": str(winner.family_id),
        "seed": int(winner.seed),
        "liability_boost_rounds": final_model.boost_rounds,
        "anchor_oof_rows": len(final_anchor),
        "liability_oof_rows": len(final_oof),
        **final_gain.__dict__,
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_p0g_anchored_liability_local_reranker_bundle(
        request=request,
        liability_booster=final_model.booster,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        p0g_anchor_identity=_p0g_identity(request, p0g_reference),
        calibration_receipt=calibration_receipt,
        intervention_receipt=intervention_receipt,
        coverage_receipt=coverage_receipt,
        transform_receipt=transform_receipt,
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        candidate_diagnostics={
            "schema_version": "advisory_p0l_candidate_diagnostics_v1",
            "winner_trial_summary": _diagnostic_summary(winner_rows),
            "candidate_return_metrics_are_diagnostic_only": True,
            "pbo_is_gate": False,
        },
        pbo_receipt=pbo,
        winner_receipt=winner_receipt,
        baseline_comparison={
            "schema_version": "advisory_p0l_baselines_v1",
            "selection_path_metrics": selection_by_path,
            "p0g_anchor_path_metrics": anchor_by_path,
            "winner_vs_p0d": p0d_comparison,
            "winner_vs_p0g": p0g_comparison,
            "winner_minus_selection_mean_primary_metric_bps": selection_lift,
        },
        reference_comparison=_reference_comparison(p0d_reference, p0g_reference, evidence),
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt.__dict__,
            "experiment_lineage": list(request.experiment_lineage),
            "independent_oos_evidence": False,
            "return_head_present": False,
            "p0g_anchor_retrained_once_per_outer_path": True,
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


def _date_index_sha256(dates: Sequence[pd.Timestamp]) -> str:
    normalized = pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize().sort_values().unique()
    return canonical_json_sha256([value.date().isoformat() for value in normalized])


def _outer_calibration_date_contract(
    *,
    train_dates: Sequence[pd.Timestamp],
    liability_eligible_dates: Sequence[pd.Timestamp],
    anchor_price_eligible_dates: Sequence[pd.Timestamp],
) -> tuple[pd.DatetimeIndex, pd.DatetimeIndex, dict[str, Any]]:
    train = set(pd.DatetimeIndex(pd.to_datetime(list(train_dates))).normalize())
    liability = pd.DatetimeIndex(
        sorted(train & set(pd.DatetimeIndex(liability_eligible_dates).normalize()))
    )
    anchor_price = pd.DatetimeIndex(
        sorted(train & set(pd.DatetimeIndex(anchor_price_eligible_dates).normalize()))
    )
    if liability.empty or anchor_price.empty or not set(anchor_price).issubset(set(liability)):
        raise _error(
            "P0-L outer calibration date roles are invalid",
            "ADVISORY_P0L_CALIBRATION_DATE_CONTRACT_INVALID",
            liability_calibration_decision_count=len(liability),
            anchor_price_calibration_decision_count=len(anchor_price),
        )
    receipt = {
        "schema_version": "advisory_p0l_outer_calibration_date_contract_v1",
        "liability_calibration_decision_count": len(liability),
        "liability_calibration_dates_sha256": _date_index_sha256(liability),
        "anchor_price_calibration_decision_count": len(anchor_price),
        "anchor_price_calibration_dates_sha256": _date_index_sha256(anchor_price),
    }
    return liability, anchor_price, receipt


def _train_anchor_oof(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    folds: Sequence[Any],
    rankings: pd.DataFrame,
    block_by_date: dict[str, int],
    candidate_daily: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspend: pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
    policy: Any,
    cost: AdvisoryPolicyCostV1,
    request: FrozenAdvisoryP0LTrainingRequestV1,
    p0d_family: Any,
    p0g_family: Any,
    suffix: str,
) -> tuple[pd.DataFrame, tuple[int, ...], list[dict[str, Any]]]:
    parts: list[pd.DataFrame] = []
    rounds: list[int] = []
    receipts: list[dict[str, Any]] = []
    for fold in folds:
        if not fold.score_dates:
            continue
        train_dates, _ = complete_matured_decision_dates(
            _rows_on_dates(labels, fold.train_dates),
            expected_candidates_per_date=request.expected_candidates_per_date,
        )
        selection = _select_anchor_price(
            features=features,
            labels=labels,
            train_dates=train_dates,
            calibration_dates=train_dates,
            rankings=rankings,
            block_by_date=block_by_date,
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            cost=cost,
            request=request,
            p0d_family=p0d_family,
            suffix=f"{suffix}_inner_{fold.block_id}",
        )
        result = train_turnover_constrained_utility_trial(
            features=features,
            labels=labels,
            train_dates=fold.train_dates,
            validation_dates=fold.score_dates,
            family=p0g_family,
            seed=request.exact_p0g_anchor_reference.winner_seed,
            target_count=request.target_count,
            shadow_price_bps_per_fraction=selection["shadow_price"],
        )
        parts.append(result.validation_predictions)
        rounds.append(result.best_iteration)
        receipts.append(
            {
                "block_id": fold.block_id,
                "train_decision_count": len(fold.train_dates),
                "validation_decision_count": len(fold.validation_dates),
                "score_decision_count": len(fold.score_dates),
                "purged_decision_count": len(fold.purged_dates),
                "embargo_decision_count": len(fold.embargo_dates),
                "shadow_price_bps_per_fraction": selection["shadow_price"],
                "p0d_turnover_budget": selection["p0d_turnover_budget"],
                "best_iteration": result.best_iteration,
            }
        )
    if not parts:
        raise _error("P0-L fixed P0-G anchor OOF is empty", "ADVISORY_P0L_ANCHOR_OOF_INVALID")
    result = pd.concat(parts, ignore_index=True)
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _error("P0-L fixed P0-G anchor OOF duplicates rows", "ADVISORY_P0L_ANCHOR_OOF_INVALID")
    return result, tuple(rounds), receipts


def _select_anchor_price(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    train_dates: Sequence[pd.Timestamp],
    calibration_dates: Sequence[pd.Timestamp],
    rankings: pd.DataFrame,
    block_by_date: dict[str, int],
    candidate_daily: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspend: pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
    policy: Any,
    cost: AdvisoryPolicyCostV1,
    request: FrozenAdvisoryP0LTrainingRequestV1,
    p0d_family: Any,
    suffix: str,
) -> dict[str, float]:
    p0d = train_fixed_p0d_reference_predictions(
        features=features,
        labels=labels,
        train_dates=train_dates,
        score_dates=calibration_dates,
        family=p0d_family,
        seed=request.exact_p0d_reference.winner_seed,
        boost_rounds=request.exact_p0d_reference.winner_boost_rounds,
    )
    p0d_eval = _evaluate_constraint_blocks(
        rankings=rankings,
        priorities=p0d,
        dates=calibration_dates,
        block_by_date=block_by_date,
        candidate_daily=candidate_daily,
        benchmark=benchmark,
        suspend=suspend,
        calendar=calendar,
        policy=policy,
        cost=cost,
        request=request,
        suffix=f"{suffix}_p0d_budget",
    )
    scale = fit_shadow_price_scale(
        _rows_on_dates(labels, train_dates).query("label_status == 'MATURED'"),
        target_count=request.target_count,
    )

    def oracle_turnover(price: float) -> float:
        priorities = _oracle_priorities(
            labels=labels,
            dates=calibration_dates,
            target_count=request.target_count,
            shadow_price=price,
        )
        evaluation = _evaluate_constraint_blocks(
            rankings=rankings,
            priorities=priorities,
            dates=calibration_dates,
            block_by_date=block_by_date,
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            cost=cost,
            request=request,
            suffix=f"{suffix}_oracle_{format(price, '.12g')}",
        )
        return float(evaluation["mean_turnover_fraction"])

    selected = select_minimum_feasible_shadow_price(
        scale_fit=scale,
        p0d_train_turnover_budget=float(p0d_eval["mean_turnover_fraction"]),
        evaluate_oracle_turnover=oracle_turnover,
    )
    return {
        "shadow_price": selected.shadow_price_bps_per_fraction,
        "p0d_turnover_budget": selected.p0d_train_turnover_budget,
    }


def _evaluate_constraint_blocks(
    *,
    rankings: pd.DataFrame,
    priorities: pd.DataFrame,
    dates: Sequence[pd.Timestamp],
    block_by_date: dict[str, int],
    candidate_daily: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspend: pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
    policy: Any,
    cost: AdvisoryPolicyCostV1,
    request: FrozenAdvisoryP0LTrainingRequestV1,
    suffix: str,
) -> dict[str, Any]:
    expected = pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize()
    priority_dates = set(pd.to_datetime(priorities["decision_as_of_trade_date"]).dt.normalize())
    if priority_dates != set(expected):
        raise _error("P0-L constraint dates differ from priorities", "ADVISORY_P0L_OOF_DATE_MISMATCH")
    grouped: dict[int, list[pd.Timestamp]] = {}
    for value in expected:
        block = block_by_date.get(value.date().isoformat())
        if block is None:
            raise _error("P0-L constraint date has no CPCV block", "ADVISORY_P0L_BLOCK_LEAKAGE")
        grouped.setdefault(int(block), []).append(value)
    daily_parts: list[pd.DataFrame] = []
    episode_parts: list[pd.DataFrame] = []
    for block, values in sorted(grouped.items()):
        block_dates = pd.DatetimeIndex(sorted(values)).normalize()
        block_priorities = priorities[
            pd.to_datetime(priorities["decision_as_of_trade_date"]).dt.normalize().isin(block_dates)
        ].copy()
        result = replay_shadow_portfolio(
            rankings=rankings,
            daily=candidate_daily,
            benchmark_daily=benchmark,
            suspend_rows=suspend,
            trading_calendar=calendar,
            policy=policy,
            policy_sha256=request.shadow_policy_sha256,
            cost_policy=cost,
            request_id=f"{request.request_id}_{suffix}_block_{block}",
            candidate_decision_dates=block_dates,
            entry_priorities=block_priorities,
        )
        block_daily = result.daily[
            pd.to_datetime(result.daily["decision_as_of_trade_date"])
            .dt.normalize()
            .isin(block_dates)
        ].copy()
        if (
            block_daily.duplicated("decision_as_of_trade_date").any()
            or set(pd.to_datetime(block_daily["decision_as_of_trade_date"]).dt.normalize())
            != set(block_dates)
        ):
            raise _error(
                "P0-L constraint block daily coverage is incomplete",
                "ADVISORY_P0L_OOF_DATE_MISMATCH",
            )
        daily_parts.append(block_daily)
        episode_parts.append(result.episodes)
    daily = pd.concat(daily_parts, ignore_index=True)
    episodes = pd.concat(episode_parts, ignore_index=True) if episode_parts else pd.DataFrame()
    matched = daily.copy()
    if matched.duplicated("decision_as_of_trade_date").any() or set(
        pd.to_datetime(matched["decision_as_of_trade_date"]).dt.normalize()
    ) != set(expected):
        raise _error("P0-L constraint daily coverage is incomplete", "ADVISORY_P0L_OOF_DATE_MISMATCH")
    return {
        "mean_turnover_fraction": float(matched["turnover_fraction"].mean()),
        "active_slot_coverage": float(matched["active_count"].sum() / (len(matched) * policy.target_count)),
        "cash_day_count": int((matched["cash_slot_count"] > 0).sum()),
        "day_count": len(matched),
        "daily": daily,
        "episodes": episodes,
    }


def _fit_final_winner(
    *,
    request: FrozenAdvisoryP0LTrainingRequestV1,
    family: Any,
    seed: int,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    eligible_dates: Sequence[pd.Timestamp],
    block_by_date: dict[str, int],
    rankings: pd.DataFrame,
    candidate_daily: pd.DataFrame,
    benchmark: pd.DataFrame,
    suspend: pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
    policy: Any,
    cost: AdvisoryPolicyCostV1,
    p0d_family: Any,
    p0g_family: Any,
) -> tuple[Any, Any, pd.DataFrame, pd.DataFrame]:
    all_dates = pd.DatetimeIndex(pd.to_datetime(labels["decision_as_of_trade_date"].unique())).normalize()
    folds = build_inner_fold_specs(
        labels=labels,
        outer_train_dates=all_dates,
        eligible_dates=eligible_dates,
        block_by_date=block_by_date,
        trading_calendar=calendar,
        embargo_trading_days=request.inner_embargo_trading_days,
    )
    anchor_oof, _, _ = _train_anchor_oof(
        features=features,
        labels=labels,
        folds=folds,
        rankings=rankings,
        block_by_date=block_by_date,
        candidate_daily=candidate_daily,
        benchmark=benchmark,
        suspend=suspend,
        calendar=calendar,
        policy=policy,
        cost=cost,
        request=request,
        p0d_family=p0d_family,
        p0g_family=p0g_family,
        suffix="final",
    )
    liability_oof = train_liability_head_oof(
        features=features,
        labels=labels,
        folds=folds,
        family=family,
        seed=seed,
        liability_clip_min=request.liability_clip_min,
        liability_clip_max=request.liability_clip_max,
    )
    anchor_eval = _evaluate_constraint_blocks(
        rankings=rankings,
        priorities=anchor_oof,
        dates=eligible_dates,
        block_by_date=block_by_date,
        candidate_daily=candidate_daily,
        benchmark=benchmark,
        suspend=suspend,
        calendar=calendar,
        policy=policy,
        cost=cost,
        request=request,
        suffix="final_anchor",
    )
    p0d_oof = _train_p0d_oof(
        features=features,
        labels=labels,
        folds=folds,
        family=p0d_family,
        seed=request.exact_p0d_reference.winner_seed,
        boost_rounds=request.exact_p0d_reference.winner_boost_rounds,
    )
    p0d_eval = _evaluate_constraint_blocks(
        rankings=rankings,
        priorities=p0d_oof,
        dates=eligible_dates,
        block_by_date=block_by_date,
        candidate_daily=candidate_daily,
        benchmark=benchmark,
        suspend=suspend,
        calendar=calendar,
        policy=policy,
        cost=cost,
        request=request,
        suffix="final_p0d",
    )

    def evaluate(priorities: pd.DataFrame) -> Mapping[str, Any]:
        candidate = _evaluate_constraint_blocks(
            rankings=rankings,
            priorities=priorities,
            dates=eligible_dates,
            block_by_date=block_by_date,
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            cost=cost,
            request=request,
            suffix="final_gain",
        )
        return compare_policy_entries_and_completeness(
            candidate_daily=candidate["daily"],
            candidate_episodes=candidate["episodes"],
            anchor_daily=anchor_eval["daily"],
            anchor_episodes=anchor_eval["episodes"],
            expected_dates=eligible_dates,
            target_count=request.target_count,
        )

    gain = select_minimum_feasible_gain(
        anchor_predictions=anchor_oof,
        liability_predictions=liability_oof.predictions,
        gain_roster=request.liability_rank_gain_roster,
        p0d_oof_turnover_budget=float(p0d_eval["mean_turnover_fraction"]),
        anchor_metrics={**anchor_eval, "actual_entry_change_count": 0},
        evaluate=evaluate,
        target_count=request.target_count,
    )
    model = fit_final_liability_head(
        features=features,
        labels=labels,
        train_dates=all_dates,
        family=family,
        seed=seed,
        boost_rounds=max(1, int(np.median(liability_oof.best_iterations))),
    )
    return model, gain, liability_oof.predictions, anchor_oof


def _train_p0d_oof(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    folds: Sequence[Any],
    family: Any,
    seed: int,
    boost_rounds: int,
) -> pd.DataFrame:
    parts = [
        train_fixed_p0d_reference_predictions(
            features=features,
            labels=labels,
            train_dates=fold.train_dates,
            score_dates=fold.score_dates,
            family=family,
            seed=seed,
            boost_rounds=boost_rounds,
        )
        for fold in folds
        if fold.score_dates
    ]
    if not parts:
        raise _error("P0-L exact P0-D OOF is empty", "ADVISORY_P0L_P0D_OOF_INVALID")
    result = pd.concat(parts, ignore_index=True)
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise _error("P0-L exact P0-D OOF duplicates rows", "ADVISORY_P0L_P0D_OOF_INVALID")
    return result


def _oracle_priorities(
    *, labels: pd.DataFrame, dates: Sequence[pd.Timestamp], target_count: int, shadow_price: float
) -> pd.DataFrame:
    prepared = add_turnover_constrained_targets(
        labels, target_count=target_count, shadow_price_bps_per_fraction=shadow_price
    )
    rows = _rows_on_dates(prepared, dates)
    if not (rows["label_status"] == "MATURED").all() or rows.groupby(
        "decision_as_of_trade_date"
    ).size().ne(20).any():
        raise _error("P0-L anchor oracle is not exact matured Top20", "ADVISORY_P0L_ANCHOR_OOF_INVALID")
    rows = rows.rename(columns={"selection_rank": "selection_effective_rank"})
    rows[SCORE_COLUMN] = rows["turnover_constrained_policy_utility_bps"]
    return rank_turnover_utility_predictions(rows)


def _selection_priorities(rankings: pd.DataFrame, dates: Sequence[pd.Timestamp]) -> pd.DataFrame:
    expected = set(pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize())
    rows = rankings[
        rankings["is_candidate_decision"]
        & pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize().isin(expected)
        & (rankings["selection_effective_rank"] <= 20)
    ][["decision_as_of_trade_date", "instrument", "selection_effective_rank"]].copy()
    return rows.rename(columns={"selection_effective_rank": "entry_priority_rank"})


def _rows_on_dates(frame: pd.DataFrame, dates: Sequence[pd.Timestamp]) -> pd.DataFrame:
    expected = set(pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize())
    rows = frame.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    return rows[rows["decision_as_of_trade_date"].isin(expected)].copy()


def _attach_labels(predictions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    columns = keys + ["label_status", "net_excess_return_bps", "holding_trading_days"]
    return add_liability_target(
        predictions.merge(labels[columns], on=keys, how="left", validate="one_to_one")
    )


def _pbo_with_identity_diagnostic(
    block_scores: pd.DataFrame, *, metric: str, group_count: int
) -> dict[str, Any]:
    pivot = block_scores.pivot(index="trial_id", columns="block_id", values=metric).sort_index()
    if pivot.isna().any().any() or not np.isfinite(pivot.to_numpy(float)).all():
        raise _error("P0-L PBO block matrix is incomplete", "ADVISORY_P0L_INCOMPLETE_CPCV")
    vectors = [tuple(float(value) for value in row) for row in pivot.to_numpy()]
    unique_count = len(set(vectors))
    identity_groups: dict[str, list[str]] = {}
    for trial_id, vector in zip(pivot.index, vectors, strict=True):
        digest = canonical_json_sha256(list(vector))
        identity_groups.setdefault(digest, []).append(str(trial_id))
    diagnostic = {
        "trial_count": len(vectors),
        "unique_block_score_vector_count": unique_count,
        "identity_groups": [values for values in identity_groups.values() if len(values) > 1],
        "pbo_is_gate": False,
    }
    if unique_count < 2:
        return {
            "schema_version": "advisory_p0l_pbo_v1",
            "status": "DEGENERATE_NOT_INTERPRETABLE",
            "pbo": None,
            **diagnostic,
        }
    return {**calculate_policy_pbo(block_scores, group_count=group_count, metric_column=metric), **diagnostic}


def _load_p0d_reference(request: Any, reference: ExactP0DReferenceV1) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    _verify_manifest_hash(root, reference.manifest_file_sha256)
    loaded = load_policy_utility_bundle(root, expected_bundle_id=reference.bundle_id, load_booster=False)
    winner = read_policy_json(root / "winner_receipt.json")["winner_by_arm"].get(reference.arm_id)
    _verify_shared_manifest(request, loaded["manifest"])
    if not winner or (
        winner.get("family_id") != reference.winner_family_id
        or int(winner.get("seed", -1)) != reference.winner_seed
        or winner.get("training_objective") != reference.winner_training_objective
        or int(winner.get("final_boost_rounds", 0)) != reference.winner_boost_rounds
    ):
        raise _error("P0-L P0-D winner identity differs", "ADVISORY_P0L_REFERENCE_MISMATCH")
    return {"root": root, "loaded": loaded, "winner": winner, "trial_metrics": pd.read_parquet(root / "cpcv_trial_metrics.parquet")}


def _load_p0g_reference(request: Any, reference: ExactP0GAnchorReferenceV1) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    _verify_manifest_hash(root, reference.manifest_file_sha256)
    loaded = load_turnover_constrained_utility_bundle(
        root, expected_bundle_id=reference.bundle_id, load_booster=False
    )
    winner = read_policy_json(root / "winner_receipt.json")
    frozen = read_policy_json(root / "training_request.json")
    _verify_shared_manifest(request, loaded["manifest"])
    if (
        winner.get("arm_id") != reference.arm_id
        or winner.get("family_id") != reference.winner_family_id
        or int(winner.get("seed", -1)) != reference.winner_seed
        or frozen.get("training_objective") != reference.winner_training_objective
        or int(winner.get("final_boost_rounds", 0)) != reference.winner_boost_rounds
    ):
        raise _error("P0-L P0-G anchor identity differs", "ADVISORY_P0L_REFERENCE_MISMATCH")
    return {"root": root, "loaded": loaded, "winner": winner, "trial_metrics": pd.read_parquet(root / "cpcv_trial_metrics.parquet")}


def _load_evidence(request: Any, reference: P0LEvidenceReferenceV1) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    _verify_manifest_hash(root, reference.manifest_file_sha256)
    loaded = (
        load_dual_head_bundle(root, expected_bundle_id=reference.bundle_id, load_boosters=False)
        if reference.role == "P0H_V1_EVIDENCE"
        else load_selection_liability_gate_bundle(root, expected_bundle_id=reference.bundle_id, load_booster=False)
    )
    _verify_shared_manifest(request, loaded["manifest"])
    if (
        loaded["manifest"].get("experiment_status") != reference.expected_experiment_status
        or loaded["manifest"].get("model_available") is not True
    ):
        raise _error("P0-L evidence terminal state differs", "ADVISORY_P0L_REFERENCE_MISMATCH")
    return loaded


def _verify_manifest_hash(root: Path, expected: str) -> None:
    path = root / "manifest.json"
    if not path.is_file() or sha256_policy_file(path) != expected:
        raise _error("P0-L reference manifest differs", "ADVISORY_P0L_REFERENCE_MISMATCH")


def _verify_shared_manifest(request: Any, manifest: Mapping[str, Any]) -> None:
    keys = (
        "policy_dataset_bundle_id", "program_id", "binding_version_id", "package_id",
        "manifest_sha256", "shadow_policy_sha256", "cost_policy_sha256",
        "split_policy_sha256", "feature_schema_hash",
    )
    mismatches = {key: (getattr(request, key), manifest.get(key)) for key in keys if manifest.get(key) != getattr(request, key)}
    if mismatches:
        raise _error("P0-L reference shared identity differs", "ADVISORY_P0L_REFERENCE_MISMATCH", mismatches=mismatches)


def _reference_rows(reference: dict[str, Any], specification: Any, paths: list[dict[str, Any]]) -> pd.DataFrame:
    rows = reference["trial_metrics"]
    selected = rows[
        (rows["arm_id"] == specification.arm_id)
        & (rows["family_id"] == specification.winner_family_id)
        & (rows["seed"].astype(int) == specification.winner_seed)
    ].copy()
    expected = {str(item["path_id"]) for item in paths}
    if len(selected) != len(expected) or set(selected["path_id"]) != expected:
        raise _error("P0-L reference lacks exact 28 paths", "ADVISORY_P0L_REFERENCE_MISMATCH")
    return selected


def _p0d_family(family_id: str) -> Any:
    matches = [item for item in approved_policy_utility_families() if item.family_id == family_id]
    if len(matches) != 1:
        raise _error("P0-L P0-D family is not approved", "ADVISORY_P0L_REFERENCE_MISMATCH")
    return matches[0]


def _p0g_family(family_id: str) -> Any:
    matches = [item for item in approved_turnover_constrained_utility_families() if item.family_id == family_id]
    if len(matches) != 1:
        raise _error("P0-L P0-G family is not approved", "ADVISORY_P0L_REFERENCE_MISMATCH")
    return matches[0]


def _verify_label_identity(request: Any, labels: pd.DataFrame) -> None:
    actual = {str(key): int(value) for key, value in labels["label_status"].value_counts().items()}
    if len(labels) != request.expected_candidate_row_count or actual != request.expected_label_status_counts:
        raise _error("P0-L label identity differs from P0-C", "ADVISORY_P0L_LABEL_INVALID", actual=actual)


def _verify_cpcv(request: Any, paths: list[dict[str, Any]], block_by_date: dict[str, int]) -> None:
    if (
        len(paths) != request.expected_outer_path_count
        or len({item["path_id"] for item in paths}) != len(paths)
        or len(set(block_by_date.values())) != request.expected_cpcv_block_count
        or any(len(item["validation_blocks"]) != request.expected_outer_validation_block_count for item in paths)
    ):
        raise _error("P0-L CPCV identity is invalid", "ADVISORY_P0L_INCOMPLETE_CPCV")


def _verify_prediction_dates(predictions: pd.DataFrame, dates: Sequence[pd.Timestamp]) -> None:
    expected = set(pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize())
    actual = set(pd.to_datetime(predictions["decision_as_of_trade_date"]).dt.normalize())
    counts = predictions.groupby(pd.to_datetime(predictions["decision_as_of_trade_date"]).dt.normalize()).size()
    if actual != expected or counts.empty or not counts.eq(20).all():
        raise _error("P0-L predictions differ from exact dates", "ADVISORY_P0L_OOF_DATE_MISMATCH")


def _verify_complete_roster(request: Any, trials: pd.DataFrame) -> None:
    counts = trials.groupby(["family_id", "seed"])["path_id"].nunique()
    if (
        len(trials) != request.expected_outer_trial_path_count
        or len(counts) != len(request.family_specs) * len(request.seed_roster)
        or not counts.eq(request.expected_outer_path_count).all()
    ):
        raise _error("P0-L 168-trial roster is incomplete", "ADVISORY_P0L_INCOMPLETE_CPCV")


def _p0g_identity(request: Any, reference: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "advisory_p0l_fixed_p0g_anchor_identity_v1",
        "bundle_id": request.exact_p0g_anchor_reference.bundle_id,
        "manifest_file_sha256": request.exact_p0g_anchor_reference.manifest_file_sha256,
        "arm_id": request.exact_p0g_anchor_reference.arm_id,
        "family_id": request.exact_p0g_anchor_reference.winner_family_id,
        "seed": request.exact_p0g_anchor_reference.winner_seed,
        "training_objective": request.exact_p0g_anchor_reference.winner_training_objective,
        "winner_boost_rounds": request.exact_p0g_anchor_reference.winner_boost_rounds,
        "winner": reference["winner"],
    }


def _reference_comparison(p0d: dict[str, Any], p0g: dict[str, Any], evidence: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "advisory_p0l_references_v1",
        "p0d_bundle_id": p0d["loaded"]["manifest"]["bundle_id"],
        "p0g_bundle_id": p0g["loaded"]["manifest"]["bundle_id"],
        "p0h_bundle_id": evidence["P0H_V1_EVIDENCE"]["manifest"]["bundle_id"],
        "p0k_bundle_id": evidence["P0K_V1_EVIDENCE"]["manifest"]["bundle_id"],
    }


def _diagnostic_summary(rows: pd.DataFrame) -> dict[str, Any]:
    columns = [
        "changed_candidate_mean_return_bps_diagnostic_only",
        "unchanged_candidate_mean_return_bps_diagnostic_only",
        "liability_prediction_min",
        "liability_prediction_max",
        "outer_actual_entry_change_count",
    ]
    return {column: _optional_float(rows[column].mean()) for column in columns}


def _publish_incomplete(
    *,
    request: FrozenAdvisoryP0LTrainingRequestV1,
    feature_result: Any,
    trial_metrics: pd.DataFrame,
    block_rows: list[dict[str, Any]],
    calibration_receipt: Mapping[str, Any],
    intervention_receipt: Mapping[str, Any],
    coverage_receipt: Mapping[str, Any],
    p0d_reference: dict[str, Any],
    p0g_reference: dict[str, Any],
    evidence: dict[str, Any],
    environment: Mapping[str, Any],
    schema_receipt: Mapping[str, Any],
    progress: PolicyUtilityProgress,
) -> dict[str, Any]:
    raw = pd.DataFrame(block_rows)
    block_scores = raw if raw.empty else raw.groupby(
        ["trial_id", "arm_id", "family_id", "seed", "block_id"], as_index=False
    )[request.primary_metric].mean()
    advancement = {
        "schema_version": "advisory_policy_utility_advancement_v1",
        "experiment_status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "advanced_to_stage_b": False,
        "checks": {"exact_28_unique_paths": False},
        "pbo_is_gate": False,
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_p0g_anchored_liability_local_reranker_bundle(
        request=request,
        liability_booster=None,
        feature_schema={
            **build_feature_schema_payload(
                market_calendar_identity=request.market_calendar_identity.model_dump(mode="json"),
                suspend_sidecar_identity=request.suspend_sidecar_identity.model_dump(mode="json"),
            ),
            "feature_schema_hash": request.feature_schema_hash,
            "status": "NO_WINNER_INCOMPLETE_CPCV",
        },
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        p0g_anchor_identity=_p0g_identity(request, p0g_reference),
        calibration_receipt=calibration_receipt,
        intervention_receipt=intervention_receipt,
        coverage_receipt=coverage_receipt,
        transform_receipt={"status": "NO_FINAL_REFIT_INCOMPLETE_CPCV"},
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        candidate_diagnostics={"status": "NOT_COMPUTABLE_INCOMPLETE_CPCV"},
        pbo_receipt={"status": "NOT_COMPUTABLE_INCOMPLETE_CPCV"},
        winner_receipt={"status": "NO_WINNER_INCOMPLETE_CPCV"},
        baseline_comparison={"status": "INCOMPLETE"},
        reference_comparison=_reference_comparison(p0d_reference, p0g_reference, evidence),
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt,
            "failure": calibration_receipt["failure"],
            "experiment_lineage": list(request.experiment_lineage),
        },
        resource_report=resource,
    )
    return {
        "status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "request_id": request.request_id,
        "bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "advancement": advancement,
        "trial_path_count": len(trial_metrics),
        "resource_report": resource,
        "activated": False,
    }


def _optional_float(value: Any) -> float | None:
    numeric = float(value)
    return numeric if np.isfinite(numeric) else None


def _error(message: str, reason_code: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(message, reason_code=reason_code, context=context or None)
