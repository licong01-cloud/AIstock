from __future__ import annotations

import gc
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.dual_head_output_constraint_bundle import load_dual_head_bundle
from backend.services.advisory_model_first.dual_head_output_constraint_training import (
    LIABILITY_SCORE_COLUMN,
    add_liability_target,
    build_inner_fold_specs,
    eligible_constraint_dates,
    fit_final_liability_head,
    score_final_liability_head,
    train_liability_head_oof,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v2 import build_feature_schema_payload
from backend.services.advisory_model_first.grouped_rank_output_constraint_bundle import (
    load_grouped_rank_bundle,
)
from backend.services.advisory_model_first.meta_label_features import build_meta_label_feature_matrix
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
    find_selection_liability_gate_bundle_for_request,
    publish_selection_liability_gate_bundle,
)
from backend.services.advisory_model_first.selection_liability_gate_contracts import (
    ExactP0DSelectionLiabilityGateReferenceV1,
    FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
    SelectionLiabilityGateEvidenceReferenceV1,
)
from backend.services.advisory_model_first.selection_liability_gate_training import (
    assert_widest_gate_metrics_match_selection,
    assert_widest_gate_matches_selection,
    build_selection_preserving_gate_priorities,
    liability_gate_completeness_not_worse,
    selection_liability_gate_candidate_metrics,
    select_widest_feasible_liability_threshold,
)
from backend.services.advisory_model_first.selection_prior_residual_bundle import (
    load_selection_prior_residual_bundle,
)
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.advisory_model_first.turnover_constrained_utility_training import (
    train_fixed_p0d_reference_predictions,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


ARM_ID = "ARM_P0K_V1_SELECTION_PRESERVING_LIABILITY_GATE"


def run_selection_liability_gate_pipeline(request_path: str | Path) -> dict[str, Any]:
    try:
        request = FrozenAdvisorySelectionLiabilityGateTrainingRequestV1.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "selection-liability-gate frozen request cannot be read or validated",
            reason_code="ADVISORY_P0K_REQUEST_INVALID",
        ) from exc
    environment = verify_policy_environment(request)
    progress = PolicyUtilityProgress(request.resource_max_rss_bytes)
    started = time.monotonic()
    verify_policy_dataset(request)
    p0d_reference = _load_p0d_reference(request, request.exact_p0d_reference)
    evidence = {
        reference.role: _load_evidence_reference(request, reference)
        for reference in (
            request.p0h_evidence_reference,
            request.p0i_evidence_reference,
            request.p0j_evidence_reference,
        )
    }
    existing = find_selection_liability_gate_bundle_for_request(request)
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
    _verify_label_status_identity(request, labels)
    eligible_dates, base_coverage_receipt = eligible_constraint_dates(
        labels,
        expected_decision_date_count=request.expected_decision_date_count,
        expected_constraint_decision_date_count=request.expected_constraint_decision_date_count,
    )
    cpcv = read_policy_json(root / "cpcv_paths.json")
    policy = transition_policy_from_payload(read_policy_json(root / "shadow_policy.json"))
    cost = AdvisoryPolicyCostV1.model_validate_json((root / "cost_policy.json").read_text(encoding="utf-8"))
    policy_source_request = read_policy_json(root / "request.json")
    paths = [item for item in cpcv["paths"] if item["status"] == "READY"]
    _verify_cpcv_identity(request, paths, cpcv["block_by_date"])
    verify_policy_training_cutoffs(request, labels)
    progress.add(
        "source_readback",
        started,
        label_rows=len(labels),
        ranking_rows=len(rankings),
        path_count=len(paths),
    )

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
    verify_policy_bound_data_identities(request, calendar)
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
    verify_policy_feature_v2_coverage(request, feature_result.features, rankings)
    try:
        progress.add(
            "features",
            started,
            feature_rows=len(feature_result.features),
            available_dates=int((feature_result.coverage["status"] == "available").sum()),
        )
    except AdvisoryModelFirstError as exc:
        failure = {"path_id": "FEATURE_BUILD", **exc.as_dict()}
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=pd.DataFrame(),
            block_rows=[],
            baseline_by_path={},
            threshold_receipt={
                "schema_version": "advisory_selection_liability_gate_threshold_receipt_v1",
                "minimum_expected_holding_days": list(request.minimum_expected_holding_days),
                "maximum_liability_thresholds": list(request.maximum_liability_thresholds),
                "coverage": base_coverage_receipt,
                "p0d_path_budgets": [],
                "selection_oof_by_path": [],
                "trial_thresholds": [],
                "failure": failure,
            },
            coverage_receipt={
                "schema_version": "advisory_selection_liability_gate_coverage_receipt_v1",
                "base": base_coverage_receipt,
                "outer_trials": [],
                "failure": failure,
            },
            p0d_reference=p0d_reference,
            evidence=evidence,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )

    p0d_rows = _reference_winner_rows(p0d_reference, request.exact_p0d_reference, paths)
    p0d_family = _p0d_family(request.exact_p0d_reference.winner_family_id)
    trial_rows: list[dict[str, Any]] = []
    block_rows: list[dict[str, Any]] = []
    threshold_receipts: list[dict[str, Any]] = []
    p0d_budget_receipts: list[dict[str, Any]] = []
    selection_oof_receipts: list[dict[str, Any]] = []
    outer_coverage_receipts: list[dict[str, Any]] = []
    baseline_by_path: dict[str, dict[str, float]] = {}
    path_failure: dict[str, Any] | None = None

    for path in paths:
        path_started = time.monotonic()
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
            p0d_metrics = evaluate_liability_gate_constraint_blocks(
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
            selection_oof_priorities = _selection_priorities(rankings, calibration_dates)
            selection_oof_metrics = evaluate_liability_gate_constraint_blocks(
                rankings=rankings,
                entry_priorities=selection_oof_priorities,
                calibration_dates=calibration_dates,
                block_by_date=cpcv["block_by_date"],
                candidate_daily=candidate_daily,
                benchmark=benchmark,
                suspend=suspend,
                calendar=calendar,
                policy=policy,
                policy_sha256=request.shadow_policy_sha256,
                cost=cost,
                request_id=f"{request.request_id}_{path_id}_selection_oof",
            )
            selection_validation_priorities = _selection_priorities(rankings, validation_dates)
            selection_metrics, selection_daily, _ = evaluate_policy_validation_blocks(
                rankings,
                selection_validation_priorities,
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
        selection_oof_receipts.append(
            {
                "path_id": path_id,
                "mean_turnover_fraction": selection_oof_metrics["mean_turnover_fraction"],
                "active_slot_coverage": selection_oof_metrics["active_slot_coverage"],
                "cash_day_count": selection_oof_metrics["cash_day_count"],
                "day_count": selection_oof_metrics["day_count"],
                "block_metrics": selection_oof_metrics["block_metrics"],
            }
        )
        baseline_by_path[path_id] = paired_policy_metrics(selection_metrics)
        for family in request.family_specs:
            for seed in request.seed_roster:
                trial_id = f"{ARM_ID}_{family.family_id}_{seed}"
                try:
                    oof = train_liability_head_oof(
                        features=feature_result.features,
                        labels=labels,
                        folds=folds,
                        family=family,
                        seed=seed,
                        liability_clip_min=request.liability_clip_min,
                        liability_clip_max=request.liability_clip_max,
                    )
                    _verify_prediction_dates(oof.predictions, calibration_dates)
                    widest = build_selection_preserving_gate_priorities(
                        oof.predictions,
                        maximum_liability_threshold=request.maximum_liability_thresholds[0],
                    )
                    assert_widest_gate_matches_selection(
                        widest.priorities,
                        selection_oof_priorities,
                    )
                    widest_metrics = evaluate_liability_gate_constraint_blocks(
                        rankings=rankings,
                        entry_priorities=widest.priorities,
                        calibration_dates=calibration_dates,
                        block_by_date=cpcv["block_by_date"],
                        candidate_daily=candidate_daily,
                        benchmark=benchmark,
                        suspend=suspend,
                        calendar=calendar,
                        policy=policy,
                        policy_sha256=request.shadow_policy_sha256,
                        cost=cost,
                        request_id=(
                            f"{request.request_id}_{path_id}_{family.family_id}_{seed}_widest"
                        ),
                    )
                    assert_widest_gate_metrics_match_selection(
                        widest_metrics,
                        selection_oof_metrics,
                    )

                    def evaluate_threshold(priorities: pd.DataFrame) -> Mapping[str, Any]:
                        metrics = evaluate_liability_gate_constraint_blocks(
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
                            request_id=f"{request.request_id}_{path_id}_{family.family_id}_{seed}",
                        )
                        metrics["complete"] = liability_gate_completeness_not_worse(
                            metrics,
                            selection_oof_metrics,
                        )
                        metrics.pop("daily_completeness", None)
                        return metrics

                    selected = select_widest_feasible_liability_threshold(
                        predictions=oof.predictions,
                        thresholds=request.maximum_liability_thresholds,
                        p0d_oof_turnover_budget=float(p0d_metrics["mean_turnover_fraction"]),
                        evaluate=evaluate_threshold,
                        target_count=request.target_count,
                    )
                    liability_rounds = max(1, int(np.median(oof.best_iterations)))
                    model = fit_final_liability_head(
                        features=feature_result.features,
                        labels=labels,
                        train_dates=train_dates,
                        family=family,
                        seed=seed,
                        boost_rounds=liability_rounds,
                    )
                    validation_predictions = score_final_liability_head(
                        features=feature_result.features,
                        model=model,
                        score_dates=validation_dates,
                        liability_clip_min=request.liability_clip_min,
                        liability_clip_max=request.liability_clip_max,
                    )
                    gate = build_selection_preserving_gate_priorities(
                        validation_predictions,
                        maximum_liability_threshold=selected.maximum_liability_threshold,
                    )
                    if min(gate.eligible_count_by_date.values(), default=0) < request.target_count:
                        raise _outer_completeness_error(
                            "frozen liability threshold has insufficient outer candidate depth"
                        )
                    diagnostic_rows = _attach_labels(validation_predictions, labels)
                    candidate_metrics = selection_liability_gate_candidate_metrics(
                        diagnostic_rows,
                        maximum_liability_threshold=selected.maximum_liability_threshold,
                    )
                    policy_metrics, policy_daily, policy_episodes = evaluate_policy_validation_blocks(
                        rankings,
                        gate.priorities,
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
                    episode_metrics = policy_episode_metrics(
                        policy_daily,
                        policy_episodes,
                        target_count=policy.target_count,
                    )
                    outer_completeness = _assert_outer_daily_completeness_not_worse(
                        policy_daily,
                        selection_daily,
                        expected_dates=validation_dates,
                        target_count=policy.target_count,
                    )
                except AdvisoryModelFirstError as exc:
                    path_failure = {
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        **exc.as_dict(),
                    }
                    break
                threshold_receipts.append(
                    {
                        "trial_id": trial_id,
                        "path_id": path_id,
                        "family_id": family.family_id,
                        "seed": seed,
                        "calibration_decision_count": len(calibration_dates),
                        "calibration_dates_sha256": canonical_json_sha256(
                            [value.date().isoformat() for value in calibration_dates]
                        ),
                        "liability_boost_rounds": liability_rounds,
                        "inner_folds": list(oof.fold_receipts),
                        "widest_selection_equivalence": True,
                        **selected.__dict__,
                    }
                )
                outer_coverage_receipts.append(
                    {
                        "trial_id": trial_id,
                        "path_id": path_id,
                        "minimum_eligible_candidate_count": min(gate.eligible_count_by_date.values()),
                        "active_slot_coverage": outer_completeness["active_slot_coverage"],
                        "selection_active_slot_coverage": outer_completeness[
                            "selection_active_slot_coverage"
                        ],
                        "cash_day_count": outer_completeness["cash_day_count"],
                        "selection_cash_day_count": outer_completeness[
                            "selection_cash_day_count"
                        ],
                        "daily_completeness_not_worse": True,
                        "validation_decision_count": len(validation_dates),
                        "validation_dates_sha256": canonical_json_sha256(
                            [value.date().isoformat() for value in validation_dates]
                        ),
                    }
                )
                trial_rows.append(
                    {
                        "trial_id": trial_id,
                        "arm_id": ARM_ID,
                        "liability_training_objective": request.liability_training_objective,
                        "family_id": family.family_id,
                        "seed": seed,
                        "path_id": path_id,
                        "validation_blocks": list(validation_blocks),
                        **candidate_metrics,
                        **{
                            f"policy_{key}": value
                            for key, value in policy_metrics.items()
                            if key != "block_metrics"
                        },
                        **episode_metrics,
                        "selection_baseline_mean_daily_net_excess_return_bps": baseline_by_path[path_id][
                            request.primary_metric
                        ],
                        "policy_lift_bps": policy_metrics[request.primary_metric]
                        - baseline_by_path[path_id][request.primary_metric],
                        "maximum_liability_threshold": selected.maximum_liability_threshold,
                        "p0d_oof_turnover_budget": selected.p0d_oof_turnover_budget,
                        "p0k_oof_turnover": selected.p0k_oof_turnover,
                        "oof_constraint_slack": selected.constraint_slack,
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
                del model, oof, validation_predictions, diagnostic_rows, gate, policy_daily, policy_episodes
                gc.collect()
            if path_failure is not None:
                break
        if path_failure is not None:
            break
        try:
            progress.add("outer_path", path_started, path_id=path_id, completed_trials=6)
        except AdvisoryModelFirstError as exc:
            path_failure = {"path_id": path_id, **exc.as_dict()}
            break

    trial_metrics = pd.DataFrame(trial_rows)
    threshold_payload = {
        "schema_version": "advisory_selection_liability_gate_threshold_receipt_v1",
        "minimum_expected_holding_days": list(request.minimum_expected_holding_days),
        "maximum_liability_thresholds": list(request.maximum_liability_thresholds),
        "coverage": base_coverage_receipt,
        "p0d_path_budgets": p0d_budget_receipts,
        "selection_oof_by_path": selection_oof_receipts,
        "trial_thresholds": threshold_receipts,
        "failure": path_failure,
    }
    coverage_payload = {
        "schema_version": "advisory_selection_liability_gate_coverage_receipt_v1",
        "base": base_coverage_receipt,
        "outer_trials": outer_coverage_receipts,
        "failure": path_failure,
    }
    if path_failure is not None:
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            baseline_by_path=baseline_by_path,
            threshold_receipt=threshold_payload,
            coverage_receipt=coverage_payload,
            p0d_reference=p0d_reference,
            evidence=evidence,
            environment=environment,
            schema_receipt=schema_receipt.__dict__,
            progress=progress,
        )
    if len(trial_metrics) != request.expected_outer_trial_path_count:
        raise AdvisoryModelFirstError(
            "selection-liability-gate outer trial roster is incomplete",
            reason_code="ADVISORY_P0K_OUTER_ROSTER_INVALID",
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
        (trial_metrics["family_id"] == winner.family_id)
        & (trial_metrics["seed"] == winner.seed)
    ].copy()
    p0d_comparison = compare_policy_arm_rows(
        candidate_rows=winner_rows,
        reference_rows=p0d_rows,
        reference_role="ARM_P0D_V2_BINARY_PARITY",
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

    final_started = time.monotonic()
    try:
        all_dates = pd.DatetimeIndex(
            pd.to_datetime(labels["decision_as_of_trade_date"].unique())
        ).normalize()
        final_folds = build_inner_fold_specs(
            labels=labels,
            outer_train_dates=all_dates,
            eligible_dates=eligible_dates,
            block_by_date=cpcv["block_by_date"],
            trading_calendar=calendar,
            embargo_trading_days=request.inner_embargo_trading_days,
        )
        family = next(item for item in request.family_specs if item.family_id == winner.family_id)
        final_oof = train_liability_head_oof(
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
        final_p0d_metrics = evaluate_liability_gate_constraint_blocks(
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
        final_selection_priorities = _selection_priorities(rankings, eligible_dates)
        final_selection_metrics = evaluate_liability_gate_constraint_blocks(
            rankings=rankings,
            entry_priorities=final_selection_priorities,
            calibration_dates=eligible_dates,
            block_by_date=cpcv["block_by_date"],
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            policy_sha256=request.shadow_policy_sha256,
            cost=cost,
            request_id=f"{request.request_id}_final_selection_oof",
        )
        widest = build_selection_preserving_gate_priorities(
            final_oof.predictions,
            maximum_liability_threshold=request.maximum_liability_thresholds[0],
        )
        assert_widest_gate_matches_selection(widest.priorities, final_selection_priorities)
        widest_metrics = evaluate_liability_gate_constraint_blocks(
            rankings=rankings,
            entry_priorities=widest.priorities,
            calibration_dates=eligible_dates,
            block_by_date=cpcv["block_by_date"],
            candidate_daily=candidate_daily,
            benchmark=benchmark,
            suspend=suspend,
            calendar=calendar,
            policy=policy,
            policy_sha256=request.shadow_policy_sha256,
            cost=cost,
            request_id=f"{request.request_id}_final_widest",
        )
        assert_widest_gate_metrics_match_selection(widest_metrics, final_selection_metrics)

        def evaluate_final_threshold(priorities: pd.DataFrame) -> Mapping[str, Any]:
            metrics = evaluate_liability_gate_constraint_blocks(
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
                request_id=f"{request.request_id}_final_threshold",
            )
            metrics["complete"] = liability_gate_completeness_not_worse(
                metrics,
                final_selection_metrics,
            )
            metrics.pop("daily_completeness", None)
            return metrics

        final_threshold = select_widest_feasible_liability_threshold(
            predictions=final_oof.predictions,
            thresholds=request.maximum_liability_thresholds,
            p0d_oof_turnover_budget=float(final_p0d_metrics["mean_turnover_fraction"]),
            evaluate=evaluate_final_threshold,
            target_count=request.target_count,
        )
        final_rounds = max(1, int(np.median(final_oof.best_iterations)))
        final_model = fit_final_liability_head(
            features=feature_result.features,
            labels=labels,
            train_dates=all_dates,
            family=family,
            seed=int(winner.seed),
            boost_rounds=final_rounds,
        )
        progress.add(
            "trials_and_final",
            final_started,
            trial_path_count=len(trial_metrics),
            winner={"family_id": str(winner.family_id), "seed": int(winner.seed)},
        )
    except AdvisoryModelFirstError as exc:
        path_failure = {"path_id": "FINAL_REFIT", **exc.as_dict()}
        threshold_payload["failure"] = path_failure
        coverage_payload["failure"] = path_failure
        return _publish_incomplete(
            request=request,
            feature_result=feature_result,
            trial_metrics=trial_metrics,
            block_rows=block_rows,
            baseline_by_path=baseline_by_path,
            threshold_receipt=threshold_payload,
            coverage_receipt=coverage_payload,
            p0d_reference=p0d_reference,
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
            "prediction_columns": [LIABILITY_SCORE_COLUMN],
            "entry_priority_score_kind": "SELECTION_PRESERVING_LIABILITY_GATE_V1",
            "model_role": request.model_role,
        }
    )
    threshold_payload["final"] = {
        "family_id": str(winner.family_id),
        "seed": int(winner.seed),
        "liability_boost_rounds": final_rounds,
        **final_threshold.__dict__,
    }
    transform_receipt = {
        "schema_version": "advisory_selection_liability_gate_transform_v1",
        "liability_location": final_model.transform.location_bps,
        "liability_scale": final_model.transform.scale_bps,
        "liability_clip": [request.liability_clip_min, request.liability_clip_max],
    }
    winner_receipt = {
        "schema_version": "advisory_selection_liability_gate_winner_v1",
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
        "final_liability_boost_rounds": final_rounds,
        "final_maximum_liability_threshold": final_threshold.maximum_liability_threshold,
        "tie_break": request.tie_break,
        "training_objective": request.liability_training_objective,
        "model_role": request.model_role,
        "advancement_status": advancement["experiment_status"],
    }
    baseline_comparison = {
        "schema_version": "advisory_selection_liability_gate_baselines_v1",
        "selection_path_metrics": baseline_by_path,
        "winner_vs_p0d": p0d_comparison,
        "winner_minus_selection_mean_primary_metric_bps": selection_lift,
    }
    reference_comparison = _reference_comparison(p0d_reference, evidence)
    candidate_diagnostics = {
        "schema_version": "advisory_selection_liability_gate_candidate_diagnostics_v1",
        "winner_trial_summary": _diagnostic_summary(winner_rows),
        "candidate_return_metrics_are_diagnostic_only": True,
        "pbo_is_gate": False,
    }
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_selection_liability_gate_bundle(
        request=request,
        liability_booster=final_model.booster,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        threshold_receipt=threshold_payload,
        coverage_receipt=coverage_payload,
        transform_receipt=transform_receipt,
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        candidate_diagnostics=candidate_diagnostics,
        pbo_receipt=pbo,
        winner_receipt=winner_receipt,
        baseline_comparison=baseline_comparison,
        reference_comparison=reference_comparison,
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt.__dict__,
            "experiment_lineage": list(request.experiment_lineage),
            "independent_oos_evidence": False,
            "return_head_present": False,
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
        "winner": winner_receipt,
        "pbo": pbo,
        "trial_path_count": len(trial_metrics),
        "activated": False,
    }


def evaluate_liability_gate_constraint_blocks(
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
    priority_dates = set(
        pd.DatetimeIndex(pd.to_datetime(entry_priorities["decision_as_of_trade_date"])).normalize()
    )
    if priority_dates != set(dates):
        raise _coverage_error("liability-gate calibration dates differ from frozen dates")
    grouped: dict[int, list[pd.Timestamp]] = {}
    for value in dates:
        block = block_by_date.get(value.date().isoformat())
        if block is None:
            raise _coverage_error("liability-gate calibration date has no CPCV block")
        grouped.setdefault(int(block), []).append(value)
    daily_parts: list[pd.DataFrame] = []
    block_metrics: list[dict[str, Any]] = []
    for block, values in sorted(grouped.items()):
        block_dates = pd.DatetimeIndex(sorted(values)).normalize()
        block_priorities = entry_priorities[
            pd.to_datetime(entry_priorities["decision_as_of_trade_date"])
            .dt.normalize()
            .isin(block_dates)
        ].copy()
        block_rankings = rankings[
            pd.to_datetime(rankings["decision_as_of_trade_date"])
            .dt.normalize()
            .isin(block_dates)
        ].copy()
        result = replay_shadow_portfolio(
            rankings=block_rankings,
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
        block_daily = _matched_calibration_daily(
            result.daily,
            block_dates,
            target_count=policy.target_count,
        )
        daily_parts.append(block_daily)
        block_metrics.append(
            {
                "block_id": block,
                "calibration_decision_count": len(block_dates),
                "mean_turnover_fraction": float(block_daily["turnover_fraction"].mean()),
                "active_slot_coverage": float(
                    block_daily["active_count"].sum()
                    / (len(block_daily) * policy.target_count)
                ),
                "cash_day_count": int((block_daily["cash_slot_count"] > 0).sum()),
                "day_count": len(block_daily),
            }
        )
    if not daily_parts:
        raise _coverage_error("liability-gate calibration produced no policy days")
    daily = pd.concat(daily_parts, ignore_index=True)
    return {
        "schema_version": "advisory_selection_liability_gate_constraint_evaluation_v1",
        "mean_turnover_fraction": float(daily["turnover_fraction"].mean()),
        "active_slot_coverage": float(
            daily["active_count"].sum() / (len(daily) * policy.target_count)
        ),
        "cash_day_count": int((daily["cash_slot_count"] > 0).sum()),
        "day_count": len(daily),
        "daily_completeness": [
            {
                "decision_as_of_trade_date": pd.Timestamp(row.decision_as_of_trade_date)
                .date()
                .isoformat(),
                "active_count": int(row.active_count),
                "cash_slot_count": int(row.cash_slot_count),
                "turnover_fraction": float(row.turnover_fraction),
            }
            for row in daily.itertuples(index=False)
        ],
        "block_metrics": block_metrics,
    }


def _matched_calibration_daily(
    daily: pd.DataFrame,
    expected_dates: Sequence[pd.Timestamp],
    *,
    target_count: int,
) -> pd.DataFrame:
    required = {
        "decision_as_of_trade_date",
        "turnover_fraction",
        "active_count",
        "cash_slot_count",
    }
    if not required.issubset(daily):
        raise _coverage_error("liability-gate calibration output omits required daily fields")
    expected = pd.DatetimeIndex(pd.to_datetime(list(expected_dates))).normalize()
    rows = daily.loc[:, list(required)].copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(
        rows["decision_as_of_trade_date"]
    ).dt.normalize()
    rows = rows[rows["decision_as_of_trade_date"].isin(expected)].copy()
    if (
        rows.duplicated("decision_as_of_trade_date").any()
        or set(rows["decision_as_of_trade_date"]) != set(expected)
    ):
        raise _coverage_error(
            "liability-gate calibration output differs from exact matched decision dates"
        )
    for column in ("turnover_fraction", "active_count", "cash_slot_count"):
        rows[column] = pd.to_numeric(rows[column], errors="coerce")
        values = rows[column].to_numpy(float)
        if not np.isfinite(values).all():
            raise _coverage_error("liability-gate calibration output contains non-finite daily state")
    if (
        (rows["turnover_fraction"] < 0).any()
        or (rows["active_count"] < 0).any()
        or (rows["cash_slot_count"] < 0).any()
        or not np.array_equal(rows["active_count"], np.rint(rows["active_count"]))
        or not np.array_equal(rows["cash_slot_count"], np.rint(rows["cash_slot_count"]))
        or not (rows["active_count"] + rows["cash_slot_count"]).eq(target_count).all()
    ):
        raise _coverage_error("liability-gate calibration output contains invalid daily state")
    return rows.sort_values("decision_as_of_trade_date").reset_index(drop=True)


def _train_p0d_oof(
    *,
    features: pd.DataFrame,
    labels: pd.DataFrame,
    folds: Sequence[Any],
    family: Any,
    seed: int,
    boost_rounds: int,
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
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
            reason_code="ADVISORY_P0K_P0D_OOF_INVALID",
        )
    result = pd.concat(parts, ignore_index=True)
    if result.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        raise AdvisoryModelFirstError(
            "exact P0-D OOF predictions contain duplicates",
            reason_code="ADVISORY_P0K_P0D_OOF_INVALID",
        )
    return result


def _verify_prediction_dates(
    predictions: pd.DataFrame,
    expected_dates: Sequence[pd.Timestamp],
) -> None:
    actual = set(
        pd.DatetimeIndex(pd.to_datetime(predictions["decision_as_of_trade_date"])).normalize()
    )
    expected = set(pd.DatetimeIndex(pd.to_datetime(list(expected_dates))).normalize())
    counts = predictions.groupby(
        pd.to_datetime(predictions["decision_as_of_trade_date"]).dt.normalize()
    ).size()
    if actual != expected or counts.empty or not counts.eq(20).all():
        raise AdvisoryModelFirstError(
            "P0-K/P0-D OOF prediction dates differ from exact calibration dates",
            reason_code="ADVISORY_P0K_OOF_DATE_MISMATCH",
        )


def _selection_priorities(
    rankings: pd.DataFrame,
    dates: Sequence[pd.Timestamp],
) -> pd.DataFrame:
    normalized = set(pd.DatetimeIndex(pd.to_datetime(list(dates))).normalize())
    rows = rankings.loc[
        rankings["is_candidate_decision"]
        & pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize().isin(normalized)
        & (pd.to_numeric(rankings["selection_effective_rank"], errors="coerce") <= 20),
        ["decision_as_of_trade_date", "instrument", "selection_effective_rank"],
    ].copy()
    rows = rows.rename(columns={"selection_effective_rank": "entry_priority_rank"})
    counts = rows.groupby(pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()).size()
    if len(counts) != len(normalized) or counts.empty or not counts.eq(20).all():
        raise _coverage_error("matched Selection priorities are not exact Top20")
    return rows


def _attach_labels(predictions: pd.DataFrame, labels: pd.DataFrame) -> pd.DataFrame:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    columns = keys + ["label_status", "net_excess_return_bps", "holding_trading_days"]
    attached = predictions.merge(labels[columns], on=keys, how="left", validate="one_to_one")
    prepared = add_liability_target(attached)
    return prepared


def _load_p0d_reference(
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
    reference: ExactP0DSelectionLiabilityGateReferenceV1,
) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    manifest_path = root / "manifest.json"
    if (
        not manifest_path.is_file()
        or sha256_policy_file(manifest_path) != reference.manifest_file_sha256
    ):
        raise _reference_error("P0-D reference manifest differs from request")
    loaded = load_policy_utility_bundle(root, expected_bundle_id=reference.bundle_id, load_booster=False)
    winner = read_policy_json(root / "winner_receipt.json")["winner_by_arm"].get(reference.arm_id)
    expected = _shared_identity(request)
    mismatches = {
        key: {"expected": value, "actual": loaded["manifest"].get(key)}
        for key, value in expected.items()
        if loaded["manifest"].get(key) != value
    }
    if mismatches:
        raise _reference_error("P0-D reference identity differs from request", mismatches=mismatches)
    if not winner or (
        winner.get("family_id") != reference.winner_family_id
        or int(winner.get("seed", -1)) != reference.winner_seed
        or str(winner.get("training_objective")) != reference.winner_training_objective
        or int(winner.get("final_boost_rounds", 0)) != reference.winner_boost_rounds
    ):
        raise _reference_error("P0-D reference winner identity differs from request")
    return {
        "root": root,
        "loaded": loaded,
        "winner": winner,
        "trial_metrics": pd.read_parquet(root / "cpcv_trial_metrics.parquet"),
    }


def _load_evidence_reference(
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
    reference: SelectionLiabilityGateEvidenceReferenceV1,
) -> dict[str, Any]:
    root = Path(reference.bundle_root).resolve()
    manifest_path = root / "manifest.json"
    if (
        not manifest_path.is_file()
        or sha256_policy_file(manifest_path) != reference.manifest_file_sha256
    ):
        raise _reference_error("P0-H/P0-I/P0-J evidence manifest differs from request", role=reference.role)
    if reference.role == "P0H_V1_EVIDENCE":
        loaded = load_dual_head_bundle(root, expected_bundle_id=reference.bundle_id, load_boosters=False)
    elif reference.role == "P0I_V1_EVIDENCE":
        loaded = load_grouped_rank_bundle(root, expected_bundle_id=reference.bundle_id, load_boosters=False)
    else:
        loaded = load_selection_prior_residual_bundle(
            root,
            expected_bundle_id=reference.bundle_id,
            load_boosters=False,
        )
    manifest = loaded["manifest"]
    mismatches = {
        key: {"expected": value, "actual": manifest.get(key)}
        for key, value in _shared_identity(request).items()
        if manifest.get(key) != value
    }
    if mismatches:
        raise _reference_error("evidence identity differs from request", role=reference.role, mismatches=mismatches)
    if (
        manifest.get("experiment_status") != reference.expected_experiment_status
        or bool(manifest.get("model_available")) != reference.expected_model_available
    ):
        raise _reference_error("evidence terminal state differs from request", role=reference.role)
    return loaded


def _reference_winner_rows(
    reference: dict[str, Any],
    specification: ExactP0DSelectionLiabilityGateReferenceV1,
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
        raise _reference_error("P0-D reference does not contain exact 28 winner paths")
    return selected


def _p0d_family(family_id: str) -> Any:
    matches = [item for item in approved_policy_utility_families() if item.family_id == family_id]
    if len(matches) != 1:
        raise _reference_error("exact P0-D winner family is not approved")
    return matches[0]


def _verify_label_status_identity(
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
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
            "P0-K label status identity differs from frozen P0-C",
            reason_code="ADVISORY_P0K_LABEL_INVALID",
            context={"actual": actual, "expected": request.expected_label_status_counts},
        )


def _verify_cpcv_identity(
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
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
            "P0-K CPCV identity differs from frozen P0-C",
            reason_code="ADVISORY_P0K_OUTER_ROSTER_INVALID",
        )


def _publish_incomplete(
    *,
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
    feature_result: Any,
    trial_metrics: pd.DataFrame,
    block_rows: list[dict[str, Any]],
    baseline_by_path: dict[str, dict[str, float]],
    threshold_receipt: dict[str, Any],
    coverage_receipt: dict[str, Any],
    p0d_reference: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
    environment: dict[str, Any],
    schema_receipt: dict[str, Any],
    progress: PolicyUtilityProgress,
) -> dict[str, Any]:
    failure = threshold_receipt.get("failure") or coverage_receipt.get("failure")
    advancement = {
        "schema_version": "advisory_policy_utility_advancement_v1",
        "experiment_status": "NEGATIVE_STOP_INCOMPLETE_CPCV",
        "advanced_to_stage_b": False,
        "stage_b_guard": "DENY_INCOMPLETE_CPCV",
        "failure": failure,
        "pbo_is_gate": False,
        "candidate_diagnostics_are_gate": False,
        "historical_replay_is_gate": False,
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
    feature_schema.update(
        {
            "feature_schema_hash": request.feature_schema_hash,
            "trained_feature_names": [],
            "prediction_columns": [LIABILITY_SCORE_COLUMN],
            "model_role": request.model_role,
        }
    )
    resource = progress.report()
    bundle_id, bundle_path, manifest = publish_selection_liability_gate_bundle(
        request=request,
        liability_booster=None,
        feature_schema=feature_schema,
        runtime_hmm_models=feature_result.runtime_hmm_models,
        runtime_hmm_unavailable=list(feature_result.runtime_hmm_unavailable),
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
        threshold_receipt=threshold_receipt,
        coverage_receipt=coverage_receipt,
        transform_receipt={
            "schema_version": "advisory_selection_liability_gate_transform_v1",
            "status": "INCOMPLETE",
        },
        trial_metrics=trial_metrics,
        block_scores=block_scores,
        candidate_diagnostics={
            "schema_version": "advisory_selection_liability_gate_candidate_diagnostics_v1",
            "status": "INCOMPLETE",
        },
        pbo_receipt={"schema_version": "advisory_policy_pbo_v1", "status": "NOT_COMPUTABLE"},
        winner_receipt={
            "schema_version": "advisory_selection_liability_gate_winner_v1",
            "status": "INCOMPLETE",
        },
        baseline_comparison={
            "schema_version": "advisory_selection_liability_gate_baselines_v1",
            "selection_path_metrics": baseline_by_path,
        },
        reference_comparison=_reference_comparison(p0d_reference, evidence),
        advancement_receipt=advancement,
        training_log={
            "environment": environment,
            "schema_receipt": schema_receipt,
            "experiment_lineage": list(request.experiment_lineage),
            "independent_oos_evidence": False,
            "return_head_present": False,
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


def _shared_identity(
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
) -> dict[str, Any]:
    return {
        "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
        "program_id": request.program_id,
        "binding_version_id": request.binding_version_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "shadow_policy_sha256": request.shadow_policy_sha256,
        "cost_policy_sha256": request.cost_policy_sha256,
        "split_policy_sha256": request.split_policy_sha256,
        "feature_schema_hash": request.feature_schema_hash,
    }


def _reference_comparison(
    p0d_reference: dict[str, Any],
    evidence: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "advisory_selection_liability_gate_reference_comparison_v1",
        "p0d_bundle_id": p0d_reference["loaded"]["manifest"]["bundle_id"],
        "p0h_bundle_id": evidence["P0H_V1_EVIDENCE"]["manifest"]["bundle_id"],
        "p0i_bundle_id": evidence["P0I_V1_EVIDENCE"]["manifest"]["bundle_id"],
        "p0j_bundle_id": evidence["P0J_V1_EVIDENCE"]["manifest"]["bundle_id"],
    }


def _diagnostic_summary(rows: pd.DataFrame) -> dict[str, Any]:
    columns = [
        "liability_mae",
        "liability_rmse",
        "liability_daily_spearman_mean",
        "liability_daily_spearman_null_count",
        "accepted_candidate_count",
        "rejected_candidate_count",
        "accepted_candidate_mean_return_bps_diagnostic_only",
        "rejected_candidate_mean_return_bps_diagnostic_only",
        "liability_clip_low_count",
        "liability_clip_high_count",
    ]
    return {
        "row_count": len(rows),
        "means": {
            column: (
                float(pd.to_numeric(rows[column], errors="coerce").mean())
                if column in rows
                else None
            )
            for column in columns
        },
    }


def _optional_float(value: Any) -> float | None:
    numeric = float(value)
    return numeric if np.isfinite(numeric) else None


def _assert_outer_daily_completeness_not_worse(
    gate_daily: pd.DataFrame,
    selection_daily: pd.DataFrame,
    *,
    expected_dates: Sequence[pd.Timestamp],
    target_count: int,
) -> dict[str, float | int]:
    required = {
        "decision_as_of_trade_date",
        "active_count",
        "cash_slot_count",
        "is_candidate_decision",
    }
    if not required.issubset(gate_daily) or not required.issubset(selection_daily):
        raise _outer_completeness_error("outer completeness rows omit required daily fields")
    expected = pd.DatetimeIndex(pd.to_datetime(list(expected_dates))).normalize()
    if expected.empty or expected.duplicated().any() or target_count != 5:
        raise _outer_completeness_error("outer completeness expected-date identity is invalid")

    def indexed(frame: pd.DataFrame) -> pd.DataFrame:
        rows = frame.loc[frame["is_candidate_decision"].eq(True), sorted(required)].copy()
        rows["decision_as_of_trade_date"] = pd.to_datetime(
            rows["decision_as_of_trade_date"]
        ).dt.normalize()
        if rows.duplicated("decision_as_of_trade_date").any():
            raise _outer_completeness_error(
                "outer completeness rows duplicate a candidate decision date"
            )
        if set(rows["decision_as_of_trade_date"]) != set(expected):
            raise _outer_completeness_error(
                "outer completeness candidate dates differ from frozen validation dates"
            )
        for column in ("active_count", "cash_slot_count"):
            rows[column] = pd.to_numeric(rows[column], errors="coerce")
            values = rows[column].to_numpy(float)
            if not np.isfinite(values).all() or not np.array_equal(values, np.rint(values)):
                raise _outer_completeness_error(
                    "outer completeness candidate rows contain invalid slot state"
                )
        if (
            (rows["active_count"] < 0).any()
            or (rows["cash_slot_count"] < 0).any()
            or not (rows["active_count"] + rows["cash_slot_count"]).eq(target_count).all()
        ):
            raise _outer_completeness_error(
                "outer completeness candidate rows violate target-count identity"
            )
        return rows.set_index("decision_as_of_trade_date").sort_index()

    gate = indexed(gate_daily)
    selection = indexed(selection_daily)
    if not gate.index.equals(selection.index):
        raise _outer_completeness_error("outer completeness dates differ from matched Selection")
    if (
        (gate["active_count"] < selection["active_count"]).any()
        or (gate["cash_slot_count"] > selection["cash_slot_count"]).any()
    ):
        raise _outer_completeness_error(
            "frozen liability threshold worsens outer daily active-slot or cash completeness"
        )
    return {
        "active_slot_coverage": float(gate["active_count"].sum() / (len(gate) * target_count)),
        "selection_active_slot_coverage": float(
            selection["active_count"].sum() / (len(selection) * target_count)
        ),
        "cash_day_count": int((gate["cash_slot_count"] > 0).sum()),
        "selection_cash_day_count": int((selection["cash_slot_count"] > 0).sum()),
    }


def _reference_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_P0K_REFERENCE_MISMATCH",
        context=context or None,
    )


def _coverage_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_P0K_COVERAGE_INVALID",
        context=context or None,
    )


def _outer_completeness_error(message: str) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_P0K_OUTER_COMPLETENESS_FAILED",
    )
