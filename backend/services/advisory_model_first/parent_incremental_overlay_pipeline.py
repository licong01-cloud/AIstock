from __future__ import annotations

import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _git_command_for_worktree,
)
from backend.services.advisory_model_first.parent_incremental_overlay_contracts import (
    PARENT_OVERLAY_CANDIDATES,
    PARENT_OVERLAY_EXPERIMENT_ID,
    PARENT_OVERLAY_HYPOTHESIS_FAMILY_ID,
    FrozenParentIncrementalOverlayRequestV1,
    ParentIncrementalOverlayReceiptV1,
    ParentIncrementalOverlayTrialV1,
    build_default_overlay_trials,
    build_parent_overlay_receipt,
    build_parent_overlay_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _cross_os_git_commit,
    _cross_os_git_dirty_paths,
    _deflated_sharpe_diagnostic,
    _descriptors_for,
    _file_descriptors,
    _finite_array,
    _mean_or_none,
    _median_or_none,
    _moving_block_interval,
    _parquet_row_count,
    _peak_rss_bytes,
    _positive_fraction,
    _read_bundle as _read_parent_bundle,
    _read_json,
    _safe_correlation,
    _std_or_none,
    _verify_ref,
    _write_json,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    ConsumedWindowV1,
    DecisionUse,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


PARENT_OVERLAY_BUNDLE_SCHEMA = "advisory_parent_incremental_overlay_bundle_v1"
BASE_SCORE_COLUMNS = (
    "decision_as_of_trade_date",
    "instrument",
    "score",
    "economic_net_excess_bps",
    "outcome_known",
)
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "overlay_roster.json",
        "overlay_score_panel.parquet",
        "daily_metrics.parquet",
        "overlay_summary.json",
        "frontier_receipt.json",
    }
)
BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "source_identity_receipt.json",
    "overlay_receipt.json",
    "resource_report.json",
    "registry_record.json",
}


def prepare_parent_overlay_request(
    *,
    parent_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenParentIncrementalOverlayRequestV1:
    """Freeze one development-only 6x4 parent-overlay request."""

    parent = Path(parent_bundle_path).resolve()
    repo = Path(repository_root).resolve()
    loaded = _read_parent_bundle(parent)
    _validate_parent_navigation_source(parent, loaded)
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "parent overlay request requires a clean repository",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repo)
    origin_main_commit = _git_origin_main_commit(repo)
    if commit != origin_main_commit:
        _raise(
            "parent overlay formal request requires HEAD to equal origin/main",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main_commit,
        )
    parent_request = loaded["request"]
    parent_record = loaded["record"]
    frontier = _read_json(parent / "frontier_receipt.json", "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID")
    evidence_refs = tuple(
        evidence_reference_for_file(parent / name, role=role)
        for role, name in (
            ("n3_parent_overlay_parent_frontier", "frontier_receipt.json"),
            ("n3_parent_overlay_parent_manifest", "manifest.json"),
            ("n3_parent_overlay_parent_proposal_summary", "proposal_summary.json"),
            ("n3_parent_overlay_parent_score_panel", "score_panel.parquet"),
        )
    )
    request = build_parent_overlay_request(
        evidence_refs=evidence_refs,
        parent_bundle_path=parent.as_posix(),
        parent_bundle_id=parent.name,
        parent_request_sha256=parent_request.request_sha256,
        parent_receipt_sha256=loaded["receipt"].receipt_sha256,
        parent_frontier_sha256=str(frontier["frontier_sha256"]),
        dataset_identity=parent_record.dataset_identity,
        policy_identity=parent_record.policy_identity,
        registry_path=parent_request.registry_path,
        route_path=parent_request.route_path,
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    _write_immutable_request(Path(output_path).resolve(), request)
    return request


def run_parent_incremental_overlay(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = FrozenParentIncrementalOverlayRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    existing = _find_existing_bundle(request)
    _verify_environment(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    parent_path = Path(request.parent_bundle_path).resolve()
    parent_loaded = _read_parent_bundle(parent_path)
    source = _load_parent_score_panel(parent_path, parent_loaded, request)
    _check_resource_limits(request, "parent_score_panel_loaded")
    overlay_scores, activity = build_overlay_scores(source, request=request)
    _check_resource_limits(request, "overlay_scores_built")
    result_panel, daily, summary, frontier = evaluate_overlay_trials(
        source_panel=source,
        overlay_scores=overlay_scores,
        activity=activity,
        request=request,
    )
    _check_resource_limits(request, "overlay_metrics_evaluated")
    bundle = _publish_bundle(
        request=request,
        parent_loaded=parent_loaded,
        score_panel=result_panel,
        daily_metrics=daily,
        overlay_summary=summary,
        frontier=frontier,
        elapsed_seconds=time.monotonic() - started,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request, bundle, delivery, exact_retry=False)


def inspect_parent_incremental_overlay_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_overlay_bundle(Path(bundle_path).resolve())
    receipt = loaded["receipt"]
    frontier = _read_json(
        Path(bundle_path) / "frontier_receipt.json",
        "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
    )
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": receipt.receipt_id,
        "selected_trial_id": receipt.selected_trial_id,
        "eligible_trial_ids": list(receipt.eligible_trial_ids),
        "next_task": receipt.next_task,
        "frontier_sha256": frontier["frontier_sha256"],
        "planned_trial_count": 24,
        "generated_trial_count": 24,
        "evaluated_trial_count": 24,
        "selected_trial_count": receipt.selected_trial_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
    }


def build_overlay_scores(
    source_panel: pd.DataFrame,
    *,
    request: FrozenParentIncrementalOverlayRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build all 24 score overlays before any outcome values are used."""

    required = {*BASE_SCORE_COLUMNS, *PARENT_OVERLAY_CANDIDATES}
    if not required.issubset(source_panel.columns):
        _raise(
            "parent overlay source omits required columns",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(required - set(source_panel.columns)),
        )
    frame = source_panel.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)
    if frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "parent overlay source has duplicate PIT keys",
            "ADVISORY_N3_PARENT_OVERLAY_PIT_LEAKAGE",
        )
    output = frame[["decision_as_of_trade_date", "instrument"]].copy()
    output["parent_rank"] = np.nan
    for trial in request.trials:
        output[trial.trial_id] = np.nan
    activity_rows: list[dict[str, Any]] = []

    for decision_date, indexes in frame.groupby("decision_as_of_trade_date", sort=True).groups.items():
        idx = pd.Index(indexes)
        daily = frame.loc[idx]
        parent_score = pd.to_numeric(daily["score"], errors="coerce")
        parent_rank = parent_score.rank(method="average", pct=True)
        _validate_daily_parent_top5_parity(daily, parent_score, parent_rank, decision_date)
        output.loc[idx, "parent_rank"] = parent_rank.to_numpy(dtype="float32")
        parent_finite = np.isfinite(parent_rank)
        for candidate_id in PARENT_OVERLAY_CANDIDATES:
            candidate = pd.to_numeric(daily[candidate_id], errors="coerce")
            candidate_finite = np.isfinite(candidate)
            finite_count = int(candidate_finite.sum())
            unique_count = int(candidate[candidate_finite].nunique(dropna=True))
            active = finite_count >= 2 and unique_count > 1
            candidate_rank = candidate.rank(method="average", pct=True) if active else pd.Series(np.nan, index=idx)
            blend_mask = parent_finite & np.isfinite(candidate_rank)
            trials = [item for item in request.trials if item.candidate_id == candidate_id]
            for trial in trials:
                overlay = parent_rank.copy()
                if active:
                    overlay.loc[blend_mask] = (1.0 - trial.weight) * parent_rank.loc[
                        blend_mask
                    ] + trial.weight * candidate_rank.loc[blend_mask]
                if not np.array_equal(np.isfinite(overlay), parent_finite):
                    _raise(
                        "parent overlay changed parent finite coverage",
                        "ADVISORY_N3_PARENT_OVERLAY_COVERAGE_FAILED",
                        trial_id=trial.trial_id,
                        decision_date=str(decision_date),
                    )
                output.loc[idx, trial.trial_id] = overlay.to_numpy(dtype="float32")
            activity_rows.append(
                {
                    "candidate_id": candidate_id,
                    "decision_as_of_trade_date": pd.Timestamp(decision_date),
                    "row_count": len(daily),
                    "finite_candidate_count": finite_count,
                    "finite_candidate_fraction": float(candidate_finite.mean()) if len(daily) else 0.0,
                    "candidate_unique_count": unique_count,
                    "candidate_active": bool(active),
                    "blended_row_count": int(blend_mask.sum()) if active else 0,
                    "parent_passthrough_row_count": int(len(daily) - blend_mask.sum()) if active else len(daily),
                    "parent_passthrough_reason": "NONE" if active else "INACTIVE_OR_DEGENERATE",
                }
            )
    output["parent_rank"] = pd.to_numeric(output["parent_rank"], errors="coerce").astype("float32")
    for trial in request.trials:
        output[trial.trial_id] = pd.to_numeric(output[trial.trial_id], errors="coerce").astype("float32")
    return output, pd.DataFrame(activity_rows).sort_values(["candidate_id", "decision_as_of_trade_date"])


def evaluate_overlay_trials(
    *,
    source_panel: pd.DataFrame,
    overlay_scores: pd.DataFrame,
    activity: pd.DataFrame,
    request: FrozenParentIncrementalOverlayRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any]]:
    base = source_panel[list(BASE_SCORE_COLUMNS)].merge(
        overlay_scores,
        on=["decision_as_of_trade_date", "instrument"],
        how="left",
        validate="one_to_one",
    )
    if len(base) != len(source_panel):
        _raise(
            "parent overlay score merge changed row count",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
        )
    daily_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    for trial_index, trial in enumerate(request.trials):
        trial_activity = activity.loc[activity["candidate_id"] == trial.candidate_id]
        trial_daily = _evaluate_one_overlay_daily(base, trial=trial, activity=trial_activity)
        daily_rows.extend(trial_daily.to_dict("records"))
        summary_rows.append(
            _summarize_overlay_trial(
                trial=trial,
                daily=trial_daily,
                request=request,
                seed=request.bootstrap_seed + trial_index * 101,
            )
        )
    daily_metrics = pd.DataFrame(daily_rows).sort_values(["trial_id", "decision_as_of_trade_date"])
    eligible = [item for item in summary_rows if item["eligible"]]
    eligible.sort(
        key=lambda item: (
            -float(item["familywise_top5_lift_lower_bps"]),
            -float(item["familywise_rank_ic_delta_lower"]),
            int(item["weight_bps"]),
            str(item["trial_id"]),
        )
    )
    selected = eligible[0]["trial_id"] if eligible else None
    frontier = {
        "schema_version": "advisory_parent_incremental_overlay_frontier_v1",
        "request_sha256": request.request_sha256,
        "selection_rule": (
            "FWER_TOP5_LIFT_LOWER_DESC__FWER_RANKIC_DELTA_LOWER_DESC__WEIGHT_ASC__TRIAL_ID_ASC__SELECT_ONCE"
        ),
        "eligible_trial_ids": [item["trial_id"] for item in eligible],
        "selected_trial_id": selected,
        "selected_trial_count": 1 if selected else 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "position_weight_output": False,
    }
    frontier["frontier_sha256"] = canonical_json_sha256(frontier)
    summary = {
        "schema_version": "advisory_parent_incremental_overlay_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": len(summary_rows),
        "trials": summary_rows,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "position_weight_output": False,
    }
    keep = [
        "decision_as_of_trade_date",
        "instrument",
        "parent_rank",
        *[trial.trial_id for trial in request.trials],
    ]
    return base[keep], daily_metrics, summary, frontier


def _evaluate_one_overlay_daily(
    score_panel: pd.DataFrame,
    *,
    trial: ParentIncrementalOverlayTrialV1,
    activity: pd.DataFrame,
) -> pd.DataFrame:
    activity_by_date = {pd.Timestamp(row.decision_as_of_trade_date): row for row in activity.itertuples(index=False)}
    rows: list[dict[str, Any]] = []
    previous_parent: set[str] | None = None
    previous_overlay: set[str] | None = None
    for decision_date, frame in score_panel.groupby("decision_as_of_trade_date", sort=True):
        data = frame[
            [
                "instrument",
                "parent_rank",
                trial.trial_id,
                "economic_net_excess_bps",
                "outcome_known",
            ]
        ].copy()
        parent = pd.to_numeric(data["parent_rank"], errors="coerce")
        overlay = pd.to_numeric(data[trial.trial_id], errors="coerce")
        outcome = pd.to_numeric(data["economic_net_excess_bps"], errors="coerce")
        known = data["outcome_known"].fillna(False).astype(bool) & np.isfinite(outcome)
        evaluable = known & np.isfinite(parent) & np.isfinite(overlay)
        parent_ic = _safe_correlation(parent[evaluable], outcome[evaluable], method="spearman")
        overlay_ic = _safe_correlation(overlay[evaluable], outcome[evaluable], method="spearman")
        parent_top = data.loc[known & np.isfinite(parent)].nlargest(5, "parent_rank", keep="first")
        overlay_top = data.loc[known & np.isfinite(overlay)].nlargest(5, trial.trial_id, keep="first")
        if len(parent_top) != 5 or len(overlay_top) != 5:
            _raise(
                "parent overlay cannot form exact five-slot daily comparison",
                "ADVISORY_N3_PARENT_OVERLAY_COVERAGE_FAILED",
                trial_id=trial.trial_id,
                decision_date=str(decision_date),
                parent_top_count=len(parent_top),
                overlay_top_count=len(overlay_top),
            )
        parent_ids = set(parent_top["instrument"].astype(str)) if len(parent_top) == 5 else set()
        overlay_ids = set(overlay_top["instrument"].astype(str)) if len(overlay_top) == 5 else set()
        parent_top5 = (
            float(pd.to_numeric(parent_top["economic_net_excess_bps"]).mean()) if len(parent_top) == 5 else np.nan
        )
        overlay_top5 = (
            float(pd.to_numeric(overlay_top["economic_net_excess_bps"]).mean()) if len(overlay_top) == 5 else np.nan
        )
        replacement_count = 5 - len(parent_ids & overlay_ids) if len(parent_ids) == len(overlay_ids) == 5 else 0
        parent_churn = (
            np.nan
            if previous_parent is None or len(parent_ids) != 5
            else float(1.0 - len(previous_parent & parent_ids) / 5.0)
        )
        overlay_churn = (
            np.nan
            if previous_overlay is None or len(overlay_ids) != 5
            else float(1.0 - len(previous_overlay & overlay_ids) / 5.0)
        )
        if len(parent_ids) == 5:
            previous_parent = parent_ids
        if len(overlay_ids) == 5:
            previous_overlay = overlay_ids
        activity_row = activity_by_date[pd.Timestamp(decision_date)]
        rows.append(
            {
                "trial_id": trial.trial_id,
                "candidate_id": trial.candidate_id,
                "weight_bps": trial.weight_bps,
                "decision_as_of_trade_date": pd.Timestamp(decision_date),
                "row_count": len(data),
                "evaluable_count": int(evaluable.sum()),
                "candidate_active": bool(activity_row.candidate_active),
                "candidate_finite_fraction": float(activity_row.finite_candidate_fraction),
                "blended_row_count": int(activity_row.blended_row_count),
                "parent_passthrough_row_count": int(activity_row.parent_passthrough_row_count),
                "parent_passthrough_reason": str(activity_row.parent_passthrough_reason),
                "parent_rank_ic": parent_ic,
                "overlay_rank_ic": overlay_ic,
                "rank_ic_delta": overlay_ic - parent_ic,
                "parent_top5_net_excess_bps": parent_top5,
                "overlay_top5_net_excess_bps": overlay_top5,
                "top5_lift_bps": overlay_top5 - parent_top5,
                "top5_replacement_count": replacement_count,
                "top5_intervened": bool(replacement_count > 0),
                "parent_top5_churn": parent_churn,
                "overlay_top5_churn": overlay_churn,
                "overlay_parent_spearman": _safe_correlation(overlay[evaluable], parent[evaluable], method="spearman"),
            }
        )
    return pd.DataFrame(rows)


def _summarize_overlay_trial(
    *,
    trial: ParentIncrementalOverlayTrialV1,
    daily: pd.DataFrame,
    request: FrozenParentIncrementalOverlayRequestV1,
    seed: int,
) -> dict[str, Any]:
    rank_delta = _finite_array(daily["rank_ic_delta"])
    lift = _finite_array(daily["top5_lift_bps"])
    parent_ic = _finite_array(daily["parent_rank_ic"])
    overlay_ic = _finite_array(daily["overlay_rank_ic"])
    parent_top5 = _finite_array(daily["parent_top5_net_excess_bps"])
    overlay_top5 = _finite_array(daily["overlay_top5_net_excess_bps"])
    parent_churn = _finite_array(daily["parent_top5_churn"])
    overlay_churn = _finite_array(daily["overlay_top5_churn"])
    overlay_corr = _finite_array(daily["overlay_parent_spearman"])
    replacements = _finite_array(daily["top5_replacement_count"])
    raw_rank = _moving_block_interval(
        rank_delta,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed,
        alpha=0.05,
    )
    family_rank = _moving_block_interval(
        rank_delta,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed,
        alpha=0.05 / request.familywise_trial_count,
    )
    raw_lift = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed + 1,
        alpha=0.05,
    )
    family_lift = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=seed + 1,
        alpha=0.05 / request.familywise_trial_count,
    )
    evaluable_days = min(len(rank_delta), len(lift))
    intervened = daily.loc[daily["top5_intervened"].astype(bool), "decision_as_of_trade_date"]
    intervention_days = int(len(intervened))
    intervention_fraction = intervention_days / evaluable_days if evaluable_days else 0.0
    intervention_quarters = int(pd.to_datetime(intervened).dt.to_period("Q").nunique()) if intervention_days else 0
    reason_codes: list[str] = []
    if evaluable_days < request.minimum_evaluable_days:
        reason_codes.append("EVALUABLE_DAYS_BELOW_MINIMUM")
    if intervention_days < request.minimum_intervention_days:
        reason_codes.append("INTERVENTION_DAYS_BELOW_MINIMUM")
    if intervention_fraction < request.minimum_intervention_fraction:
        reason_codes.append("INTERVENTION_FRACTION_BELOW_MINIMUM")
    if intervention_quarters < request.minimum_intervention_quarters:
        reason_codes.append("INTERVENTION_QUARTERS_BELOW_MINIMUM")
    if family_rank[0] is None or family_rank[0] <= 0:
        reason_codes.append("FAMILYWISE_RANK_IC_DELTA_LOWER_NOT_POSITIVE")
    if family_lift[0] is None or family_lift[0] <= 0:
        reason_codes.append("FAMILYWISE_TOP5_LIFT_LOWER_NOT_POSITIVE")
    if not len(rank_delta) or not len(lift):
        reason_codes.append("DEGENERATE_DAILY_METRICS")
    dsr = _deflated_sharpe_diagnostic(lift, trial_count=request.familywise_trial_count)
    return {
        "trial_id": trial.trial_id,
        "candidate_id": trial.candidate_id,
        "source_expression_sha256": trial.source_expression_sha256,
        "weight_bps": trial.weight_bps,
        "weight": trial.weight,
        "evaluable_day_count": evaluable_days,
        "candidate_active_day_count": int(daily["candidate_active"].astype(bool).sum()),
        "candidate_finite_fraction_mean": float(daily["candidate_finite_fraction"].mean()),
        "intervention_day_count": intervention_days,
        "intervention_day_fraction": intervention_fraction,
        "intervention_quarter_count": intervention_quarters,
        "top5_replacement_count_sum": int(daily["top5_replacement_count"].sum()),
        "top5_replacement_count_mean": _mean_or_none(replacements),
        "parent_rank_ic_mean": _mean_or_none(parent_ic),
        "overlay_rank_ic_mean": _mean_or_none(overlay_ic),
        "rank_ic_delta_mean": _mean_or_none(rank_delta),
        "rank_ic_delta_median": _median_or_none(rank_delta),
        "rank_ic_delta_std": _std_or_none(rank_delta),
        "rank_ic_delta_positive_fraction": _positive_fraction(rank_delta),
        "rank_ic_delta_confidence_lower": raw_rank[0],
        "rank_ic_delta_confidence_upper": raw_rank[1],
        "familywise_rank_ic_delta_lower": family_rank[0],
        "familywise_rank_ic_delta_upper": family_rank[1],
        "parent_top5_mean_net_excess_bps": _mean_or_none(parent_top5),
        "overlay_top5_mean_net_excess_bps": _mean_or_none(overlay_top5),
        "top5_lift_mean_bps": _mean_or_none(lift),
        "top5_lift_confidence_lower_bps": raw_lift[0],
        "top5_lift_confidence_upper_bps": raw_lift[1],
        "familywise_top5_lift_lower_bps": family_lift[0],
        "familywise_top5_lift_upper_bps": family_lift[1],
        "parent_top5_churn_mean": _mean_or_none(parent_churn),
        "overlay_top5_churn_mean": _mean_or_none(overlay_churn),
        "overlay_parent_spearman_mean": _mean_or_none(overlay_corr),
        "daily_lift_sharpe": dsr["observed_sharpe"],
        "daily_lift_skew": dsr["skew"],
        "daily_lift_kurtosis": dsr["kurtosis"],
        "deflated_sharpe_probability": dsr["deflated_sharpe_probability"],
        "eligible": not reason_codes,
        "reason_codes": reason_codes,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
    }


def _validate_daily_parent_top5_parity(
    daily: pd.DataFrame,
    parent_score: pd.Series,
    parent_rank: pd.Series,
    decision_date: Any,
) -> None:
    finite = np.isfinite(parent_score) & np.isfinite(parent_rank)
    raw = daily.loc[finite].assign(_value=parent_score[finite]).nlargest(5, "_value", keep="first")
    ranked = daily.loc[finite].assign(_value=parent_rank[finite]).nlargest(5, "_value", keep="first")
    raw_ids = raw["instrument"].astype(str).tolist()
    rank_ids = ranked["instrument"].astype(str).tolist()
    if len(raw_ids) != 5 or raw_ids != rank_ids:
        _raise(
            "parent raw score and canonical rank Top5 are not exact",
            "ADVISORY_N3_PARENT_OVERLAY_BASELINE_PARITY_FAILED",
            decision_date=str(decision_date),
            raw_top5=raw_ids,
            rank_top5=rank_ids,
        )


def _validate_parent_navigation_source(path: Path, loaded: Mapping[str, Any]) -> None:
    manifest = loaded["manifest"]
    receipt = loaded["receipt"]
    record = loaded["record"]
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID")
    summary = _read_json(path / "proposal_summary.json", "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID")
    proposals = summary.get("proposals")
    if not isinstance(proposals, list):
        _raise(
            "parent overlay proposal summary is invalid",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
        )
    selected_navigation = {
        str(item.get("proposal_id"))
        for item in proposals
        if isinstance(item, dict)
        and isinstance(item.get("familywise_rank_ic_lower"), (int, float))
        and float(item["familywise_rank_ic_lower"]) > 0
        and isinstance(item.get("parent_score_spearman_mean"), (int, float))
        and abs(float(item["parent_score_spearman_mean"])) < 0.8
    }
    summary_by_id = {str(item.get("proposal_id")): item for item in proposals if isinstance(item, dict)}
    invalid = (
        summary.get("trial_count") != 24
        or len(proposals) != 24
        or len(summary_by_id) != 24
        or summary.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or summary.get("sealed_holdout_accessed") is not False
        or summary.get("deployable") is not False
        or manifest.get("study_type") != ResearchStudyType.EXPLORATORY_SCREEN.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or receipt.selected_trial_count != 0
        or receipt.selected_proposal_id is not None
        or record.experiment_id != "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1"
        or record.evaluated_trial_count != 24
        or frontier.get("selected_proposal_id") is not None
        or frontier.get("selected_trial_count") != 0
        or selected_navigation != set(PARENT_OVERLAY_CANDIDATES)
    )
    if invalid:
        _raise(
            "parent QE alpha bundle is not the frozen selected-zero navigation source",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
        )
    expected_hashes = {trial.candidate_id: trial.source_expression_sha256 for trial in build_default_overlay_trials()}
    if any(
        summary_by_id[candidate].get("expression_sha256") != digest for candidate, digest in expected_hashes.items()
    ):
        _raise(
            "parent overlay candidate expression identity drift",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
        )


def _load_parent_score_panel(
    parent_path: Path,
    parent_loaded: Mapping[str, Any],
    request: FrozenParentIncrementalOverlayRequestV1,
) -> pd.DataFrame:
    parent_request = parent_loaded["request"]
    expected = {*BASE_SCORE_COLUMNS, *[item.proposal_id for item in parent_request.proposals]}
    frame = pd.read_parquet(parent_path / "score_panel.parquet")
    if set(frame.columns) != expected:
        _raise(
            "parent overlay score panel schema drift",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(expected - set(frame.columns)),
            extra_columns=sorted(set(frame.columns) - expected),
        )
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    instruments = frame["instrument"].astype(str)
    if not instruments.eq(instruments.str.upper()).all():
        _raise(
            "parent overlay instruments are not canonical uppercase",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
        )
    frame["instrument"] = instruments
    if frame.empty or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "parent overlay score panel PIT keys are empty or duplicated",
            "ADVISORY_N3_PARENT_OVERLAY_PIT_LEAKAGE",
        )
    if not frame["outcome_known"].fillna(False).astype(bool).all():
        _raise(
            "parent overlay development panel contains unknown H20 outcomes",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
        )
    dates = frame["decision_as_of_trade_date"]
    if (
        dates.min().date() != request.signal_start
        or dates.max().date() != request.signal_end
        or dates.nunique() < request.minimum_evaluable_days
    ):
        _raise(
            "parent overlay development window drift",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
            start=str(dates.min()),
            end=str(dates.max()),
            decision_days=int(dates.nunique()),
        )
    return frame.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def _verify_environment(request: FrozenParentIncrementalOverlayRequestV1) -> None:
    repo = Path(request.repository_root)
    if _cross_os_git_commit(repo) != request.repository_commit:
        _raise(
            "parent overlay repository commit drift",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
        )
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "parent overlay repository became dirty",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
            dirty_paths=dirty[:20],
        )
    for reference in request.evidence_refs:
        _verify_ref(reference)
    parent_path = Path(request.parent_bundle_path).resolve()
    loaded = _read_parent_bundle(parent_path)
    _validate_parent_navigation_source(parent_path, loaded)
    frontier = _read_json(parent_path / "frontier_receipt.json", "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH")
    invalid = (
        loaded["manifest"]["bundle_id"] != request.parent_bundle_id
        or loaded["request"].request_sha256 != request.parent_request_sha256
        or loaded["receipt"].receipt_sha256 != request.parent_receipt_sha256
        or frontier.get("frontier_sha256") != request.parent_frontier_sha256
        or loaded["record"].dataset_identity != request.dataset_identity
        or loaded["record"].policy_identity != request.policy_identity
    )
    if invalid:
        _raise(
            "parent overlay request/source relational identity drift",
            "ADVISORY_N3_PARENT_OVERLAY_SOURCE_IDENTITY_MISMATCH",
        )


def _publish_bundle(
    *,
    request: FrozenParentIncrementalOverlayRequestV1,
    parent_loaded: Mapping[str, Any],
    score_panel: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    overlay_summary: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root) / "parent_incremental_overlay_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    _write_json(temporary / "request.json", request.model_dump(mode="json"))
    _write_json(
        temporary / "overlay_roster.json",
        {
            "schema_version": "advisory_parent_incremental_overlay_roster_v1",
            "request_sha256": request.request_sha256,
            "trials": [item.model_dump(mode="json") for item in request.trials],
            "trial_count": len(request.trials),
            "sealed_holdout_accessed": False,
            "position_weight_output": False,
        },
    )
    score_panel.to_parquet(temporary / "overlay_score_panel.parquet", index=False)
    daily_metrics.to_parquet(temporary / "daily_metrics.parquet", index=False)
    _write_json(temporary / "overlay_summary.json", overlay_summary)
    _write_json(temporary / "frontier_receipt.json", frontier)
    source_payload = {
        "schema_version": "advisory_parent_incremental_overlay_source_identity_v1",
        "request_sha256": request.request_sha256,
        "parent_bundle_id": request.parent_bundle_id,
        "parent_request_sha256": request.parent_request_sha256,
        "parent_receipt_sha256": request.parent_receipt_sha256,
        "parent_frontier_sha256": request.parent_frontier_sha256,
        "parent_registry_record_sha256": parent_loaded["record"].record_sha256,
        "evidence_refs": [item.model_dump(mode="json") for item in request.evidence_refs],
        "dataset_identity": request.dataset_identity,
        "policy_identity": request.policy_identity,
        "score_panel_row_count": len(score_panel),
        "decision_date_count": int(score_panel["decision_as_of_trade_date"].nunique()),
        "repository_commit": request.repository_commit,
        "sealed_holdout_accessed": False,
        "database_read_performed": False,
        "network_read_performed": False,
        "qlib_read_performed": False,
    }
    _write_json(temporary / "source_identity_receipt.json", source_payload)
    temporary_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    if temporary_bytes > request.resource_max_temp_bytes:
        _raise(
            "parent overlay temporary output exceeds frozen limit",
            "ADVISORY_N3_PARENT_OVERLAY_RESOURCE_LIMIT_EXCEEDED",
            temporary_bytes=temporary_bytes,
        )
    resource_payload = {
        "schema_version": "advisory_parent_incremental_overlay_resource_report_v1",
        "elapsed_seconds": float(elapsed_seconds),
        "peak_rss_bytes": _peak_rss_bytes(),
        "temporary_bytes": temporary_bytes,
        "resource_max_rss_bytes": request.resource_max_rss_bytes,
        "resource_max_temp_bytes": request.resource_max_temp_bytes,
        "wall_time_limit_seconds": None,
        "wall_time_is_telemetry_only": True,
    }
    _write_json(temporary / "resource_report.json", resource_payload)
    result_descriptors = _descriptors_for(temporary, RESULT_IDENTITY_MEMBERS)
    selected = frontier.get("selected_trial_id")
    eligible = tuple(str(item) for item in frontier.get("eligible_trial_ids", ()))
    receipt = build_parent_overlay_receipt(
        request_sha256=request.request_sha256,
        selected_trial_count=1 if selected else 0,
        selected_trial_id=selected,
        eligible_trial_ids=eligible,
        next_task=("N3_PARENT_OVERLAY_CONFIRMATION_DESIGN" if selected else "N3_ALPHA_INFORMATION_SET_EXPANSION_MVE"),
        source_identity_sha256=sha256_file(temporary / "source_identity_receipt.json"),
        result_files_sha256=canonical_json_sha256(result_descriptors),
        resource_report_sha256=sha256_file(temporary / "resource_report.json"),
    )
    _write_json(temporary / "overlay_receipt.json", receipt.model_dump(mode="json"))
    bundle_id = canonical_json_sha256(
        {
            "schema_version": PARENT_OVERLAY_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    destination = root / bundle_id
    record = _build_registry_record(
        request=request,
        receipt_path=temporary / "overlay_receipt.json",
        receipt_artifact_uri=(destination / "overlay_receipt.json").as_posix(),
        receipt=receipt,
    )
    _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
    descriptors = _file_descriptors(temporary)
    if set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "parent overlay bundle member roster drift",
            "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
            members=sorted(descriptors),
        )
    manifest = {
        "schema_version": PARENT_OVERLAY_BUNDLE_SCHEMA,
        "bundle_id": bundle_id,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "parent_bundle_id": request.parent_bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.EXPLORATORY_SCREEN.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "result_class": ResearchResultClass.EXPLORATORY.value,
        "planned_trial_count": 24,
        "generated_trial_count": 24,
        "evaluated_trial_count": 24,
        "selected_trial_count": receipt.selected_trial_count,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
        "files": descriptors,
    }
    _write_json(temporary / "manifest.json", manifest)
    if destination.exists():
        _read_overlay_bundle(destination)
        return destination
    temporary.replace(destination)
    _read_overlay_bundle(destination)
    return destination


def _build_registry_record(
    *,
    request: FrozenParentIncrementalOverlayRequestV1,
    receipt_path: Path,
    receipt_artifact_uri: str,
    receipt: ParentIncrementalOverlayReceiptV1,
) -> AdvisoryResearchTrialRecordV1:
    schema_identity = canonical_json_sha256(
        {
            "parent_bundle_id": request.parent_bundle_id,
            "trial_roster": [item.model_dump(mode="json") for item in request.trials],
            "overlay_formula": "(1-w)*parent_rank+w*candidate_rank__missing_or_inactive_fallback_parent",
        }
    )
    return build_trial_record(
        experiment_id=PARENT_OVERLAY_EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N3_PARENT_INCREMENTAL_OVERLAY_EXPLORATORY_SCREEN",
        study_type=ResearchStudyType.EXPLORATORY_SCREEN,
        hypothesis_family_id=PARENT_OVERLAY_HYPOTHESIS_FAMILY_ID,
        parent_lineage=(
            "ADVISORY-N1-TIER1-ORACLE",
            "ADVISORY-N2B-INDEPENDENT-PACKAGE-ALPHA-AUDIT-V2",
            "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1",
        ),
        unique_variable="FROZEN_6_NAVIGATION_SIGNALS_X_4_PARENT_RANK_OVERLAY_WEIGHTS",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity=request.dataset_identity,
        schema_identity=schema_identity,
        policy_identity=request.policy_identity,
        planned_trial_count=24,
        generated_trial_count=24,
        evaluated_trial_count=24,
        selected_trial_count=receipt.selected_trial_count,
        consumed_windows=(
            ConsumedWindowV1(
                window_id="P0C_DEVELOPMENT_CONSUMED_20240704_20260202",
                dataset_identity=request.dataset_identity,
                start_date=request.signal_start,
                end_date=request.signal_end,
            ),
        ),
        result_class=ResearchResultClass.EXPLORATORY,
        decision_use=DecisionUse.NAVIGATION_ONLY,
        evidence_refs=(
            evidence_reference_for_file(receipt_path, role="n3_parent_incremental_overlay_receipt").model_copy(
                update={"artifact_uri": receipt_artifact_uri}
            ),
        ),
        recorded_at=datetime.now(timezone.utc),
    )


def _read_overlay_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID")
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "parent overlay bundle descriptor roster is invalid",
            "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
        )
    for name, descriptor in descriptors.items():
        member = path / name
        actual_rows = _parquet_row_count(member) if member.suffix == ".parquet" and member.is_file() else None
        if (
            not member.is_file()
            or sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (actual_rows is not None and actual_rows != descriptor.get("row_count"))
        ):
            _raise(
                "parent overlay bundle member identity drift",
                "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
                member=name,
            )
    try:
        request = FrozenParentIncrementalOverlayRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = ParentIncrementalOverlayReceiptV1.model_validate_json(
            (path / "overlay_receipt.json").read_text(encoding="utf-8")
        )
        record = AdvisoryResearchTrialRecordV1.model_validate_json(
            (path / "registry_record.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "parent overlay bundle contract member is invalid",
            "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": PARENT_OVERLAY_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID")
    resource = _read_json(path / "resource_report.json", "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID")
    receipt_descriptor = descriptors["overlay_receipt.json"]
    invalid = (
        manifest.get("schema_version") != PARENT_OVERLAY_BUNDLE_SCHEMA
        or path.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or manifest.get("parent_bundle_id") != request.parent_bundle_id
        or receipt.request_sha256 != request.request_sha256
        or receipt.source_identity_sha256 != descriptors["source_identity_receipt.json"]["sha256"]
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or record.experiment_id != PARENT_OVERLAY_EXPERIMENT_ID
        or record.attempt_id != request.request_id
        or record.study_type != ResearchStudyType.EXPLORATORY_SCREEN
        or record.decision_use != DecisionUse.NAVIGATION_ONLY
        or record.result_class != ResearchResultClass.EXPLORATORY
        or record.planned_trial_count != 24
        or record.generated_trial_count != 24
        or record.evaluated_trial_count != 24
        or record.selected_trial_count != receipt.selected_trial_count
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].sha256 != receipt_descriptor["sha256"]
        or record.evidence_refs[0].size_bytes != receipt_descriptor["size_bytes"]
        or frontier.get("selected_trial_id") != receipt.selected_trial_id
        or frontier.get("selected_trial_count") != receipt.selected_trial_count
        or manifest.get("objective_contract") != ObjectiveContract.ALPHA_RANKING.value
        or manifest.get("study_type") != ResearchStudyType.EXPLORATORY_SCREEN.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or manifest.get("result_class") != ResearchResultClass.EXPLORATORY.value
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("factor_catalog_written") is not False
        or manifest.get("strategy_package_written") is not False
        or manifest.get("position_weight_output") is not False
        or resource.get("wall_time_limit_seconds") is not None
        or resource.get("wall_time_is_telemetry_only") is not True
        or not isinstance(resource.get("peak_rss_bytes"), int)
        or int(resource.get("peak_rss_bytes", -1)) > request.resource_max_rss_bytes
        or not isinstance(resource.get("temporary_bytes"), int)
        or int(resource.get("temporary_bytes", -1)) > request.resource_max_temp_bytes
    )
    if invalid:
        _raise(
            "parent overlay bundle relational identity is invalid",
            "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
        )
    return {"manifest": manifest, "request": request, "receipt": receipt, "record": record}


def _find_existing_bundle(request: FrozenParentIncrementalOverlayRequestV1) -> Path | None:
    root = Path(request.output_root) / "parent_incremental_overlay_bundles"
    if not root.exists():
        return None
    matches: list[Path] = []
    for path in root.iterdir():
        if not path.is_dir() or path.name.startswith(".") or not (path / "manifest.json").is_file():
            continue
        try:
            manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise(
            "one parent overlay request maps to multiple bundles",
            "ADVISORY_N3_PARENT_OVERLAY_BUNDLE_INVALID",
        )
    if matches:
        _read_overlay_bundle(matches[0])
        return matches[0]
    return None


def _deliver_bundle(
    *,
    request: FrozenParentIncrementalOverlayRequestV1,
    bundle_path: Path,
) -> dict[str, Any]:
    loaded = _read_overlay_bundle(bundle_path)
    registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((loaded["record"],))
    route = _write_route_page(
        path=Path(request.route_path),
        request=request,
        receipt=loaded["receipt"],
        bundle_id=loaded["manifest"]["bundle_id"],
        registry_sha256=str(registry["registry_sha256"]),
    )
    return {"registry": registry, "route": route}


def _write_route_page(
    *,
    path: Path,
    request: FrozenParentIncrementalOverlayRequestV1,
    receipt: ParentIncrementalOverlayReceiptV1,
    bundle_id: str,
    registry_sha256: str,
) -> dict[str, Any]:
    selected = receipt.selected_trial_id or "NONE"
    content = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            "- active_main_line: `N3_PARENT_INCREMENTAL_OVERLAY`",
            "- active_auxiliary_line: `NONE`",
            f"- next_task: `{receipt.next_task}`",
            f"- exploratory_candidate: `{selected}`",
            f"- parent_qe_alpha_bundle_id: `{request.parent_bundle_id}`",
            f"- parent_overlay_bundle_id: `{bundle_id}`",
            f"- trial_registry_sha256: `{registry_sha256}`",
            "- objective_contract: `ALPHA_RANKING`",
            "- decision_use: `NAVIGATION_ONLY`",
            "- sealed_holdout_accessed: `false`",
            "- deployable/runtime/factor/strategy_package/position_weight: `false/false/false/false/false`",
            "",
            "该页面只记录一次性探索路线，不构成确认、激活、资金仓位或交易输入。",
            "",
        )
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = content.encode("utf-8")
    if path.exists() and path.read_bytes() == encoded:
        return {
            "status": "EXACT_NOOP",
            "route_path": path.as_posix(),
            "route_sha256": sha256_file(path),
            "next_task": receipt.next_task,
        }
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {
        "status": "UPDATED",
        "route_path": path.as_posix(),
        "route_sha256": sha256_file(path),
        "next_task": receipt.next_task,
    }


def _run_response(
    request: FrozenParentIncrementalOverlayRequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    return {
        **inspect_parent_incremental_overlay_bundle(bundle),
        "request_id": request.request_id,
        "bundle_path": bundle.as_posix(),
        "exact_retry": exact_retry,
        "registry": dict(delivery["registry"]),
        "route": dict(delivery["route"]),
    }


def _check_resource_limits(request: FrozenParentIncrementalOverlayRequestV1, stage: str) -> None:
    rss = _peak_rss_bytes()
    if rss > request.resource_max_rss_bytes:
        _raise(
            "parent overlay resident memory exceeds frozen limit",
            "ADVISORY_N3_PARENT_OVERLAY_RESOURCE_LIMIT_EXCEEDED",
            stage=stage,
            peak_rss_bytes=rss,
        )


def _git_origin_main_commit(repository_root: Path) -> str:
    command, root = _git_command_for_worktree(repository_root)
    try:
        commit = (
            subprocess.run(
                [*command, "rev-parse", "origin/main"],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.strip()
            .lower()
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _raise(
            "parent overlay origin/main commit cannot be read",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise(
            "parent overlay origin/main commit is invalid",
            "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
            origin_main_commit=commit,
        )
    return commit


def _write_immutable_request(path: Path, request: FrozenParentIncrementalOverlayRequestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            request.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if path.exists():
        try:
            existing = FrozenParentIncrementalOverlayRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "existing parent overlay request is invalid",
                "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 != request.request_sha256 or path.read_bytes() != encoded:
            _raise(
                "parent overlay request path already contains different content",
                "ADVISORY_N3_PARENT_OVERLAY_REQUEST_INVALID",
            )
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "build_overlay_scores",
    "evaluate_overlay_trials",
    "inspect_parent_incremental_overlay_bundle",
    "prepare_parent_overlay_request",
    "run_parent_incremental_overlay",
]
