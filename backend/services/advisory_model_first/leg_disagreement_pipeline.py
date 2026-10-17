from __future__ import annotations

import json
import math
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, NoReturn, Sequence

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _git_command_for_worktree,
    _read_bundle as _read_n2a_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.leg_disagreement_contracts import (
    LEG_MVE_EXPERIMENT_ID,
    LEG_MVE_EXPANDED_FEATURES,
    LEG_MVE_FEATURE_SCHEMA_HASH,
    LEG_MVE_FEATURE_SCHEMA_VERSION,
    LEG_MVE_HYPOTHESIS_FAMILY_ID,
    LegDisagreementReceiptV1,
    FrozenLegDisagreementRequestV1,
    build_leg_disagreement_receipt,
    build_leg_disagreement_request,
)
from backend.services.advisory_model_first.parent_incremental_overlay_pipeline import (
    _read_overlay_bundle,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _cross_os_git_commit,
    _cross_os_git_dirty_paths,
    _deflated_sharpe_diagnostic,
    _file_descriptors,
    _moving_block_interval,
    _parquet_row_count,
    _peak_rss_bytes,
    _safe_correlation,
    _verify_ref,
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
from backend.services.advisory_model_first.tier1_oracle_pipeline import _read_n1_bundle
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SOURCE_SCORE_COLUMNS = (
    "score__LSTM_ONLY",
    "score__FUNDGROWTH_ONLY",
    "score__IC_WEIGHTED_PARENT",
)
SOURCE_REQUIRED_COLUMNS = (
    "decision_as_of_trade_date",
    "instrument",
    *SOURCE_SCORE_COLUMNS,
    "economic_net_excess_bps",
    "outcome_known",
)
MODEL_SCORE_COLUMNS = {
    "N3_LEG_LINEAR_COMPARATOR_V1": "linear_oof_score",
    "N3_LEG_DISAGREEMENT_EXPANDED_V1": "expanded_oof_score",
}
LEG_MVE_BUNDLE_SCHEMA = "advisory_n3_leg_disagreement_bundle_v1"
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "feature_schema.json",
        "feature_panel.parquet",
        "oof_score_panel.parquet",
        "fold_diagnostics.parquet",
        "daily_metrics.parquet",
        "model_summary.json",
        "frontier_receipt.json",
    }
)
BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "source_identity_receipt.json",
    "resource_report.json",
    "learnability_receipt.json",
    "registry_record.json",
}


def prepare_leg_disagreement_request(
    *,
    parent_overlay_bundle_path: str | Path,
    n2a_bundle_path: str | Path,
    n1_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> FrozenLegDisagreementRequestV1:
    """Freeze one development-only N3 leg-disagreement learnability request."""

    parent_path = Path(parent_overlay_bundle_path).resolve()
    n2a_path = Path(n2a_bundle_path).resolve()
    n1_path = Path(n1_bundle_path).resolve()
    repo = Path(repository_root).resolve()
    parent = _read_overlay_bundle(parent_path)
    n2a = _read_n2a_bundle(n2a_path)
    n1 = _read_n1_bundle(n1_path)
    _validate_bound_sources(parent_path=parent_path, parent=parent, n2a_path=n2a_path, n2a=n2a, n1_path=n1_path, n1=n1)
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "leg disagreement request requires a clean repository",
            "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repo)
    origin_main_commit = _git_origin_main_commit(repo)
    if commit != origin_main_commit:
        _raise(
            "leg disagreement formal request requires HEAD to equal origin/main",
            "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main_commit,
        )
    evidence_refs = tuple(
        evidence_reference_for_file(path, role=role)
        for role, path in (
            ("n3_leg_parent_overlay_manifest", parent_path / "manifest.json"),
            ("n3_leg_parent_overlay_receipt", parent_path / "overlay_receipt.json"),
            (
                "n3_leg_parent_qe_score_panel",
                _resolve_bound_path(parent["request"].parent_bundle_path) / "score_panel.parquet",
            ),
            ("n3_leg_n2a_manifest", n2a_path / "manifest.json"),
            ("n3_leg_n2a_request", n2a_path / "request.json"),
            ("n3_leg_n2a_full_universe", n2a_path / "full_universe_signal_outcomes.parquet"),
            ("n3_leg_n1_manifest", n1_path / "manifest.json"),
            ("n3_leg_n1_cpcv", n1_path / "n1_label_interval_cpcv.json"),
            ("n3_leg_n1_regime_daily", n1_path / "learnability_daily.parquet"),
        )
    )
    source_dataset_identity = n2a["record"].dataset_identity
    parent_dataset_identity = parent["record"].dataset_identity
    split_identity = n1["request"].split_policy_sha256
    dataset_identity = canonical_json_sha256(
        {
            "source_dataset_identity": source_dataset_identity,
            "parent_dataset_identity": parent_dataset_identity,
            "n1_split_policy_sha256": split_identity,
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }
    )
    request = build_leg_disagreement_request(
        evidence_refs=evidence_refs,
        parent_overlay_bundle_path=parent_path.as_posix(),
        parent_overlay_bundle_id=parent_path.name,
        parent_overlay_request_sha256=parent["request"].request_sha256,
        parent_overlay_receipt_sha256=parent["receipt"].receipt_sha256,
        n2a_bundle_path=n2a_path.as_posix(),
        n2a_bundle_id=n2a_path.name,
        n2a_request_sha256=n2a["request"].request_sha256,
        n2a_receipt_sha256=n2a["receipt"].receipt_sha256,
        n1_bundle_path=n1_path.as_posix(),
        n1_bundle_id=n1_path.name,
        n1_request_sha256=n1["request"].request_sha256,
        n1_split_policy_sha256=split_identity,
        source_dataset_identity=source_dataset_identity,
        parent_dataset_identity=parent_dataset_identity,
        dataset_identity=dataset_identity,
        policy_identity=parent["record"].policy_identity,
        registry_path=_resolve_bound_path(parent["request"].registry_path).as_posix(),
        route_path=_resolve_bound_path(parent["request"].route_path).as_posix(),
        repository_root=repo.as_posix(),
        repository_commit=commit,
        output_root=Path(output_root).resolve().as_posix(),
    )
    _write_immutable_request(Path(output_path).resolve(), request)
    return request


def run_leg_disagreement_mve(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    request = FrozenLegDisagreementRequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    existing = _find_existing_bundle(request)
    _verify_environment(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request, existing, delivery, exact_retry=True)
    sources = _load_verified_sources(request)
    _check_resource_limits(request, "sources_loaded")
    features = build_leg_feature_panel(sources["n2a_source"])
    _validate_parent_source_parity(features=features, parent_panel=sources["parent_panel"], request=request)
    _check_resource_limits(request, "feature_panel_built")
    oof, fold_diagnostics = run_leg_crossfit(
        features=features,
        paths=sources["cpcv"]["paths"],
        request=request,
    )
    _check_resource_limits(request, "crossfit_complete")
    daily, summary, frontier = evaluate_leg_models(
        oof_scores=oof,
        regime_daily=sources["regime_daily"],
        request=request,
    )
    _validate_parent_daily_parity(daily=daily, parent_daily=sources["parent_daily"])
    _check_resource_limits(request, "evaluation_complete")
    bundle = _publish_bundle(
        request=request,
        features=features,
        oof_scores=oof,
        fold_diagnostics=fold_diagnostics,
        daily_metrics=daily,
        model_summary=summary,
        frontier=frontier,
        elapsed_seconds=time.monotonic() - started,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return _run_response(request, bundle, delivery, exact_retry=False)


def inspect_leg_disagreement_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_leg_bundle(Path(bundle_path).resolve())
    receipt = loaded["receipt"]
    frontier = _read_json(Path(bundle_path) / "frontier_receipt.json", "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID")
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_id": loaded["request"].request_id,
        "receipt_id": receipt.receipt_id,
        "selected_trial_id": receipt.selected_trial_id,
        "eligible_trial_ids": list(receipt.eligible_trial_ids),
        "next_task": receipt.next_task,
        "frontier_sha256": frontier["frontier_sha256"],
        "planned_trial_count": 2,
        "generated_trial_count": 2,
        "evaluated_trial_count": 2,
        "selected_trial_count": receipt.selected_trial_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
    }


def build_leg_feature_panel(source: pd.DataFrame) -> pd.DataFrame:
    """Build the exact T-visible leg consensus/disagreement feature roster."""

    missing = set(SOURCE_REQUIRED_COLUMNS) - set(source.columns)
    if missing:
        _raise(
            "leg disagreement source omits required columns",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(missing),
        )
    frame = source.loc[:, SOURCE_REQUIRED_COLUMNS].copy()
    frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    instruments = frame["instrument"].astype(str)
    if not instruments.eq(instruments.str.upper()).all():
        _raise(
            "leg disagreement source instruments are not canonical uppercase",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    frame["instrument"] = instruments
    if frame.empty or frame.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "leg disagreement source PIT keys are empty or duplicated",
            "ADVISORY_N3_LEG_MVE_PIT_LEAKAGE",
        )
    for column in SOURCE_SCORE_COLUMNS:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
        if not np.isfinite(frame[column].to_numpy(dtype=float)).all():
            _raise(
                "leg disagreement source score is not fully finite",
                "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
                column=column,
            )
    ranked = frame.groupby("decision_as_of_trade_date", sort=False)[list(SOURCE_SCORE_COLUMNS)].rank(
        method="average",
        pct=True,
        ascending=True,
    )
    frame["lstm_rank_pct"] = ranked["score__LSTM_ONLY"].astype("float32")
    frame["fund_rank_pct"] = ranked["score__FUNDGROWTH_ONLY"].astype("float32")
    frame["parent_rank_pct"] = ranked["score__IC_WEIGHTED_PARENT"].astype("float32")
    frame["leg_rank_signed_gap"] = (frame["lstm_rank_pct"] - frame["fund_rank_pct"]).astype("float32")
    frame["leg_rank_abs_gap"] = frame["leg_rank_signed_gap"].abs().astype("float32")
    frame["leg_rank_consensus_min"] = frame[["lstm_rank_pct", "fund_rank_pct"]].min(axis=1).astype("float32")
    frame["leg_rank_consensus_product"] = (frame["lstm_rank_pct"] * frame["fund_rank_pct"]).astype("float32")
    frame["parent_rank_x_agreement"] = (frame["parent_rank_pct"] * (1.0 - frame["leg_rank_abs_gap"])).astype("float32")
    feature_values = frame.loc[:, LEG_MVE_EXPANDED_FEATURES].to_numpy(dtype=float)
    if not np.isfinite(feature_values).all():
        _raise(
            "leg disagreement feature builder produced non-finite values",
            "ADVISORY_N3_LEG_MVE_PIT_LEAKAGE",
        )
    ordered = (
        "decision_as_of_trade_date",
        "instrument",
        *SOURCE_SCORE_COLUMNS,
        *LEG_MVE_EXPANDED_FEATURES,
        "economic_net_excess_bps",
        "outcome_known",
    )
    return frame.loc[:, ordered].sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def run_leg_crossfit(
    *,
    features: pd.DataFrame,
    paths: Sequence[Mapping[str, Any]],
    request: FrozenLegDisagreementRequestV1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fit two frozen Ridge trials and average exactly seven OOF predictions per row."""

    try:
        from sklearn.linear_model import Ridge
        from sklearn.preprocessing import StandardScaler
    except ImportError as exc:  # pragma: no cover - dependency is a runtime contract
        _raise(
            "leg disagreement sklearn dependency is unavailable",
            "ADVISORY_N3_LEG_MVE_OOF_INVALID",
            error_type=type(exc).__name__,
        )
    frame = features.copy().reset_index(drop=True)
    labels = pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    evaluable = frame["outcome_known"].fillna(False).astype(bool).to_numpy() & np.isfinite(labels)
    if len(paths) != request.expected_ready_path_count:
        _raise(
            "leg disagreement CPCV path count drift",
            "ADVISORY_N3_LEG_MVE_CPCV_INVALID",
            expected=request.expected_ready_path_count,
            actual=len(paths),
        )
    dates = pd.DatetimeIndex(frame["decision_as_of_trade_date"]).normalize()
    source_dates = set(dates.unique())
    score_output = frame.loc[
        :,
        [
            "decision_as_of_trade_date",
            "instrument",
            "parent_rank_pct",
            "economic_net_excess_bps",
            "outcome_known",
        ],
    ].copy()
    diagnostics: list[dict[str, Any]] = []
    for trial in request.model_trials:
        sums: np.ndarray = np.zeros(len(frame), dtype=np.float64)
        counts: np.ndarray = np.zeros(len(frame), dtype=np.uint8)
        columns = list(trial.feature_columns)
        for path in paths:
            if path.get("status") != "READY":
                _raise(
                    "leg disagreement CPCV path is not READY",
                    "ADVISORY_N3_LEG_MVE_CPCV_INVALID",
                    path_id=path.get("path_id"),
                )
            train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize()
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize()
            if not len(train_dates) or not len(validation_dates) or set(train_dates) & set(validation_dates):
                _raise(
                    "leg disagreement CPCV train/validation date identity is invalid",
                    "ADVISORY_N3_LEG_MVE_CPCV_INVALID",
                    path_id=path.get("path_id"),
                )
            if not set(train_dates).issubset(source_dates) or not set(validation_dates).issubset(source_dates):
                _raise(
                    "leg disagreement CPCV path references a date outside the source panel",
                    "ADVISORY_N3_LEG_MVE_CPCV_INVALID",
                    path_id=path.get("path_id"),
                )
            train_mask = dates.isin(train_dates) & evaluable
            validation_mask = dates.isin(validation_dates)
            train_index = np.flatnonzero(train_mask)
            validation_index = np.flatnonzero(validation_mask)
            if not len(train_index) or not len(validation_index):
                _raise(
                    "leg disagreement CPCV path has no train or validation rows",
                    "ADVISORY_N3_LEG_MVE_CPCV_INVALID",
                    path_id=path.get("path_id"),
                )
            scaler = StandardScaler()
            x_train = scaler.fit_transform(frame.loc[train_index, columns].to_numpy(dtype=float))
            x_validation = scaler.transform(frame.loc[validation_index, columns].to_numpy(dtype=float))
            model = Ridge(alpha=trial.alpha, solver=trial.solver, fit_intercept=trial.fit_intercept)
            model.fit(x_train, labels[train_index])
            predicted = np.asarray(model.predict(x_validation), dtype=float)
            if not np.isfinite(predicted).all():
                _raise(
                    "leg disagreement Ridge produced non-finite OOF predictions",
                    "ADVISORY_N3_LEG_MVE_OOF_INVALID",
                    path_id=path.get("path_id"),
                    trial_id=trial.trial_id,
                )
            sums[validation_index] += predicted
            counts[validation_index] += 1
            diagnostics.append(
                {
                    "trial_id": trial.trial_id,
                    "path_id": str(path.get("path_id")),
                    "train_row_count": int(len(train_index)),
                    "validation_row_count": int(len(validation_index)),
                    "coefficient_json": json.dumps(
                        [float(value) for value in np.asarray(model.coef_).reshape(-1)],
                        separators=(",", ":"),
                    ),
                    "intercept": float(model.intercept_),
                }
            )
        if not np.equal(counts, request.expected_oof_predictions_per_row).all():
            unique, frequencies = np.unique(counts, return_counts=True)
            _raise(
                "leg disagreement OOF prediction multiplicity drift",
                "ADVISORY_N3_LEG_MVE_OOF_INVALID",
                trial_id=trial.trial_id,
                counts={str(int(key)): int(value) for key, value in zip(unique, frequencies, strict=True)},
            )
        score_output[MODEL_SCORE_COLUMNS[trial.trial_id]] = (sums / counts).astype("float32")
        score_output[f"{MODEL_SCORE_COLUMNS[trial.trial_id]}_count"] = counts
    return score_output, pd.DataFrame(diagnostics).sort_values(["trial_id", "path_id"]).reset_index(drop=True)


def evaluate_leg_models(
    *,
    oof_scores: pd.DataFrame,
    regime_daily: pd.DataFrame,
    request: FrozenLegDisagreementRequestV1,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    """Evaluate the fixed expanded candidate against parent and linear comparator."""

    required = {
        "decision_as_of_trade_date",
        "instrument",
        "parent_rank_pct",
        "economic_net_excess_bps",
        "outcome_known",
        "linear_oof_score",
        "expanded_oof_score",
    }
    if not required.issubset(oof_scores.columns):
        _raise(
            "leg disagreement OOF score panel schema drift",
            "ADVISORY_N3_LEG_MVE_OOF_INVALID",
            missing_columns=sorted(required - set(oof_scores.columns)),
        )
    scores = oof_scores.copy()
    scores["decision_as_of_trade_date"] = pd.to_datetime(scores["decision_as_of_trade_date"]).dt.normalize()
    if scores.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise(
            "leg disagreement OOF score keys are duplicated",
            "ADVISORY_N3_LEG_MVE_OOF_INVALID",
        )
    numeric_scores = scores[["parent_rank_pct", "linear_oof_score", "expanded_oof_score"]]
    if not np.isfinite(numeric_scores.to_numpy(dtype=float)).all():
        _raise(
            "leg disagreement OOF score panel contains non-finite values",
            "ADVISORY_N3_LEG_MVE_OOF_INVALID",
        )
    regimes = regime_daily.loc[:, ["decision_as_of_trade_date", "regime"]].copy()
    regimes["decision_as_of_trade_date"] = pd.to_datetime(regimes["decision_as_of_trade_date"]).dt.normalize()
    if regimes.duplicated(["decision_as_of_trade_date"]).any():
        _raise(
            "leg disagreement regime dates are duplicated",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    regime_map = regimes.set_index("decision_as_of_trade_date")["regime"].astype(str).to_dict()
    rows: list[dict[str, Any]] = []
    previous: dict[str, set[str] | None] = {"parent": None, "linear": None, "expanded": None}
    for decision_date, frame in scores.groupby("decision_as_of_trade_date", sort=True):
        parent_ids = _top_ids(frame, "parent_rank_pct")
        linear_ids = _top_ids(frame, "linear_oof_score")
        expanded_ids = _top_ids(frame, "expanded_oof_score")
        parent_top5_evaluable = _top5_outcome_evaluable(frame, parent_ids)
        linear_top5_evaluable = _top5_outcome_evaluable(frame, linear_ids)
        expanded_top5_evaluable = _top5_outcome_evaluable(frame, expanded_ids)
        finite_label = frame["outcome_known"].fillna(False).astype(bool) & np.isfinite(
            pd.to_numeric(frame["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
        )
        labeled = frame.loc[finite_label]
        row = {
            "decision_as_of_trade_date": decision_date,
            "regime": regime_map.get(decision_date),
            "row_count": int(len(frame)),
            "finite_label_row_count": int(finite_label.sum()),
            "parent_rank_ic": _safe_correlation(
                labeled["parent_rank_pct"], labeled["economic_net_excess_bps"], method="spearman"
            ),
            "linear_rank_ic": _safe_correlation(
                labeled["linear_oof_score"], labeled["economic_net_excess_bps"], method="spearman"
            ),
            "expanded_rank_ic": _safe_correlation(
                labeled["expanded_oof_score"], labeled["economic_net_excess_bps"], method="spearman"
            ),
            "parent_top5_net_excess_bps": _top5_net_value(frame, parent_ids),
            "linear_top5_net_excess_bps": _top5_net_value(frame, linear_ids),
            "expanded_top5_net_excess_bps": _top5_net_value(frame, expanded_ids),
            "parent_top5_evaluable": parent_top5_evaluable,
            "linear_top5_evaluable": linear_top5_evaluable,
            "expanded_top5_evaluable": expanded_top5_evaluable,
            "parent_instruments": ",".join(sorted(parent_ids)),
            "linear_instruments": ",".join(sorted(linear_ids)),
            "expanded_instruments": ",".join(sorted(expanded_ids)),
            "expanded_parent_replacement_count": int(5 - len(parent_ids & expanded_ids)),
            "expanded_linear_replacement_count": int(5 - len(linear_ids & expanded_ids)),
            "intervened": expanded_ids != parent_ids,
        }
        for name, ids in (("parent", parent_ids), ("linear", linear_ids), ("expanded", expanded_ids)):
            prior = previous[name]
            row[f"{name}_top5_churn"] = None if prior is None else float(1.0 - len(prior & ids) / 5.0)
            previous[name] = ids
        rows.append(row)
    daily = pd.DataFrame(rows)
    for prefix in ("linear", "expanded"):
        daily[f"{prefix}_rank_ic_delta_parent"] = daily[f"{prefix}_rank_ic"] - daily["parent_rank_ic"]
        daily[f"{prefix}_top5_lift_parent_bps"] = (
            daily[f"{prefix}_top5_net_excess_bps"] - daily["parent_top5_net_excess_bps"]
        )
    daily["expanded_rank_ic_delta_linear"] = daily["expanded_rank_ic"] - daily["linear_rank_ic"]
    daily["expanded_top5_lift_linear_bps"] = daily["expanded_top5_net_excess_bps"] - daily["linear_top5_net_excess_bps"]
    paired_columns = [
        "expanded_rank_ic_delta_parent",
        "expanded_top5_lift_parent_bps",
        "expanded_rank_ic_delta_linear",
        "expanded_top5_lift_linear_bps",
    ]
    daily["evaluable"] = np.isfinite(daily[paired_columns].to_numpy(dtype=float)).all(axis=1)
    alpha = 0.05 / request.familywise_hypothesis_count
    inference = {
        "expanded_parent_rank_ic_delta": _metric_inference(
            daily["expanded_rank_ic_delta_parent"], request=request, alpha=alpha, threshold=0.0, seed_offset=0
        ),
        "expanded_parent_top5_lift_bps": _metric_inference(
            daily["expanded_top5_lift_parent_bps"],
            request=request,
            alpha=alpha,
            threshold=request.minimum_parent_lift_bps,
            seed_offset=1,
        ),
        "expanded_linear_rank_ic_delta": _metric_inference(
            daily["expanded_rank_ic_delta_linear"], request=request, alpha=alpha, threshold=0.0, seed_offset=2
        ),
        "expanded_linear_top5_lift_bps": _metric_inference(
            daily["expanded_top5_lift_linear_bps"], request=request, alpha=alpha, threshold=0.0, seed_offset=3
        ),
    }
    evaluable_days = daily["evaluable"].astype(bool)
    intervention = daily["intervened"].astype(bool) & evaluable_days
    mapped = daily["regime"].notna() & evaluable_days
    by_regime = daily.loc[intervention & mapped].groupby("regime", sort=True).size().astype(int).to_dict()
    observed_regimes = sorted(daily.loc[mapped, "regime"].astype(str).unique())
    support_reasons: list[str] = []
    evaluable_day_count = int(evaluable_days.sum())
    if evaluable_day_count < request.minimum_evaluable_days:
        support_reasons.append("EVALUABLE_DAY_COUNT_BELOW_MINIMUM")
    if int(intervention.sum()) < request.minimum_intervention_days:
        support_reasons.append("INTERVENTION_DAY_COUNT_BELOW_MINIMUM")
    intervention_fraction = float(intervention.sum() / evaluable_day_count) if evaluable_day_count else 0.0
    if intervention_fraction < request.minimum_intervention_fraction:
        support_reasons.append("INTERVENTION_FRACTION_BELOW_MINIMUM")
    if any(by_regime.get(regime, 0) < request.minimum_intervention_days_per_regime for regime in observed_regimes):
        support_reasons.append("INTERVENTION_REGIME_SUPPORT_BELOW_MINIMUM")
    reasons = list(support_reasons)
    required_inference = (
        ("expanded_parent_rank_ic_delta", 0.0),
        ("expanded_parent_top5_lift_bps", request.minimum_parent_lift_bps),
        ("expanded_linear_rank_ic_delta", 0.0),
        ("expanded_linear_top5_lift_bps", 0.0),
    )
    for name, threshold in required_inference:
        lower = inference[name]["familywise_confidence_lower"]
        if lower is None or float(lower) <= threshold:
            reasons.append(f"{name.upper()}_LOWER_NOT_ABOVE_THRESHOLD")
    eligible = not reasons
    selected = "N3_LEG_DISAGREEMENT_EXPANDED_V1" if eligible else None
    support = {
        "evaluable_day_count": evaluable_day_count,
        "total_decision_day_count": int(len(daily)),
        "intervention_day_count": int(intervention.sum()),
        "intervention_fraction": intervention_fraction,
        "intervention_days_by_regime": {str(key): int(value) for key, value in by_regime.items()},
        "regime_mapped_day_count": int(mapped.sum()),
        "support_sufficient": not support_reasons,
        "reason_codes": support_reasons,
    }
    summary = {
        "schema_version": "advisory_n3_leg_disagreement_model_summary_v1",
        "request_sha256": request.request_sha256,
        "trial_count": 2,
        "familywise_hypothesis_count": request.familywise_hypothesis_count,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "support": support,
        "top5_evaluable_day_count": {
            name: int(daily[f"{name}_top5_evaluable"].astype(bool).sum()) for name in ("parent", "linear", "expanded")
        },
        "inference": inference,
        "parent_rank_ic_mean": _mean(daily["parent_rank_ic"]),
        "linear_rank_ic_mean": _mean(daily["linear_rank_ic"]),
        "expanded_rank_ic_mean": _mean(daily["expanded_rank_ic"]),
        "parent_top5_mean_net_excess_bps": _mean(daily["parent_top5_net_excess_bps"]),
        "linear_top5_mean_net_excess_bps": _mean(daily["linear_top5_net_excess_bps"]),
        "expanded_top5_mean_net_excess_bps": _mean(daily["expanded_top5_net_excess_bps"]),
        "expanded_parent_lift_dsr": _deflated_sharpe_diagnostic(
            daily["expanded_top5_lift_parent_bps"].tolist(), trial_count=2
        ),
        "expanded_parent_score_spearman_mean": _mean_by_day_score_correlation(
            scores, "expanded_oof_score", "parent_rank_pct"
        ),
        "eligible": eligible,
        "reason_codes": reasons,
        "selected_trial_id": selected,
    }
    frontier_payload = {
        "schema_version": "advisory_n3_leg_disagreement_frontier_v1",
        "request_sha256": request.request_sha256,
        "eligible_trial_ids": (["N3_LEG_DISAGREEMENT_EXPANDED_V1"] if eligible else []),
        "selected_trial_id": selected,
        "selected_trial_count": 1 if selected else 0,
        "candidate_reselection_allowed": False,
        "exact_retry_allowed": True,
        "selection_rule": "ALL_FOUR_FAMILYWISE_LOWERS_AND_SUPPORT__SELECT_ONCE",
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "sealed_holdout_accessed": False,
        "deployable": False,
    }
    frontier_payload["frontier_sha256"] = canonical_json_sha256(frontier_payload)
    return daily, summary, frontier_payload


def _validate_bound_sources(
    *,
    parent_path: Path,
    parent: Mapping[str, Any],
    n2a_path: Path,
    n2a: Mapping[str, Any],
    n1_path: Path,
    n1: Mapping[str, Any],
) -> None:
    del parent_path, n2a_path, n1_path
    parent_receipt = parent["receipt"]
    invalid = (
        parent["record"].experiment_id != "ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1"
        or parent_receipt.selected_trial_count != 0
        or parent_receipt.selected_trial_id is not None
        or parent_receipt.next_task != "N3_ALPHA_INFORMATION_SET_EXPANSION_MVE"
        or parent_receipt.decision_use != DecisionUse.NAVIGATION_ONLY
        or parent_receipt.sealed_holdout_accessed is not False
        or parent_receipt.deployable is not False
        or n2a["record"].experiment_id != "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT"
        or n2a["record"].evaluated_trial_count != 0
        or n2a["record"].decision_use != DecisionUse.NAVIGATION_ONLY
        or n2a["record"].policy_identity != parent["record"].policy_identity
        or n1["request"].decision_date_start.isoformat() != "2024-07-04"
        or n1["request"].decision_date_end.isoformat() != "2026-02-02"
        or n1["learnability"].sealed_holdout_accessed is not False
    )
    if invalid:
        _raise(
            "leg disagreement bound source relation is invalid",
            "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
        )


def _load_verified_sources(request: FrozenLegDisagreementRequestV1) -> dict[str, Any]:
    parent_path = Path(request.parent_overlay_bundle_path).resolve()
    n2a_path = Path(request.n2a_bundle_path).resolve()
    n1_path = Path(request.n1_bundle_path).resolve()
    parent = _read_overlay_bundle(parent_path)
    n2a = _read_n2a_bundle(n2a_path)
    n1 = _read_n1_bundle(n1_path)
    _validate_bound_sources(parent_path=parent_path, parent=parent, n2a_path=n2a_path, n2a=n2a, n1_path=n1_path, n1=n1)
    _validate_request_source_identities(request=request, parent=parent, n2a=n2a, n1=n1)
    source_path = n2a_path / "full_universe_signal_outcomes.parquet"
    source = _read_parquet(source_path, "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH")
    missing = set(SOURCE_REQUIRED_COLUMNS) - set(source.columns)
    if missing:
        _raise(
            "leg disagreement N2-A source schema drift",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(missing),
        )
    dates = pd.to_datetime(source["decision_as_of_trade_date"]).dt.normalize()
    known = source["outcome_known"].fillna(False).astype(bool)
    finite_label = np.isfinite(pd.to_numeric(source["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float))
    evaluable = known.to_numpy(dtype=bool) & finite_label
    nonfinite_known = known.to_numpy(dtype=bool) & ~finite_label
    if (
        len(source) != request.expected_source_row_count
        or int(known.sum()) != request.expected_known_row_count
        or int(evaluable.sum()) != request.expected_evaluable_row_count
        or int(nonfinite_known.sum()) != request.expected_nonfinite_known_row_count
        or int((~known).sum()) != request.expected_unknown_row_count
        or dates.nunique() != request.expected_decision_date_count
        or dates.min().date() != request.signal_start
        or dates.max().date() != request.signal_end
    ):
        _raise(
            "leg disagreement N2-A source coverage drift",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
            row_count=len(source),
            known_count=int(known.sum()),
            evaluable_count=int(evaluable.sum()),
            nonfinite_known_count=int(nonfinite_known.sum()),
            unknown_count=int((~known).sum()),
            decision_date_count=int(dates.nunique()),
        )
    parent_panel = _read_parquet(
        _resolve_bound_path(parent["request"].parent_bundle_path) / "score_panel.parquet",
        "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        columns=[
            "decision_as_of_trade_date",
            "instrument",
            "score",
            "economic_net_excess_bps",
            "outcome_known",
        ],
    )
    parent_daily_raw = _read_parquet(
        parent_path / "daily_metrics.parquet",
        "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        columns=[
            "trial_id",
            "decision_as_of_trade_date",
            "parent_rank_ic",
            "parent_top5_net_excess_bps",
            "parent_top5_churn",
        ],
    )
    parity_columns = ["parent_rank_ic", "parent_top5_net_excess_bps", "parent_top5_churn"]
    if parent_daily_raw.groupby("decision_as_of_trade_date")[parity_columns].nunique(dropna=False).gt(1).any().any():
        _raise(
            "leg disagreement parent overlay baseline is not invariant across trials",
            "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED",
        )
    parent_daily = (
        parent_daily_raw.sort_values(["decision_as_of_trade_date", "trial_id"])
        .drop_duplicates("decision_as_of_trade_date", keep="first")
        .drop(columns="trial_id")
        .reset_index(drop=True)
    )
    cpcv = _read_json(n1_path / "n1_label_interval_cpcv.json", "ADVISORY_N3_LEG_MVE_CPCV_INVALID")
    if (
        cpcv.get("request_sha256") != request.n1_request_sha256
        or len(cpcv.get("paths", ())) != request.expected_ready_path_count
        or any(item.get("status") != "READY" for item in cpcv.get("paths", ()))
    ):
        _raise(
            "leg disagreement N1 CPCV source drift",
            "ADVISORY_N3_LEG_MVE_CPCV_INVALID",
        )
    regime_daily = _read_parquet(
        n1_path / "learnability_daily.parquet",
        "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        columns=["decision_as_of_trade_date", "regime"],
    )
    return {
        "parent": parent,
        "n2a": n2a,
        "n1": n1,
        "n2a_source": source,
        "parent_panel": parent_panel,
        "parent_daily": parent_daily,
        "cpcv": cpcv,
        "regime_daily": regime_daily,
    }


def _validate_request_source_identities(
    *,
    request: FrozenLegDisagreementRequestV1,
    parent: Mapping[str, Any],
    n2a: Mapping[str, Any],
    n1: Mapping[str, Any],
) -> None:
    if (
        Path(request.parent_overlay_bundle_path).resolve().name != request.parent_overlay_bundle_id
        or parent["request"].request_sha256 != request.parent_overlay_request_sha256
        or parent["receipt"].receipt_sha256 != request.parent_overlay_receipt_sha256
        or Path(request.n2a_bundle_path).resolve().name != request.n2a_bundle_id
        or n2a["request"].request_sha256 != request.n2a_request_sha256
        or n2a["receipt"].receipt_sha256 != request.n2a_receipt_sha256
        or Path(request.n1_bundle_path).resolve().name != request.n1_bundle_id
        or n1["request"].request_sha256 != request.n1_request_sha256
        or n1["request"].split_policy_sha256 != request.n1_split_policy_sha256
        or n2a["record"].dataset_identity != request.source_dataset_identity
        or parent["record"].dataset_identity != request.parent_dataset_identity
        or parent["record"].policy_identity != request.policy_identity
    ):
        _raise(
            "leg disagreement request/source relational identity drift",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        )


def _validate_parent_source_parity(
    *,
    features: pd.DataFrame,
    parent_panel: pd.DataFrame,
    request: FrozenLegDisagreementRequestV1,
) -> None:
    known = features.loc[
        features["outcome_known"].fillna(False).astype(bool),
        [
            "decision_as_of_trade_date",
            "instrument",
            "score__IC_WEIGHTED_PARENT",
            "economic_net_excess_bps",
        ],
    ].copy()
    parent = parent_panel.copy()
    for frame in (known, parent):
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str)
    known = known.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)
    parent = parent.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)
    known_keys = pd.MultiIndex.from_frame(known[["decision_as_of_trade_date", "instrument"]])
    parent_keys = pd.MultiIndex.from_frame(parent[["decision_as_of_trade_date", "instrument"]])
    exact = (
        len(known) == request.expected_known_row_count
        and len(parent) == request.expected_known_row_count
        and known_keys.equals(parent_keys)
        and parent["outcome_known"].fillna(False).astype(bool).all()
        and np.array_equal(
            known["score__IC_WEIGHTED_PARENT"].to_numpy(dtype=float),
            parent["score"].to_numpy(dtype=float),
        )
        and np.allclose(
            known["economic_net_excess_bps"].to_numpy(dtype=float),
            parent["economic_net_excess_bps"].to_numpy(dtype=float),
            rtol=0.0,
            atol=0.0,
            equal_nan=True,
        )
    )
    if not exact:
        _raise(
            "leg disagreement N2-A/current-parent source parity failed",
            "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED",
            n2a_known_rows=len(known),
            parent_rows=len(parent),
        )


def _validate_parent_daily_parity(*, daily: pd.DataFrame, parent_daily: pd.DataFrame) -> None:
    merged = daily.merge(
        parent_daily,
        on="decision_as_of_trade_date",
        how="outer",
        validate="one_to_one",
        suffixes=("_new", "_frozen"),
        indicator=True,
    )
    if not merged["_merge"].eq("both").all():
        _raise(
            "leg disagreement parent daily parity date coverage failed",
            "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED",
        )
    for column in ("parent_rank_ic", "parent_top5_churn"):
        if not np.allclose(
            merged[f"{column}_new"].to_numpy(dtype=float),
            merged[f"{column}_frozen"].to_numpy(dtype=float),
            rtol=0.0,
            atol=1e-12,
            equal_nan=True,
        ):
            _raise(
                "leg disagreement parent daily metric parity failed",
                "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED",
                metric=column,
            )
    evaluable_column = (
        "parent_top5_evaluable_new" if "parent_top5_evaluable_new" in merged.columns else "parent_top5_evaluable"
    )
    evaluable = merged[evaluable_column].fillna(False).astype(bool).to_numpy()
    current_top5 = merged["parent_top5_net_excess_bps_new"].to_numpy(dtype=float)
    frozen_top5 = merged["parent_top5_net_excess_bps_frozen"].to_numpy(dtype=float)
    if np.isfinite(current_top5[~evaluable]).any() or not np.allclose(
        current_top5[evaluable],
        frozen_top5[evaluable],
        rtol=0.0,
        atol=1e-12,
        equal_nan=False,
    ):
        _raise(
            "leg disagreement parent daily metric parity failed",
            "ADVISORY_N3_LEG_MVE_BASELINE_PARITY_FAILED",
            metric="parent_top5_net_excess_bps",
        )


def _publish_bundle(
    *,
    request: FrozenLegDisagreementRequestV1,
    features: pd.DataFrame,
    oof_scores: pd.DataFrame,
    fold_diagnostics: pd.DataFrame,
    daily_metrics: pd.DataFrame,
    model_summary: Mapping[str, Any],
    frontier: Mapping[str, Any],
    elapsed_seconds: float,
) -> Path:
    root = Path(request.output_root) / "leg_disagreement_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    _write_json(temporary / "request.json", request.model_dump(mode="json"))
    _write_json(
        temporary / "feature_schema.json",
        {
            "schema_version": LEG_MVE_FEATURE_SCHEMA_VERSION,
            "feature_schema_hash": LEG_MVE_FEATURE_SCHEMA_HASH,
            "comparator_features": list(request.model_trials[0].feature_columns),
            "expanded_features": list(request.model_trials[1].feature_columns),
            "rank_semantics": "SAME_DATE_CANONICAL_MEMBER_AVERAGE_PCT_ASCENDING",
            "sealed_holdout_accessed": False,
        },
    )
    features.to_parquet(temporary / "feature_panel.parquet", index=False)
    oof_scores.to_parquet(temporary / "oof_score_panel.parquet", index=False)
    fold_diagnostics.to_parquet(temporary / "fold_diagnostics.parquet", index=False)
    daily_metrics.to_parquet(temporary / "daily_metrics.parquet", index=False)
    _write_json(temporary / "model_summary.json", model_summary)
    _write_json(temporary / "frontier_receipt.json", frontier)
    source_payload = {
        "schema_version": "advisory_n3_leg_disagreement_source_identity_v1",
        "request_sha256": request.request_sha256,
        "evidence_refs": [item.model_dump(mode="json") for item in request.evidence_refs],
        "parent_overlay_bundle_id": request.parent_overlay_bundle_id,
        "n2a_bundle_id": request.n2a_bundle_id,
        "n1_bundle_id": request.n1_bundle_id,
        "source_dataset_identity": request.source_dataset_identity,
        "parent_dataset_identity": request.parent_dataset_identity,
        "dataset_identity": request.dataset_identity,
        "policy_identity": request.policy_identity,
        "source_row_count": int(len(features)),
        "known_row_count": int(features["outcome_known"].fillna(False).astype(bool).sum()),
        "evaluable_row_count": int(
            (
                features["outcome_known"].fillna(False).astype(bool)
                & np.isfinite(pd.to_numeric(features["economic_net_excess_bps"], errors="coerce"))
            ).sum()
        ),
        "nonfinite_known_row_count": int(
            (
                features["outcome_known"].fillna(False).astype(bool)
                & ~np.isfinite(pd.to_numeric(features["economic_net_excess_bps"], errors="coerce"))
            ).sum()
        ),
        "unknown_row_count": int((~features["outcome_known"].fillna(False).astype(bool)).sum()),
        "decision_date_count": int(features["decision_as_of_trade_date"].nunique()),
        "repository_commit": request.repository_commit,
        "database_read_performed": False,
        "network_read_performed": False,
        "qlib_read_performed": False,
        "minute_data_read_performed": False,
        "sealed_holdout_accessed": False,
    }
    _write_json(temporary / "source_identity_receipt.json", source_payload)
    temporary_bytes = sum(path.stat().st_size for path in temporary.iterdir() if path.is_file())
    if temporary_bytes > request.resource_max_temp_bytes:
        _raise(
            "leg disagreement temporary output exceeds frozen limit",
            "ADVISORY_N3_LEG_MVE_RESOURCE_LIMIT_EXCEEDED",
            temporary_bytes=temporary_bytes,
        )
    resource_payload = {
        "schema_version": "advisory_n3_leg_disagreement_resource_report_v1",
        "elapsed_seconds": float(elapsed_seconds),
        "peak_rss_bytes": _peak_rss_bytes(),
        "temporary_bytes": temporary_bytes,
        "resource_max_rss_bytes": request.resource_max_rss_bytes,
        "resource_max_temp_bytes": request.resource_max_temp_bytes,
        "wall_time_limit_seconds": None,
        "wall_time_is_telemetry_only": True,
    }
    _write_json(temporary / "resource_report.json", resource_payload)
    result_descriptors = {
        name: descriptor for name, descriptor in _file_descriptors(temporary).items() if name in RESULT_IDENTITY_MEMBERS
    }
    selected = model_summary.get("selected_trial_id")
    eligible = ("N3_LEG_DISAGREEMENT_EXPANDED_V1",) if selected else ()
    receipt = build_leg_disagreement_receipt(
        request_sha256=request.request_sha256,
        selected_trial_count=1 if selected else 0,
        selected_trial_id=selected,
        eligible_trial_ids=eligible,
        next_task=("N3_LEG_DISAGREEMENT_CONFIRMATION_DESIGN" if selected else "N3_MINUTE_INFORMATION_SET_MVE"),
        source_identity_sha256=sha256_file(temporary / "source_identity_receipt.json"),
        result_files_sha256=canonical_json_sha256(result_descriptors),
        resource_report_sha256=sha256_file(temporary / "resource_report.json"),
    )
    _write_json(temporary / "learnability_receipt.json", receipt.model_dump(mode="json"))
    bundle_id = canonical_json_sha256(
        {
            "schema_version": LEG_MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    destination = root / bundle_id
    record = _build_registry_record(
        request=request,
        receipt_path=temporary / "learnability_receipt.json",
        receipt_artifact_uri=(destination / "learnability_receipt.json").as_posix(),
        receipt=receipt,
    )
    _write_json(temporary / "registry_record.json", record.model_dump(mode="json"))
    descriptors = _file_descriptors(temporary)
    if set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "leg disagreement bundle member roster drift",
            "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
            members=sorted(descriptors),
        )
    manifest = {
        "schema_version": LEG_MVE_BUNDLE_SCHEMA,
        "bundle_id": bundle_id,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
        "parent_overlay_bundle_id": request.parent_overlay_bundle_id,
        "n2a_bundle_id": request.n2a_bundle_id,
        "n1_bundle_id": request.n1_bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.LEARNABILITY_AUDIT.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "result_class": ResearchResultClass.EXPLORATORY.value,
        "planned_trial_count": 2,
        "generated_trial_count": 2,
        "evaluated_trial_count": 2,
        "selected_trial_count": receipt.selected_trial_count,
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_eligible": False,
        "final_model_written": False,
        "factor_catalog_written": False,
        "strategy_package_written": False,
        "position_weight_output": False,
        "files": descriptors,
    }
    _write_json(temporary / "manifest.json", manifest)
    if destination.exists():
        _read_leg_bundle(destination)
        _raise(
            "leg disagreement bundle destination appeared concurrently",
            "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
            bundle_id=bundle_id,
        )
    temporary.replace(destination)
    _read_leg_bundle(destination)
    return destination


def _build_registry_record(
    *,
    request: FrozenLegDisagreementRequestV1,
    receipt_path: Path,
    receipt_artifact_uri: str,
    receipt: LegDisagreementReceiptV1,
) -> AdvisoryResearchTrialRecordV1:
    return build_trial_record(
        experiment_id=LEG_MVE_EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N3_ALPHA_INFORMATION_SET_EXPANSION_LEG_DISAGREEMENT",
        study_type=ResearchStudyType.LEARNABILITY_AUDIT,
        hypothesis_family_id=LEG_MVE_HYPOTHESIS_FAMILY_ID,
        parent_lineage=(
            "ADVISORY-N1-TIER1-LEARNABILITY",
            "ADVISORY-N2A-THREE-ARM-ALPHA-AUDIT",
            "ADVISORY-N3-QE-UPSTREAM-ALPHA-MVE-V1",
            "ADVISORY-N3-PARENT-INCREMENTAL-OVERLAY-V1",
        ),
        unique_variable="FIXED_LINEAR_LEGS_VS_FIXED_NONLINEAR_LEG_CONSENSUS_DISAGREEMENT",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity=request.dataset_identity,
        schema_identity=request.feature_schema_hash,
        policy_identity=request.policy_identity,
        planned_trial_count=2,
        generated_trial_count=2,
        evaluated_trial_count=2,
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
            evidence_reference_for_file(receipt_path, role="n3_leg_disagreement_learnability_receipt").model_copy(
                update={"artifact_uri": receipt_artifact_uri}
            ),
        ),
        recorded_at=datetime.now(timezone.utc),
    )


def _read_leg_bundle(path: Path) -> dict[str, Any]:
    manifest = _read_json(path / "manifest.json", "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID")
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict) or set(descriptors) != BUNDLE_MEMBERS:
        _raise(
            "leg disagreement bundle descriptor roster is invalid",
            "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
        )
    for name, descriptor in descriptors.items():
        if not isinstance(descriptor, dict):
            _raise(
                "leg disagreement bundle member descriptor is invalid",
                "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
                member=name,
            )
        member = path / name
        actual_rows = _parquet_row_count(member) if member.suffix == ".parquet" and member.is_file() else None
        if (
            not member.is_file()
            or sha256_file(member) != descriptor.get("sha256")
            or member.stat().st_size != descriptor.get("size_bytes")
            or (actual_rows is not None and actual_rows != descriptor.get("row_count"))
        ):
            _raise(
                "leg disagreement bundle member identity drift",
                "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
                member=name,
            )
    try:
        request = FrozenLegDisagreementRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = LegDisagreementReceiptV1.model_validate_json(
            (path / "learnability_receipt.json").read_text(encoding="utf-8")
        )
        record = AdvisoryResearchTrialRecordV1.model_validate_json(
            (path / "registry_record.json").read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "leg disagreement bundle contract member is invalid",
            "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": LEG_MVE_BUNDLE_SCHEMA,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    result_descriptors = {name: descriptors[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    frontier = _read_json(path / "frontier_receipt.json", "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID")
    frontier_functional = {key: value for key, value in frontier.items() if key != "frontier_sha256"}
    resource = _read_json(path / "resource_report.json", "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID")
    summary = _read_json(path / "model_summary.json", "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID")
    receipt_descriptor = descriptors["learnability_receipt.json"]
    invalid = (
        manifest.get("schema_version") != LEG_MVE_BUNDLE_SCHEMA
        or path.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or manifest.get("request_sha256") != request.request_sha256
        or manifest.get("receipt_sha256") != receipt.receipt_sha256
        or receipt.request_sha256 != request.request_sha256
        or receipt.source_identity_sha256 != descriptors["source_identity_receipt.json"]["sha256"]
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or receipt.resource_report_sha256 != descriptors["resource_report.json"]["sha256"]
        or record.experiment_id != LEG_MVE_EXPERIMENT_ID
        or record.attempt_id != request.request_id
        or record.study_type != ResearchStudyType.LEARNABILITY_AUDIT
        or record.decision_use != DecisionUse.NAVIGATION_ONLY
        or record.result_class != ResearchResultClass.EXPLORATORY
        or record.planned_trial_count != 2
        or record.generated_trial_count != 2
        or record.evaluated_trial_count != 2
        or record.selected_trial_count != receipt.selected_trial_count
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].role != "n3_leg_disagreement_learnability_receipt"
        or record.evidence_refs[0].sha256 != receipt_descriptor["sha256"]
        or record.evidence_refs[0].size_bytes != receipt_descriptor["size_bytes"]
        or frontier.get("frontier_sha256") != canonical_json_sha256(frontier_functional)
        or frontier.get("selected_trial_id") != receipt.selected_trial_id
        or tuple(frontier.get("eligible_trial_ids", ())) != receipt.eligible_trial_ids
        or summary.get("selected_trial_id") != receipt.selected_trial_id
        or manifest.get("parent_overlay_bundle_id") != request.parent_overlay_bundle_id
        or manifest.get("n2a_bundle_id") != request.n2a_bundle_id
        or manifest.get("n1_bundle_id") != request.n1_bundle_id
        or manifest.get("planned_trial_count") != 2
        or manifest.get("generated_trial_count") != 2
        or manifest.get("evaluated_trial_count") != 2
        or manifest.get("selected_trial_count") != receipt.selected_trial_count
        or manifest.get("objective_contract") != ObjectiveContract.ALPHA_RANKING.value
        or manifest.get("result_class") != ResearchResultClass.EXPLORATORY.value
        or manifest.get("study_type") != ResearchStudyType.LEARNABILITY_AUDIT.value
        or manifest.get("decision_use") != DecisionUse.NAVIGATION_ONLY.value
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("deployable") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("final_model_written") is not False
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
            "leg disagreement bundle relational identity is invalid",
            "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
        )
    return {"manifest": manifest, "request": request, "receipt": receipt, "record": record}


def _find_existing_bundle(request: FrozenLegDisagreementRequestV1) -> Path | None:
    root = Path(request.output_root) / "leg_disagreement_bundles"
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
            "one leg disagreement request maps to multiple bundles",
            "ADVISORY_N3_LEG_MVE_BUNDLE_INVALID",
        )
    if matches:
        _read_leg_bundle(matches[0])
        return matches[0]
    return None


def _deliver_bundle(*, request: FrozenLegDisagreementRequestV1, bundle_path: Path) -> dict[str, Any]:
    loaded = _read_leg_bundle(bundle_path)
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
    request: FrozenLegDisagreementRequestV1,
    receipt: LegDisagreementReceiptV1,
    bundle_id: str,
    registry_sha256: str,
) -> dict[str, Any]:
    selected = receipt.selected_trial_id or "NONE"
    content = "\n".join(
        (
            "# Advisory 当前研究路线",
            "",
            "- active_main_line: `N3_LEG_DISAGREEMENT_INFORMATION_SET_MVE`",
            "- active_auxiliary_line: `NONE`",
            f"- next_task: `{receipt.next_task}`",
            f"- exploratory_candidate: `{selected}`",
            f"- parent_overlay_bundle_id: `{request.parent_overlay_bundle_id}`",
            f"- leg_disagreement_bundle_id: `{bundle_id}`",
            f"- trial_registry_sha256: `{registry_sha256}`",
            "- objective_contract: `ALPHA_RANKING`",
            "- study_type: `LEARNABILITY_AUDIT`",
            "- decision_use: `NAVIGATION_ONLY`",
            "- sealed_holdout_accessed: `false`",
            "- deployable/runtime/model/factor/strategy_package/position_weight: `false/false/false/false/false/false`",
            "",
            "该页面只记录开发窗口learnability导航，不构成确认、激活、资金仓位或交易输入。",
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


def _verify_environment(request: FrozenLegDisagreementRequestV1) -> None:
    repo = Path(request.repository_root)
    if _cross_os_git_commit(repo) != request.repository_commit:
        _raise(
            "leg disagreement repository commit drift",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
        )
    dirty = _cross_os_git_dirty_paths(repo)
    if dirty:
        _raise(
            "leg disagreement repository became dirty",
            "ADVISORY_N3_LEG_MVE_SOURCE_IDENTITY_MISMATCH",
            dirty_paths=dirty[:20],
        )
    for reference in request.evidence_refs:
        _verify_ref(reference)
    parent = _read_overlay_bundle(Path(request.parent_overlay_bundle_path))
    n2a = _read_n2a_bundle(Path(request.n2a_bundle_path))
    n1 = _read_n1_bundle(Path(request.n1_bundle_path))
    _validate_bound_sources(
        parent_path=Path(request.parent_overlay_bundle_path),
        parent=parent,
        n2a_path=Path(request.n2a_bundle_path),
        n2a=n2a,
        n1_path=Path(request.n1_bundle_path),
        n1=n1,
    )
    _validate_request_source_identities(request=request, parent=parent, n2a=n2a, n1=n1)


def _run_response(
    request: FrozenLegDisagreementRequestV1,
    bundle: Path,
    delivery: Mapping[str, Any],
    *,
    exact_retry: bool,
) -> dict[str, Any]:
    return {
        **inspect_leg_disagreement_bundle(bundle),
        "request_id": request.request_id,
        "bundle_path": bundle.as_posix(),
        "exact_retry": exact_retry,
        "registry": dict(delivery["registry"]),
        "route": dict(delivery["route"]),
    }


def _metric_inference(
    values: Sequence[float] | pd.Series,
    *,
    request: FrozenLegDisagreementRequestV1,
    alpha: float,
    threshold: float,
    seed_offset: int,
) -> dict[str, Any]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if len(array) < 2:
        return {
            "point_estimate": float(array.mean()) if len(array) else None,
            "confidence_lower": None,
            "confidence_upper": None,
            "familywise_confidence_lower": None,
            "familywise_confidence_upper": None,
            "bootstrap_standard_error": None,
            "mde": None,
            "threshold": float(threshold),
            "observation_count": int(len(array)),
        }
    ordinary = _moving_block_interval(
        array,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + seed_offset,
        alpha=0.05,
    )
    familywise = _moving_block_interval(
        array,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + seed_offset,
        alpha=alpha,
    )
    rng = np.random.default_rng(request.bootstrap_seed + seed_offset)
    block = min(request.block_length_trading_days, len(array))
    blocks_needed = math.ceil(len(array) / block)
    starts = rng.integers(0, len(array), size=(request.bootstrap_repetitions, blocks_needed))
    offsets = np.arange(block)
    indexes = (starts[:, :, None] + offsets[None, None, :]) % len(array)
    samples = array[indexes.reshape(request.bootstrap_repetitions, -1)[:, : len(array)]]
    standard_error = float(samples.mean(axis=1).std(ddof=1))
    mde = float((1.959963984540054 + 0.8416212335729143) * standard_error)
    return {
        "point_estimate": float(array.mean()),
        "confidence_lower": ordinary[0],
        "confidence_upper": ordinary[1],
        "familywise_confidence_lower": familywise[0],
        "familywise_confidence_upper": familywise[1],
        "bootstrap_standard_error": standard_error,
        "mde": mde,
        "threshold": float(threshold),
        "observation_count": int(len(array)),
    }


def _top_ids(frame: pd.DataFrame, score_column: str) -> set[str]:
    ranked = frame.loc[:, ["instrument", score_column]].copy()
    ranked["instrument"] = ranked["instrument"].astype(str)
    ranked = ranked.sort_values([score_column, "instrument"], ascending=[False, True], kind="mergesort")
    if len(ranked) < 5:
        _raise(
            "leg disagreement daily panel has fewer than five rows",
            "ADVISORY_N3_LEG_MVE_OOF_INVALID",
        )
    return set(ranked.head(5)["instrument"])


def _top5_net_value(frame: pd.DataFrame, instruments: set[str]) -> float:
    top = frame.loc[frame["instrument"].astype(str).isin(instruments)]
    labels = pd.to_numeric(top["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    available = _top5_outcome_evaluable(frame, instruments)
    return float(labels.mean()) if available else float("nan")


def _top5_outcome_evaluable(frame: pd.DataFrame, instruments: set[str]) -> bool:
    top = frame.loc[frame["instrument"].astype(str).isin(instruments)]
    labels = pd.to_numeric(top["economic_net_excess_bps"], errors="coerce").to_numpy(dtype=float)
    return bool(len(top) == 5 and top["outcome_known"].fillna(False).astype(bool).all() and np.isfinite(labels).all())


def _mean(values: pd.Series) -> float | None:
    array = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float)
    array = array[np.isfinite(array)]
    return float(array.mean()) if len(array) else None


def _mean_by_day_score_correlation(frame: pd.DataFrame, left: str, right: str) -> float | None:
    values = [
        _safe_correlation(group[left], group[right], method="spearman")
        for _, group in frame.groupby("decision_as_of_trade_date", sort=True)
    ]
    finite = np.asarray([value for value in values if value is not None], dtype=float)
    return float(finite.mean()) if len(finite) else None


def _check_resource_limits(request: FrozenLegDisagreementRequestV1, stage: str) -> None:
    rss = _peak_rss_bytes()
    if rss > request.resource_max_rss_bytes:
        _raise(
            "leg disagreement resident memory exceeds frozen limit",
            "ADVISORY_N3_LEG_MVE_RESOURCE_LIMIT_EXCEEDED",
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
            "leg disagreement origin/main commit cannot be read",
            "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise(
            "leg disagreement origin/main commit is invalid",
            "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
            origin_main_commit=commit,
        )
    return commit


def _write_immutable_request(path: Path, request: FrozenLegDisagreementRequestV1) -> None:
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
            existing = FrozenLegDisagreementRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "existing leg disagreement request is invalid",
                "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 != request.request_sha256 or path.read_bytes() != encoded:
            _raise(
                "leg disagreement request path already contains different content",
                "ADVISORY_N3_LEG_MVE_REQUEST_INVALID",
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


def _read_parquet(
    path: Path,
    reason_code: str,
    *,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=list(columns) if columns is not None else None)
    except Exception as exc:
        _raise(
            "leg disagreement parquet cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )


def _read_json(path: Path, reason_code: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "leg disagreement JSON cannot be read",
            reason_code,
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(payload, dict):
        _raise(
            "leg disagreement JSON root is not an object",
            reason_code,
            path=path.as_posix(),
        )
    return payload


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _resolve_bound_path(value: str | Path) -> Path:
    text = str(value).replace("\\", "/")
    if os.name == "nt" and text.startswith("/mnt/") and len(text) > 6 and text[5].isalpha() and text[6] == "/":
        return Path(f"{text[5].upper()}:/{text[7:]}").resolve()
    if os.name != "nt" and len(text) > 2 and text[0].isalpha() and text[1:3] == ":/":
        return Path(f"/mnt/{text[0].lower()}/{text[3:]}").resolve()
    return Path(value).resolve()


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "build_leg_feature_panel",
    "evaluate_leg_models",
    "inspect_leg_disagreement_bundle",
    "prepare_leg_disagreement_request",
    "run_leg_crossfit",
    "run_leg_disagreement_mve",
]
