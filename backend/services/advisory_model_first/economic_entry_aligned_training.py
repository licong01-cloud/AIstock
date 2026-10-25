"""One fixed refit of return and entry-loss heads on the same past-only rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryTrainingRequestV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail
from backend.services.advisory_model_first.economic_entry_training import prepare_economic_training_rows
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EntryLossLabelV2
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


@dataclass(frozen=True)
class AlignedEntryTrainingResultV3:
    return_model: Any
    risk_model: Any
    request: AlignedEntryTrainingRequestV3
    feature_bounds: dict[str, tuple[float, float]]
    price_support: dict[int, dict[str, float | int]]
    diagnostics: dict[str, Any]
    split_receipt: pd.DataFrame


def risk_labels_content_sha256(labels: tuple[EntryLossLabelV2, ...]) -> str:
    return canonical_json_sha256([value.model_dump(mode="json") for value in labels])


def common_fit_rows_sha256(rows: pd.DataFrame, feature_names: tuple[str, ...]) -> str:
    selected = rows.loc[rows.split.isin(["train", "validation"]) & rows.common_training_eligible,
                        KEY + ["split", "label_information_end", *feature_names, "return_target_bps", "entry_loss_target_bps"]]
    return canonical_json_sha256([
        {name: value.isoformat() if isinstance(value, pd.Timestamp) else value for name, value in row.items()}
        for row in selected.sort_values(KEY).to_dict("records")
    ])


def assemble_aligned_rows_v3(
    *, features: pd.DataFrame, labels: tuple[EntryLossLabelV2, ...], source_request: EconomicEntryTrainingRequestV1,
) -> pd.DataFrame:
    source_request = EconomicEntryTrainingRequestV1.model_validate(source_request.model_dump())
    labels = tuple(EntryLossLabelV2.model_validate(value.model_dump()) for value in labels)
    rows = prepare_economic_training_rows(features=features, labels=tuple(value.original for value in labels),
                                         request=source_request)
    by_key = {(pd.Timestamp(value.original.decision_date), pd.Timestamp(value.original.target_date), value.original.instrument): value
              for value in labels}
    if len(by_key) != len(labels):
        _fail("aligned risk labels contain duplicate candidate keys")
    ordered = [by_key[key] for key in rows[KEY].itertuples(index=False, name=None)]
    rows["risk_status_v2"] = [value.status for value in ordered]
    rows["risk_reason_v2"] = [value.reason_code for value in ordered]
    rows["entry_loss_target_bps"] = [value.entry_net_max_loss_bps for value in ordered]
    rows["common_training_eligible"] = rows.training_eligible & rows.risk_status_v2.eq("AVAILABLE")
    # Explicitly remove the original peak target; never train on it under a new name.
    rows = rows.drop(columns="risk_target_bps")
    return rows


def prepare_aligned_rows_v3(
    *, features: pd.DataFrame, labels: tuple[EntryLossLabelV2, ...], request: AlignedEntryTrainingRequestV3,
) -> pd.DataFrame:
    request = AlignedEntryTrainingRequestV3.model_validate(request.model_dump())
    if risk_labels_content_sha256(labels) != request.risk_labels_content_sha256:
        _fail("aligned training risk label content differs from registered inputs")
    rows = assemble_aligned_rows_v3(features=features, labels=labels, source_request=request.source_request)
    if common_fit_rows_sha256(rows, request.source_request.feature_names) != request.common_fit_rows_sha256:
        _fail("aligned common supervision matrix, targets or mask differ from registration")
    return rows


def train_aligned_entry_model_v3(
    *, features: pd.DataFrame, labels: tuple[EntryLossLabelV2, ...], request: AlignedEntryTrainingRequestV3,
) -> AlignedEntryTrainingResultV3:
    import lightgbm as lgb
    if lgb.__version__ != request.lightgbm_version:
        _fail("aligned fit runtime differs from registered LightGBM version")
    rows = prepare_aligned_rows_v3(features=features, labels=labels, request=request)
    configuration = request.source_request
    train = rows.loc[rows.split.eq("train") & rows.common_training_eligible]
    validation = rows.loc[rows.split.eq("validation") & rows.common_training_eligible]
    if len(train) < configuration.minimum_bin_observations or validation.empty:
        _fail("aligned candidate lacks mature past-only supervision")
    parameters = dict(configuration.effective_parameters)
    rounds = parameters.pop("num_boost_round")
    mean_objective = parameters.pop("return_objective")
    risk_objective, alpha = parameters.pop("risk_objective"), parameters.pop("risk_alpha")
    matrix = train.loc[:, configuration.feature_names].astype(float)
    return_model = lgb.train({**parameters, "objective": mean_objective},
                             lgb.Dataset(matrix, label=train.return_target_bps.astype(float)), num_boost_round=rounds)
    risk_model = lgb.train({**parameters, "objective": risk_objective, "alpha": alpha},
                           lgb.Dataset(matrix, label=train.entry_loss_target_bps.astype(float)), num_boost_round=rounds)
    validation_matrix = validation.loc[:, configuration.feature_names].astype(float)
    means = np.asarray(return_model.predict(validation_matrix, num_threads=2), dtype=float)
    risks = np.asarray(risk_model.predict(validation_matrix, num_threads=2), dtype=float)
    if (means.shape != (len(validation),) or risks.shape != (len(validation),)
            or not np.isfinite(means).all() or not np.isfinite(risks).all() or (risks < 0).any() or (risks > 10000).any()):
        _fail("aligned validation predictions are malformed")
    bounds = {name: (float(matrix[name].min()), float(matrix[name].max())) for name in configuration.feature_names}
    support = {}
    bins = np.floor(train.query_gap_bps / configuration.gap_bin_width_bps).astype(int)
    for bin_id, group in train.groupby(bins, sort=True):
        days = group[KEY[0]].nunique()
        if len(group) >= configuration.minimum_bin_observations and days >= configuration.minimum_bin_days:
            support[int(bin_id)] = {"observation_count": len(group), "decision_day_count": days,
                                    "observed_min_gap_bps": float(group.query_gap_bps.min()),
                                    "observed_max_gap_bps": float(group.query_gap_bps.max())}
    diagnostics = {
        "train_rows": len(train), "validation_rows": len(validation), "candidate_rows_preserved": len(rows),
        "new_return_heads": 1, "new_risk_heads": 1, "return_training_mode": request.return_training_mode,
        "risk_metric": request.risk_metric, "risk_quantile": request.risk_quantile,
        "validation_return_abs_error_p90_bps": float(np.quantile(np.abs(validation.return_target_bps.to_numpy() - means), .9)),
        "validation_entry_loss_q90_coverage": float(np.mean(validation.entry_loss_target_bps.to_numpy() <= risks)),
        "uncertainty_semantics": "validation_absolute_error_diagnostic_not_mean_confidence_bound",
        "purged_rows": int(rows.split.eq("PURGED_LABEL_END").sum()), "supported_price_bin_count": len(support),
        "test_used_for_training_or_calibration": False, "economic_effectiveness": "NOT_EVALUATED",
        "decision_use": "NAVIGATION_ONLY", "deployable": False,
    }
    return AlignedEntryTrainingResultV3(return_model, risk_model, request, bounds, support, diagnostics,
                                       rows.loc[:, KEY + ["split", "status", "risk_status_v2", "risk_reason_v2",
                                                         "common_training_eligible", "label_information_end"]].copy())
