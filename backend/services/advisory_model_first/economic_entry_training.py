"""Fixed lightweight entry-value learner; labels never become D features."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_entry_contracts import (
    EconomicEntryLabelV1,
    EconomicEntryTrainingRequestV1,
)
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _fail, _frame


@dataclass(frozen=True)
class EconomicEntryTrainingResult:
    return_model: Any
    risk_model: Any
    request: EconomicEntryTrainingRequestV1
    feature_bounds: dict[str, tuple[float, float]]
    price_support: dict[int, dict[str, float | int]]
    diagnostics: dict[str, Any]
    split_receipt: pd.DataFrame


def prepare_economic_training_rows(
    *, features: pd.DataFrame, labels: tuple[EconomicEntryLabelV1, ...], request: EconomicEntryTrainingRequestV1,
) -> pd.DataFrame:
    request = EconomicEntryTrainingRequestV1.model_validate(request.model_dump())
    if max(len(features), len(labels)) > request.resource_max_rows:
        _fail("economic training exceeds its registered row resource budget")
    labels = tuple(EconomicEntryLabelV1.model_validate(label.model_dump()) for label in labels)
    required_features = set(request.feature_names) - {"query_gap_bps"}
    required_columns = set(KEY) | required_features | {
        "feature_visible_through", "feature_source_sha256",
    }
    if not required_columns.issubset(features.columns):
        _fail("economic training is missing required frozen feature/source columns")
    frame = _frame(features.loc[:, sorted(required_columns)], KEY, required_columns)
    if not frame["feature_source_sha256"].eq(request.feature_source_sha256).all():
        _fail("training feature source identity mismatch")
    visible = frame["feature_visible_through"].map(_day)
    if (visible > frame[KEY[0]]).any():
        _fail("economic training feature exceeds D cutoff", "ADVISORY_ECONOMIC_FEATURE_PIT")
    if not labels or any(label.input_identity_sha256 != request.input_identity_sha256 for label in labels):
        _fail("economic training label identity mismatch or empty roster")
    label_rows = pd.DataFrame([{
        KEY[0]: pd.Timestamp(label.decision_date), KEY[1]: pd.Timestamp(label.target_date),
        "instrument": label.instrument, "status": label.status,
        "label_information_end": pd.Timestamp(label.label_information_end),
        "query_gap_bps": label.actual_gap_bps, "return_target_bps": label.entry_advantage_bps,
        "risk_target_bps": label.daily_mark_max_drawdown_bps,
    } for label in labels])
    if label_rows.duplicated(KEY).any():
        _fail("economic labels have duplicate candidate keys")
    # The query condition comes from real label observations, not a caller's T feature.
    frame = frame.drop(columns=["query_gap_bps"], errors="ignore")
    # Complexity audit: one-to-one D/T/symbol join cannot multiply candidates.
    # Inputs are capped by the registered row budget (first study 7,720 rows;
    # default cap 100,000), narrowed to 8 D features plus identity/clock columns.
    # Model matrix is O(rows * 9) float64, at most 7.2 MB at the default cap;
    # no universe-by-date Cartesian product, no test-dependent transformation.
    merged = frame.merge(label_rows, on=KEY, how="outer", validate="one_to_one", indicator=True)
    if not merged["_merge"].eq("both").all():
        _fail("economic features and labels must preserve the exact same candidate roster")
    if not merged["label_information_end"].le(pd.Timestamp(request.label_cutoff)).all():
        # A known censored boundary may extend beyond the cutoff but cannot be trained.
        illegal = merged["status"].eq("AVAILABLE") & merged["label_information_end"].gt(pd.Timestamp(request.label_cutoff))
        if illegal.any():
            _fail("available label uses outcomes beyond the study cutoff")
    merged["split"] = "OUTSIDE_STUDY"
    decision = merged[KEY[0]]
    for name, start, end in (
        ("train", request.train_start, request.train_end),
        ("validation", request.validation_start, request.validation_end),
        ("test", request.test_start, request.test_end),
    ):
        selected = decision.between(pd.Timestamp(start), pd.Timestamp(end))
        merged.loc[selected, "split"] = name
        if name != "test":
            purged = selected & merged["label_information_end"].gt(pd.Timestamp(end))
            merged.loc[purged, "split"] = "PURGED_LABEL_END"
    matrix = merged.loc[:, request.feature_names].apply(pd.to_numeric, errors="coerce")
    finite = np.isfinite(matrix.to_numpy(dtype=float)).all(axis=1)
    merged["training_eligible"] = merged["status"].eq("AVAILABLE") & finite
    merged["feature_values_available"] = finite
    # Neither future label-status nor path values are part of this whitelist.
    merged.loc[:, request.feature_names] = matrix
    return merged.drop(columns="_merge")


def train_economic_entry_model(
    *, features: pd.DataFrame, labels: tuple[EconomicEntryLabelV1, ...], request: EconomicEntryTrainingRequestV1,
) -> EconomicEntryTrainingResult:
    import lightgbm as lgb

    rows = prepare_economic_training_rows(features=features, labels=labels, request=request)
    train = rows.loc[rows["split"].eq("train") & rows["training_eligible"]]
    validation = rows.loc[rows["split"].eq("validation") & rows["training_eligible"]]
    if len(train) < request.minimum_bin_observations or validation.empty:
        _fail("economic training lacks mature past-only train/validation observations",
              "ADVISORY_ECONOMIC_TRAINING_INSUFFICIENT")
    common = dict(request.effective_parameters)
    rounds = common.pop("num_boost_round")
    return_objective = common.pop("return_objective")
    risk_objective, risk_alpha = common.pop("risk_objective"), common.pop("risk_alpha")
    matrix = train.loc[:, request.feature_names].astype(float)
    return_model = lgb.train(
        {**common, "objective": return_objective},
        lgb.Dataset(matrix, label=train["return_target_bps"].astype(float)), num_boost_round=rounds,
    )
    risk_model = lgb.train(
        {**common, "objective": risk_objective, "alpha": risk_alpha},
        lgb.Dataset(matrix, label=train["risk_target_bps"].astype(float)), num_boost_round=rounds,
    )
    bounds = {name: (float(matrix[name].min()), float(matrix[name].max())) for name in request.feature_names}
    support: dict[int, dict[str, float | int]] = {}
    bins = np.floor(train["query_gap_bps"] / request.gap_bin_width_bps).astype(int)
    for bin_id, group in train.groupby(bins, sort=True):
        day_count = group[KEY[0]].nunique()
        if len(group) >= request.minimum_bin_observations and day_count >= request.minimum_bin_days:
            support[int(bin_id)] = {"observation_count": len(group), "decision_day_count": day_count,
                                    "observed_min_gap_bps": float(group["query_gap_bps"].min()),
                                    "observed_max_gap_bps": float(group["query_gap_bps"].max())}
    validation_matrix = validation.loc[:, request.feature_names].astype(float)
    predicted_return = np.asarray(return_model.predict(validation_matrix, num_threads=2), dtype=float)
    predicted_risk = np.asarray(risk_model.predict(validation_matrix, num_threads=2), dtype=float)
    if not np.isfinite(predicted_return).all() or not np.isfinite(predicted_risk).all() or (predicted_risk < 0).any():
        _fail("validation model returned non-finite or invalid risk estimates")
    absolute_error = np.abs(validation["return_target_bps"].to_numpy() - predicted_return)
    diagnostics = {
        "train_rows": len(train), "validation_rows": len(validation),
        "validation_return_abs_error_p90_bps": float(np.quantile(absolute_error, .9)),
        "uncertainty_semantics": "validation_absolute_error_diagnostic_not_mean_confidence_bound",
        "validation_risk_q90_coverage": float(np.mean(validation["risk_target_bps"].to_numpy() <= predicted_risk)),
        "supported_price_bin_count": len(support),
        "purged_rows": int(rows["split"].eq("PURGED_LABEL_END").sum()),
        "test_used_for_training_or_calibration": False, "economic_effectiveness": "NOT_EVALUATED",
    }
    return EconomicEntryTrainingResult(
        return_model, risk_model, request, bounds, support, diagnostics,
        rows.loc[:, KEY + ["split", "status", "training_eligible", "label_information_end"]].copy(),
    )
