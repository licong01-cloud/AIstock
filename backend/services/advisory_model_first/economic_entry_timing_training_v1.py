"""One common D recipe and common population for two fixed, new model fits."""
from dataclasses import dataclass
import re
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import _records
from backend.services.advisory_model_first.economic_entry_aligned_training import prepare_aligned_rows_v3
from backend.services.advisory_model_first.economic_entry_information_source import _numeric
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame
from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import ARMS, CANDIDATE_NAMES, TimingTrainingRequestV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

INPUT_COLUMNS = [*KEY, *CANDIDATE_NAMES[:-1], "feature_visible_through", "daily_input_sha256"]


def checked_timing_inputs(inputs):
    if (len(inputs) > 100000 or not inputs.columns.is_unique or set(inputs.columns) != set(INPUT_COLUMNS)):
        _fail("timing D input schema/budget differs")
    frame = _frame(inputs, KEY, set(INPUT_COLUMNS)).copy()
    visible = pd.to_datetime(frame.feature_visible_through, errors="coerce")
    if visible.dt.tz is not None or not visible.eq(frame[KEY[0]]).all() or not (frame[KEY[0]] < frame[KEY[1]]).all():
        _fail("timing D input clock differs")
    frame["feature_visible_through"] = visible
    if not frame.daily_input_sha256.map(lambda value: isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None).all():
        _fail("timing input lacks exact daily hash")
    for name in CANDIDATE_NAMES[:-1]:
        frame[name] = _numeric(frame[name])
    return frame


def timing_input_rows_sha256(inputs):
    return sha(_records(checked_timing_inputs(inputs).sort_values(KEY)))


def assemble_timing_rows_v1(*, original_features, labels, inputs, parent_request):
    # Validated immutable parent supplies label identity and purge only. Discard
    # its old values and eligible mask; the same new core supplies BOTH new fits.
    prior = prepare_aligned_rows_v3(features=original_features, labels=labels, request=parent_request)
    prior = prior.loc[:, [*KEY, "split", "status", "risk_status_v2", "label_information_end",
        "query_gap_bps", "return_target_bps", "entry_loss_target_bps"]]
    fresh = checked_timing_inputs(inputs)
    rows = prior.merge(fresh, on=KEY, how="outer", validate="one_to_one", indicator=True)
    if not rows._merge.eq("both").all():
        _fail("timing inputs must retain every original candidate key")
    rows["values_available"] = np.isfinite(rows.loc[:, CANDIDATE_NAMES].to_numpy(dtype=float)).all(axis=1)
    rows["timing_training_eligible"] = rows.status.eq("AVAILABLE") & rows.risk_status_v2.eq("AVAILABLE") & rows.values_available
    return rows.drop(columns="_merge")


def timing_common_fit_sha256(rows):
    eligible = rows.split.isin(("train", "validation")) & rows.timing_training_eligible
    return sha(_records(rows.loc[eligible, [*KEY, "split", "label_information_end", *CANDIDATE_NAMES,
        "return_target_bps", "entry_loss_target_bps"]].sort_values(KEY)))


@dataclass(frozen=True)
class TimingArmV1:
    request: TimingTrainingRequestV1
    arm: str
    feature_names: tuple[str, ...]
    return_model: Any
    risk_model: Any
    common_bounds: dict
    price_support: dict
    diagnostics: dict


@dataclass(frozen=True)
class TimingTrainingResultV1:
    arms: dict[str, TimingArmV1]
    split_receipt: pd.DataFrame
    diagnostics: dict


def train_timing_entry_v1(*, original_features, labels, inputs, request):
    import lightgbm as lgb
    request = TimingTrainingRequestV1.model_validate(request.model_dump())
    if request.parent_request.lightgbm_version != lgb.__version__ or timing_input_rows_sha256(inputs) != request.input_rows_sha256:
        _fail("timing runtime/input identity differs")
    rows = assemble_timing_rows_v1(original_features=original_features, labels=labels, inputs=inputs, parent_request=request.parent_request)
    if timing_common_fit_sha256(rows) != request.common_fit_rows_sha256:
        _fail("timing shared fit identity differs")
    configuration = request.parent_request.source_request
    train = rows.loc[rows.split.eq("train") & rows.timing_training_eligible]
    validation = rows.loc[rows.split.eq("validation") & rows.timing_training_eligible]
    if len(train) < configuration.minimum_bin_observations or validation.empty:
        _fail("timing lacks mature common train/validation support")
    support = {}
    bins = np.floor(train.query_gap_bps/configuration.gap_bin_width_bps).astype(int)
    for bin_id, group in train.groupby(bins, sort=True):
        count, days = len(group), group[KEY[0]].nunique()
        if count >= configuration.minimum_bin_observations and days >= configuration.minimum_bin_days:
            support[int(bin_id)] = {"observation_count": count, "decision_day_count": days,
                "observed_min_gap_bps": float(group.query_gap_bps.min()), "observed_max_gap_bps": float(group.query_gap_bps.max())}
    bounds = {name: (float(train[name].min()), float(train[name].max())) for name in CANDIDATE_NAMES}
    arms = {}
    for arm, names in ARMS.items():
        parameters = dict(configuration.effective_parameters)
        rounds, mean_objective = parameters.pop("num_boost_round"), parameters.pop("return_objective")
        risk_objective, alpha = parameters.pop("risk_objective"), parameters.pop("risk_alpha")
        matrix = train.loc[:, names].astype(float)
        mean = lgb.train({**parameters, "objective": mean_objective}, lgb.Dataset(matrix, label=train.return_target_bps.astype(float)), num_boost_round=rounds)
        risk = lgb.train({**parameters, "objective": risk_objective, "alpha": alpha}, lgb.Dataset(matrix, label=train.entry_loss_target_bps.astype(float)), num_boost_round=rounds)
        observed = validation.loc[:, names].astype(float)
        predicted, downside = np.asarray(mean.predict(observed, num_threads=2)), np.asarray(risk.predict(observed, num_threads=2))
        if (predicted.shape != (len(observed),) or downside.shape != (len(observed),) or not np.isfinite(predicted).all()
                or not np.isfinite(downside).all() or (downside < 0).any() or (downside > 10000).any()):
            _fail("timing validation heads returned invalid values")
        diagnostics = {"train_rows": len(train), "validation_rows": len(validation),
            "validation_return_abs_error_p90_bps": float(np.quantile(abs(validation.return_target_bps.to_numpy()-predicted), .9)),
            "validation_entry_loss_q90_coverage": float(np.mean(validation.entry_loss_target_bps.to_numpy() <= downside)),
            "common_fit_rows_sha256": request.common_fit_rows_sha256, "test_used_for_training_or_calibration": False}
        arms[arm] = TimingArmV1(request, arm, names, mean, risk, dict(bounds), dict(support), diagnostics)
    return TimingTrainingResultV1(arms,
        rows.loc[:, [*KEY, "split", "label_information_end", "values_available", "timing_training_eligible"]],
        {"model_configuration_count": 2, "fitted_head_count": 4, "economic_candidate_count": 1,
         "candidate_rows_preserved": len(rows), "unknown_rows": int((~rows.values_available).sum()),
         "deployable": False, "decision_use": "NAVIGATION_ONLY", "economic_effectiveness": "NOT_EVALUATED"})
