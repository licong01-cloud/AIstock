"""One frozen D-only two-head valuation fit; no QE dispatch or test selection."""
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, model_validator

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import KEY, _day, _frame
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorEstimateV1
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import build_value_anchor_gap_support_v1
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


class ValueAnchorStudyPlanV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["economic_value_anchor_study_v1"] = "economic_value_anchor_study_v1"
    hypothesis: Literal["H-VALUE-ANCHOR-1"] = "H-VALUE-ANCHOR-1"
    parent_plan_ref: EvidenceReferenceV1
    parent_prepared_manifest_ref: EvidenceReferenceV1
    feature_manifest_ref: EvidenceReferenceV1
    implementation_sha256: str
    study_type: Literal["EXPLORATORY_SCREEN"] = "EXPLORATORY_SCREEN"
    objective_contract: Literal["RISK_MANAGED_ADVISORY"] = "RISK_MANAGED_ADVISORY"
    decision_use: Literal["NAVIGATION_ONLY"] = "NAVIGATION_ONLY"
    deployable: Literal[False] = False

    @model_validator(mode="after")
    def validate_identity(self):
        if len(self.implementation_sha256) != 64 or any(char not in "0123456789abcdef" for char in self.implementation_sha256):
            raise ValueError("value implementation must be an exact hash")
        for ref, role in ((self.parent_plan_ref, "value_parent_plan"), (self.parent_prepared_manifest_ref, "value_prices_snapshot"),
                          (self.feature_manifest_ref, "value_d_snapshot")):
            if ref.role != role:
                raise ValueError("value source evidence role differs")
        return self

    @property
    def parameters(self):
        return {"feature_names": list(D_FEATURES), "learning_rate": .05, "max_depth": 3, "num_leaves": 7,
            "min_data_in_leaf": 30, "seed": 20261002, "num_threads": 2, "num_boost_round": 200,
            "mean_objective": "regression", "path_objective": "quantile", "path_alpha": .1,
            "lightgbm_version": "4.6.0", "gap_bin_bps": 100, "min_bin_observations": 30, "min_bin_days": 5,
            "gap_tail_quantiles": [.025, .975], "downside_reference_bps": 800,
            "configuration_count": 1, "head_count": 2, "candidate_count": 1}

    @property
    def plan_sha256(self):
        return sha({**self.model_dump(mode="json"), "parameters": self.parameters})

    @property
    def experiment_id(self):
        return "advvalue_"+self.plan_sha256[:24]


def assemble_value_anchor_rows_v1(*, inputs, labels, observations, configuration):
    fields = [*KEY, *D_FEATURES, "feature_visible_through", "daily_input_sha256"]
    feature = _frame(inputs.loc[:, fields], KEY, set(fields))
    visible = feature.feature_visible_through.map(_day)
    if not visible.eq(feature[KEY[0]]).all():
        raise ValueError("value model input must be D-visible")
    target = _frame(labels, KEY, set(KEY)|{"value_label_status", "gross_value_ratio", "path_min_value_ratio", "label_information_end"})
    quote = _frame(observations.loc[:, KEY+["actual_gap_bps"]], KEY, set(KEY)|{"actual_gap_bps"})
    joined = feature.merge(target.loc[:, KEY+["value_label_status", "gross_value_ratio", "path_min_value_ratio", "label_information_end"]],
        on=KEY, how="outer", validate="one_to_one", indicator=True)
    if not joined._merge.eq("both").all():
        raise ValueError("value model must retain all frozen candidate keys")
    joined = joined.drop(columns="_merge").merge(quote, on=KEY, how="outer", validate="one_to_one", indicator=True)
    if not joined._merge.eq("both").all():
        raise ValueError("value observations must retain all candidate keys")
    joined = joined.drop(columns="_merge")
    for name in (*D_FEATURES, "gross_value_ratio", "path_min_value_ratio", "actual_gap_bps"):
        if joined[name].map(lambda value: isinstance(value, (bool, np.bool_))).any():
            raise ValueError("value numeric inputs cannot be booleans")
        joined[name] = pd.to_numeric(joined[name], errors="coerce")
    joined["split"], joined["purged"] = "unused", False
    end = joined.label_information_end.map(_day)
    for split in ("train", "validation", "test"):
        left, right = (pd.Timestamp(getattr(configuration, f"{split}_{boundary}")) for boundary in ("start", "end"))
        in_split = joined[KEY[0]].between(left, right)
        joined.loc[in_split, "split"] = split
        joined.loc[in_split, "purged"] = end.loc[in_split].gt(right)
    joined["values_available"] = np.isfinite(joined.loc[:, D_FEATURES].to_numpy(dtype=float)).all(axis=1)
    numeric = joined.loc[:, ["gross_value_ratio", "path_min_value_ratio"]].to_numpy(dtype=float)
    available = joined.value_label_status.eq("AVAILABLE")
    if (available & (~np.isfinite(numeric).all(axis=1) | (numeric <= 0).any(axis=1))).any():
        raise ValueError("available value labels must be finite and positive")
    joined["training_eligible"] = available & joined.values_available & ~joined.purged
    return joined


@dataclass(frozen=True)
class ValueAnchorFitV1:
    mean_model: object
    path_model: object
    constant: ValueAnchorEstimateV1
    gap_support: object
    diagnostics: dict


def value_anchor_predict_v1(fitted, matrix, *, arm="model"):
    if tuple(matrix.columns) != tuple(D_FEATURES) or arm not in ("model", "constant"):
        raise ValueError("value forecast needs its exact D-only feature order/family")
    values = matrix.to_numpy(dtype=float)
    if values.ndim != 2 or not np.isfinite(values).all():
        raise ValueError("value forecast matrix must be complete finite D data")
    if arm == "constant":
        return [fitted.constant]*len(matrix)
    estimates = [np.asarray(model.predict(matrix, num_threads=2)) for model in (fitted.mean_model, fitted.path_model)]
    if any(value.shape != (len(matrix),) for value in estimates):
        raise ValueError("value model output dimensions differ")
    return [ValueAnchorEstimateV1(float(mean), float(lower)) for mean, lower in zip(*estimates, strict=True)]


def train_value_anchor_v1(*, rows, plan, configuration):
    import lightgbm as lgb
    if lgb.__version__ != plan.parameters["lightgbm_version"]:
        raise ValueError("value model runtime version differs")
    train = rows.loc[rows.split.eq("train") & rows.training_eligible]
    validation = rows.loc[rows.split.eq("validation") & rows.training_eligible]
    if len(train) < 30 or train[KEY[0]].nunique() < 5 or validation.empty:
        raise ValueError("value model lacks mature train/validation support")
    # Observation support and D bounds use no label/purge/maturity filtering.
    domain = rows.loc[rows.split.eq("train") & rows.values_available]
    observations = domain.copy()
    # The last train decision may target a validation-day open. It is not
    # observable at the train boundary and must not enlarge price support.
    cutoff = pd.Timestamp(configuration.train_end)
    observations.loc[observations[KEY[1]].gt(cutoff), "actual_gap_bps"] = np.nan
    support = build_value_anchor_gap_support_v1(observations)
    if not support.intervals_bps:
        raise ValueError("value model lacks train-only price support; no fit")
    bounds = {name: (float(domain[name].min()), float(domain[name].max())) for name in D_FEATURES}
    params = {name: plan.parameters[name] for name in ("learning_rate", "max_depth", "num_leaves", "min_data_in_leaf", "seed", "num_threads")}
    params.update(verbosity=-1, deterministic=True, force_col_wise=True)
    matrix = train.loc[:, D_FEATURES].astype(float)
    models = [lgb.train({**params, "objective": "regression"}, lgb.Dataset(matrix, label=train.gross_value_ratio), num_boost_round=200),
        lgb.train({**params, "objective": "quantile", "alpha": .1}, lgb.Dataset(matrix, label=train.path_min_value_ratio), num_boost_round=200)]
    constant = ValueAnchorEstimateV1(float(train.gross_value_ratio.mean()), float(train.path_min_value_ratio.quantile(.1, interpolation="linear")))
    fit = ValueAnchorFitV1(*models, constant, support, {})
    estimate = value_anchor_predict_v1(fit, validation.loc[:, D_FEATURES])
    mean, lower = np.array([(item.mean_gross_value_ratio, item.path_min_ratio_q10) for item in estimate]).T
    error = validation.path_min_value_ratio.to_numpy()-lower
    diagnostics = {"train_rows": len(train), "validation_rows": len(validation), "candidate_rows": len(rows),
        "purged_rows": int(rows.purged.sum()), "unknown_d_rows": int((~rows.values_available).sum()),
        "validation_mean_squared_error": float(np.mean((validation.gross_value_ratio.to_numpy()-mean)**2)),
        "validation_path_pinball_loss": float(np.mean(np.maximum(.1*error, -.9*error))),
        "validation_path_lower_coverage": float(np.mean(validation.path_min_value_ratio.to_numpy() < lower)),
        "training_d_feature_ranges_diagnostic_only": bounds,
        "fitted_head_count": 2, "model_configuration_count": 1, "candidate_count": 1,
        "test_used_for_training_or_calibration": False, "decision_use": "NAVIGATION_ONLY", "deployable": False}
    return ValueAnchorFitV1(*models, constant, support, diagnostics)
