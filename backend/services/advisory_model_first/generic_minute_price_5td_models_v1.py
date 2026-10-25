"""Four fixed fits: 19-dimensional price-conditioned control versus 35-dimensional D-minute."""
from dataclasses import dataclass
import time

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_price_campaign_models_v2 import export_gbdt_v2, predict_json_v2
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, MINUTE_FEATURES, PARAMETERS, POLICY, POLICY_SHA256, SCHEMA, SCHEMA_SHA256,
    GenericPrice5TDConfigurationV1, check_resource_budget_v1,
)
from backend.services.advisory_model_first.generic_price_5td_models_v1 import _support, feature_values
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class GenericMinutePrice5TDFitV1:
    recipe: dict
    models: dict
    intervals_bps: tuple
    diagnostics: dict
    model_sha256: str


def fitted_identity(fitted):
    # Validation diagnostics are not a fitted model input; the stage binds them separately.
    return sha(dict(recipe=fitted.recipe, models=fitted.models, intervals_bps=fitted.intervals_bps))


def values_v1(features):
    daily, available = feature_values(features)
    if not set(MINUTE_FEATURES).issubset(features.columns):
        raise ValueError("minute price query omits the declared minute schema")
    minute = features.loc[:, MINUTE_FEATURES].map(_number).to_numpy(dtype=float)
    return np.column_stack((daily, minute)), available


def matrix_v1(features, medians, *, gaps, arm):
    values, _ = values_v1(features)
    medians = np.asarray(medians, dtype=float)
    gaps = np.asarray([_number(value) for value in gaps], dtype=float)
    if (arm not in ARMS or medians.shape != (17,) or not np.isfinite(medians).all()
            or gaps.shape != (len(values),) or not np.isfinite(gaps).all() or (gaps <= -10000).any()):
        raise ValueError("minute price train-only encoding/arm/coordinates differ")
    missing = np.isnan(values)
    encoded = np.where(missing, medians, values)
    daily = np.column_stack((encoded[:, :9], missing[:, :9].astype(float), gaps/100))
    if arm == "matched":
        return daily
    return np.column_stack((daily, encoded[:, 9:], missing[:, 9:].astype(float)))


def train_generic_minute_price_5td_v1(*, rows, configuration, before_fit):
    from sklearn.ensemble import GradientBoostingRegressor
    started = time.monotonic()
    check_resource_budget_v1(started)
    config = GenericPrice5TDConfigurationV1.model_validate(configuration)
    required = {*KEY, *FEATURES, *MINUTE_FEATURES, "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio",
                "label_status", "label_information_end", "policy_sha256", "label_contract"}
    if (not isinstance(rows, pd.DataFrame) or len(rows) > 7720 or not rows.columns.is_unique
            or not required.issubset(rows.columns) or rows.duplicated(list(KEY)).any()
            or not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("minute price independent GP5 supervision identity differs")
    data = rows.copy(deep=True)
    for name in KEY[:2]:
        data[name] = pd.to_datetime(data[name])
    end = pd.Timestamp(config.train_end)
    domain = data.loc[data[KEY[0]].between(pd.Timestamp(config.train_start), end)
                      & data[KEY[1]].le(end)].copy()
    # Filter clocks before parsing any feature values. Test prices cannot affect even validation of training X.
    _, available = values_v1(domain)
    domain = domain.loc[available]
    values, _ = values_v1(domain)
    medians = [float(np.median(column[~np.isnan(column)])) if (~np.isnan(column)).any() else 0.
               for column in values.T]
    support = _support(domain)
    maturity = pd.to_datetime(domain.label_information_end)
    if maturity[domain.label_status.eq("AVAILABLE")].isna().any():
        raise ValueError("minute price AVAILABLE label has no maturity")
    mask = domain.label_status.eq("AVAILABLE") & maturity.le(end)
    mask &= domain.observed_gap_bps.map(lambda value: False if pd.isna(value) else support.contains(float(value)))
    train = domain.loc[mask].sort_values(list(KEY))
    if len(train) < 100 or train[KEY[0]].nunique() < 20:
        raise ValueError("minute price lacks mature shared supervision; no physical fits")
    targets = train.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda value: _number(value, positive=True))
    if targets.isna().any().any():
        raise ValueError("minute price AVAILABLE label has unknown values")
    models = {}
    for arm in ARMS:
        x = matrix_v1(train, medians, gaps=train.observed_gap_bps, arm=arm)
        for name, target, quantile in (("mean", targets.gross_terminal_ratio, False),
                                       ("path", targets.path_min_ratio, True)):
            check_resource_budget_v1(started)
            before_fit(arm+"_"+name)
            estimator = GradientBoostingRegressor(**PARAMETERS, loss="quantile" if quantile else "squared_error", alpha=.1)
            estimator.fit(x, target)
            check_resource_budget_v1(started)
            body = export_gbdt_v2(estimator)
            if not np.allclose(predict_json_v2(body, x), estimator.predict(x), rtol=1e-10, atol=1e-10):
                raise ValueError("minute price non-executing JSON parity failed")
            models[arm+"_"+name] = body
    diagnostics = dict(train_rows=len(train), train_days=int(train[KEY[0]].nunique()), physical_fit_count=4,
        training_keys_sha256=sha([[str(v) for v in row] for row in train.loc[:, KEY].itertuples(index=False, name=None)]),
        training_rows_with_minute_missing=int(train.loc[:, MINUTE_FEATURES].isna().any(axis=1).sum()),
        test_used_for_training_or_calibration=False, deployable=False, validation_diagnostics_only={})
    validation = data.loc[data[KEY[0]].between(pd.Timestamp(config.validation_start), pd.Timestamp(config.validation_end))]
    validation = validation.loc[validation.label_status.eq("AVAILABLE")
                                & pd.to_datetime(validation.label_information_end).le(pd.Timestamp(config.validation_end))]
    validation = validation.loc[validation.observed_gap_bps.map(
        lambda value: False if pd.isna(value) else support.contains(float(value)))]
    for arm in ARMS:
        metric = dict(supported_rows=len(validation), used_for_selection=False)
        if len(validation):
            x = matrix_v1(validation, medians, gaps=validation.observed_gap_bps, arm=arm)
            mean, lower = (predict_json_v2(models[arm+"_"+head], x) for head in ("mean", "path"))
            observed = validation.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(
                lambda value: _number(value, positive=True)).to_numpy(dtype=float)
            if not np.isfinite(observed).all():
                raise ValueError("minute price AVAILABLE validation target is invalid")
            error = observed[:, 1]-lower
            metric.update(mean_squared_error=float(np.mean((mean-observed[:, 0])**2)),
                path_pinball_loss=float(np.mean(np.maximum(.1*error, -.9*error))),
                path_lower_coverage=float(np.mean(observed[:, 1] < lower)))
        diagnostics["validation_diagnostics_only"][arm] = metric
    recipe = dict(schema=SCHEMA, schema_sha256=SCHEMA_SHA256, daily_features=list(FEATURES),
        minute_features=list(MINUTE_FEATURES), medians=medians, missing_encoding="TRAIN_MEDIAN_PLUS_FLAGS",
        candidate_dimensions=35, matched_dimensions=19, policy_sha256=POLICY_SHA256,
        label_contract=POLICY["label_contract"], parameters=PARAMETERS, configuration=config.model_dump(mode="json"))
    fitted = GenericMinutePrice5TDFitV1(recipe, models, support.intervals_bps, diagnostics, "")
    return GenericMinutePrice5TDFitV1(recipe, models, support.intervals_bps, diagnostics, fitted_identity(fitted))


def validate_fit_v1(fitted):
    if (not isinstance(fitted, GenericMinutePrice5TDFitV1) or fitted_identity(fitted) != fitted.model_sha256
            or fitted.recipe.get("schema_sha256") != SCHEMA_SHA256 or fitted.recipe.get("schema") != SCHEMA
            or fitted.recipe.get("daily_features") != list(FEATURES) or fitted.recipe.get("minute_features") != list(MINUTE_FEATURES)
            or fitted.recipe.get("policy_sha256") != POLICY_SHA256 or fitted.recipe.get("parameters") != PARAMETERS
            or fitted.recipe.get("label_contract") != POLICY["label_contract"]
            or fitted.recipe.get("candidate_dimensions") != 35 or fitted.recipe.get("matched_dimensions") != 19
            or set(fitted.models) != {arm+"_"+head for arm in ARMS for head in ("mean", "path")}):
        raise ValueError("minute price bundle/model/schema/policy identity differs")
    return ValueAnchorGapSupportV1(fitted.intervals_bps)
