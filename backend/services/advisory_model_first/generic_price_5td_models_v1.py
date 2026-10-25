"""Fixed GP5/matched four fits and immutable non-executing model identity."""
from dataclasses import asdict, dataclass

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_price_campaign_models_v2 import export_gbdt_v2, predict_json_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import (
    ARMS, FEATURES, KEY, PARAMETERS, POLICY, POLICY_SHA256, STOCK_FEATURES, GenericPrice5TDConfigurationV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class GenericPrice5TDFitV1:
    recipe: dict
    models: dict
    intervals_bps: tuple
    diagnostics: dict
    model_sha256: str


def fitted_identity(fitted):
    return sha({name: value for name, value in asdict(fitted).items() if name != "model_sha256"})


def feature_values(features):
    if (not isinstance(features, pd.DataFrame) or len(features) > 500000
            or not features.columns.is_unique or not set(FEATURES).issubset(features.columns)):
        raise ValueError("GP5 query feature schema/row budget differs")
    values = features.loc[:, FEATURES].map(_number).to_numpy(dtype=float)
    available = features.loc[:, STOCK_FEATURES].map(_number).notna().any(axis=1).to_numpy()
    return values, available


def matrix(features, medians, *, gaps=None):
    values, _ = feature_values(features)
    medians = np.asarray(medians, dtype=float)
    if medians.shape != (9,) or not np.isfinite(medians).all():
        raise ValueError("GP5 train-only missing encoding differs")
    missing = np.isnan(values)
    encoded = np.column_stack((np.where(missing, medians, values), missing.astype(float)))
    if gaps is not None:
        gaps = np.asarray([_number(v) for v in gaps], dtype=float)
        if gaps.shape != (len(values),) or not np.isfinite(gaps).all() or (gaps <= -10000).any():
            raise ValueError("GP5 scenario price coordinates differ")
        encoded = np.column_stack((encoded, gaps/100))
    return encoded


def _support(domain):
    finite = domain.observed_gap_bps.notna()
    data = domain.loc[finite, [KEY[0], "observed_gap_bps"]].copy()
    if data.empty:
        return ValueAnchorGapSupportV1(())
    gaps = data.observed_gap_bps.map(_number).astype(float)
    if gaps.isna().any() or gaps.le(-10000).any():
        raise ValueError("GP5 observed prices differ")
    lo, hi = np.quantile(gaps, [.025, .975])
    data["bucket"] = np.floor(gaps/100).astype(int)
    intervals = []
    for bucket, group in data.groupby("bucket", sort=True):
        a, b = max(lo, bucket*100), min(hi, (bucket+1)*100)
        # Upper bucket edge is exclusive; preserve holes and floating precision.
        b = min(b, np.nextafter(float((bucket+1)*100), -np.inf))
        if len(group) >= 30 and group[KEY[0]].nunique() >= 5 and a <= b:
            intervals.append((float(a), float(b)))
    return ValueAnchorGapSupportV1(tuple(intervals))


def train_generic_price_5td_v1(*, rows, configuration, before_fit):
    from sklearn.ensemble import GradientBoostingRegressor
    config = GenericPrice5TDConfigurationV1.model_validate(configuration)
    required = {*KEY, *FEATURES, "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio",
                "label_status", "label_information_end", "policy_sha256", "label_contract"}
    if (not isinstance(rows, pd.DataFrame) or len(rows) > 7720 or not rows.columns.is_unique
            or not required.issubset(rows.columns) or rows.duplicated(list(KEY)).any()
            or not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()):
        raise ValueError("GP5 complete rows or independent label policy differs")
    data = rows.copy(deep=True)
    for name in KEY[:2]:
        data[name] = pd.to_datetime(data[name])
    start, end = pd.Timestamp(config.train_start), pd.Timestamp(config.train_end)
    _, available = feature_values(data)
    domain = data.loc[data[KEY[0]].between(start, end) & data[KEY[1]].le(end) & available].copy()
    values, _ = feature_values(domain)
    medians = [float(np.median(column[~np.isnan(column)])) if (~np.isnan(column)).any() else 0.
               for column in values.T]
    support = _support(domain)
    maturity = pd.to_datetime(domain.label_information_end)
    if domain.label_status.eq("AVAILABLE").any() and maturity[domain.label_status.eq("AVAILABLE")].isna().any():
        raise ValueError("GP5 available label has unknown maturity")
    mask = domain.label_status.eq("AVAILABLE") & maturity.le(end)
    mask &= domain.observed_gap_bps.map(lambda v: False if pd.isna(v) else support.contains(float(v)))
    train = domain.loc[mask].sort_values(list(KEY))
    if len(train) < 100 or train[KEY[0]].nunique() < 20:
        raise ValueError("GP5 lacks mature shared supervision; no physical fits")
    targets = train.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda v: _number(v, positive=True))
    if targets.isna().any().any():
        raise ValueError("GP5 AVAILABLE labels contain unknown values")
    models = {}
    for arm in ARMS:
        x = matrix(train, medians, gaps=train.observed_gap_bps if arm == "candidate" else None)
        for name, target, quantile in (("mean", targets.gross_terminal_ratio, False),
                                       ("path", targets.path_min_ratio, True)):
            head = arm+"_"+name
            before_fit(head)
            estimator = GradientBoostingRegressor(**PARAMETERS, loss="quantile" if quantile else "squared_error", alpha=.1)
            estimator.fit(x, target)
            body = export_gbdt_v2(estimator)
            if not np.allclose(predict_json_v2(body, x), estimator.predict(x), rtol=1e-10, atol=1e-10):
                raise ValueError("GP5 public estimator/JSON parity failed")
            models[head] = body
    recipe = dict(features=list(FEATURES), missing_encoding="TRAIN_MEDIAN_PLUS_FLAGS",
                  medians=medians, policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"],
                  parameters=PARAMETERS, configuration=config.model_dump(mode="json"))
    diagnostics = dict(train_rows=len(train), train_days=int(train[KEY[0]].nunique()), physical_fit_count=4,
                       training_keys_sha256=sha([[str(v) for v in row] for row in train.loc[:, KEY].itertuples(index=False, name=None)]),
                       test_used_for_training_or_calibration=False, deployable=False)
    validation = data.loc[data[KEY[0]].between(pd.Timestamp(config.validation_start), pd.Timestamp(config.validation_end))
                          & data.label_status.eq("AVAILABLE")
                          & pd.to_datetime(data.label_information_end).le(pd.Timestamp(config.validation_end))]
    validation = validation.loc[validation.observed_gap_bps.map(
        lambda value: False if pd.isna(value) else support.contains(float(value)))]
    diagnostics["validation_diagnostics_only"] = {}
    for arm in ARMS:
        if validation.empty:
            diagnostics["validation_diagnostics_only"][arm] = {"supported_rows": 0, "used_for_selection": False}
            continue
        x = matrix(validation, medians, gaps=validation.observed_gap_bps if arm == "candidate" else None)
        mean, lower = (predict_json_v2(models[arm+"_"+head], x) for head in ("mean", "path"))
        observed = validation.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(
            lambda value: _number(value, positive=True)).to_numpy(dtype=float)
        if not np.isfinite(observed).all():
            raise ValueError("GP5 AVAILABLE validation labels are invalid")
        error = observed[:, 1]-lower
        diagnostics["validation_diagnostics_only"][arm] = dict(supported_rows=len(validation), used_for_selection=False,
            mean_squared_error=float(np.mean((mean-observed[:, 0])**2)),
            path_pinball_loss=float(np.mean(np.maximum(.1*error, -.9*error))),
            path_lower_coverage=float(np.mean(observed[:, 1] < lower)))
    fitted = GenericPrice5TDFitV1(recipe, models, support.intervals_bps, diagnostics, "")
    return GenericPrice5TDFitV1(recipe, models, support.intervals_bps, diagnostics, fitted_identity(fitted))


def validate_fit(fitted):
    if (not isinstance(fitted, GenericPrice5TDFitV1) or fitted_identity(fitted) != fitted.model_sha256
            or fitted.recipe.get("features") != list(FEATURES)
            or fitted.recipe.get("policy_sha256") != POLICY_SHA256
            or fitted.recipe.get("label_contract") != POLICY["label_contract"]
            or fitted.recipe.get("parameters") != PARAMETERS
            or set(fitted.models) != {arm+"_"+head for arm in ARMS for head in ("mean", "path")}):
        raise ValueError("GP5 model/schema/label/policy identity differs")
    return ValueAnchorGapSupportV1(fitted.intervals_bps)
