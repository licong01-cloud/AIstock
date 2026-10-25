"""Two fixed GP5 heads, only new D-past lag information, frozen daily control."""
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_price_campaign_models_v2 import export_gbdt_v2, predict_json_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, validate_fit as validate_control
from backend.services.advisory_model_first.generic_return_volume_price_5td_contracts_v1 import (
    KEY, LAG_FEATURES, MODEL_FEATURES, PARAMETERS, POLICY, POLICY_SHA256, SCHEMA_SHA256,
    GenericPrice5TDConfigurationV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class GenericReturnVolumePrice5TDFitV1:
    recipe: dict
    models: dict
    intervals_bps: tuple
    diagnostics: dict
    model_sha256: str


def fitted_identity(fitted):
    return sha({k: v for k, v in asdict(fitted).items() if k != "model_sha256"})


def _values(features):
    _, available = feature_values(features)
    if not set(MODEL_FEATURES).issubset(features.columns):
        raise ValueError("return-volume price feature schema differs")
    return features.loc[:, MODEL_FEATURES].map(_number).to_numpy(dtype=float), available


def matrix_v1(features, medians, gaps):
    values, _ = _values(features)
    medians = np.asarray(medians, dtype=float)
    gaps = np.asarray([_number(v) for v in gaps], dtype=float)
    if (medians.shape != (12,) or not np.isfinite(medians).all() or gaps.shape != (len(features),)
            or not np.isfinite(gaps).all() or (gaps <= -10000).any()):
        raise ValueError("return-volume encoding or hypothetical prices differ")
    missing = np.isnan(values)
    return np.column_stack((np.where(missing, medians, values), missing.astype(float), gaps/100))


def train_return_volume_price_5td_v1(*, rows, frozen_control, configuration, before_fit):
    from sklearn.ensemble import GradientBoostingRegressor
    config = GenericPrice5TDConfigurationV1.model_validate(configuration)
    support = validate_control(frozen_control)
    required = {*KEY, *MODEL_FEATURES, "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio",
                "label_status", "label_information_end", "policy_sha256", "label_contract"}
    if (not isinstance(rows, pd.DataFrame) or len(rows) > 7720 or not rows.columns.is_unique
            or not required.issubset(rows.columns) or rows.duplicated(list(KEY)).any()
            or not rows.policy_sha256.eq(POLICY_SHA256).all() or not rows.label_contract.eq(POLICY["label_contract"]).all()
            or frozen_control.recipe["configuration"] != config.model_dump(mode="json")):
        raise ValueError("return-volume original rows/label policy/frozen configuration differ")
    data = rows.copy()
    for name in KEY[:2]:
        data[name] = data[name].map(_day).map(pd.Timestamp)
    start, end = pd.Timestamp(config.train_start), pd.Timestamp(config.train_end)
    domain = data.loc[data[KEY[0]].between(start, end) & data[KEY[1]].le(end)]
    _, available = feature_values(domain)
    domain = domain.loc[available].copy()
    new_values = domain.loc[:, LAG_FEATURES].map(_number).to_numpy(dtype=float)
    medians = [*frozen_control.recipe["medians"], *[float(np.median(c[~np.isnan(c)])) if (~np.isnan(c)).any() else 0.
                                                 for c in new_values.T]]
    maturity = domain.label_information_end.map(lambda v: _day(v, nullable=True)).map(pd.Timestamp)
    if maturity[domain.label_status.eq("AVAILABLE")].isna().any():
        raise ValueError("return-volume AVAILABLE label has no maturity")
    gaps = domain.observed_gap_bps.map(_number)
    mask = domain.label_status.eq("AVAILABLE") & maturity.le(end)
    mask &= gaps.map(lambda v: pd.notna(v) and support.contains(float(v)))
    train = domain.loc[mask].sort_values(list(KEY))
    keys_sha = sha([[str(v) for v in row] for row in train.loc[:, KEY].itertuples(index=False, name=None)])
    if (len(train) < 100 or train[KEY[0]].nunique() < 20
            or keys_sha != frozen_control.diagnostics["training_keys_sha256"]):
        raise ValueError("return-volume mature training KEYs differ from frozen control; no fits")
    target = train.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda v: _number(v, positive=True))
    if target.isna().any().any() or (target.path_min_ratio > target.gross_terminal_ratio+1e-6).any():
        raise ValueError("return-volume AVAILABLE paired targets contradict prices")
    x = matrix_v1(train, medians, train.observed_gap_bps)
    models = {}
    for head, values in (("mean", target.gross_terminal_ratio), ("path", target.path_min_ratio)):
        before_fit("candidate_"+head)
        estimator = GradientBoostingRegressor(**PARAMETERS, loss="quantile" if head == "path" else "squared_error", alpha=.1)
        estimator.fit(x, values)
        body = export_gbdt_v2(estimator)
        if not np.allclose(predict_json_v2(body, x), estimator.predict(x), rtol=1e-10, atol=1e-10):
            raise ValueError("return-volume JSON estimator parity failed")
        models[head] = body
    recipe = dict(features=list(MODEL_FEATURES), medians=medians, schema_sha256=SCHEMA_SHA256,
        policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"], parameters=PARAMETERS,
        configuration=config.model_dump(mode="json"), frozen_control_model_sha256=frozen_control.model_sha256)
    diagnostics = dict(train_rows=len(train), train_days=int(train[KEY[0]].nunique()), physical_fit_count=2,
        training_keys_sha256=keys_sha, test_used_for_training_or_calibration=False, deployable=False,
        lag_known_train_counts={k: int(train[k].notna().sum()) for k in LAG_FEATURES})
    validation = data.loc[data[KEY[0]].between(pd.Timestamp(config.validation_start), pd.Timestamp(config.validation_end))].copy()
    maturity = validation.label_information_end.map(lambda v: _day(v, nullable=True)).map(pd.Timestamp)
    selected = validation.label_status.eq("AVAILABLE") & maturity.le(pd.Timestamp(config.validation_end))
    selected &= validation.observed_gap_bps.map(lambda v: pd.notna(v) and support.contains(float(v)))
    validation = validation.loc[selected]
    diagnostics["validation_diagnostics_only"] = dict(supported_rows=len(validation), used_for_selection=False)
    if not validation.empty:
        _, known = feature_values(validation)
        value = validation.loc[known, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda v: _number(v, positive=True))
        if value.isna().any().any():
            raise ValueError("return-volume validation paired labels contain unknown values")
        xv = matrix_v1(validation.loc[known], medians, validation.loc[known, "observed_gap_bps"])
        if len(xv):
            mean, lower = (predict_json_v2(models[head], xv) for head in ("mean", "path"))
            error = value.path_min_ratio.to_numpy()-lower
            diagnostics["validation_diagnostics_only"].update(mean_squared_error=float(np.mean((mean-value.gross_terminal_ratio)**2)),
                path_pinball_loss=float(np.mean(np.maximum(.1*error, -.9*error))), path_lower_coverage=float(np.mean(error < 0)))
    # Diagnostics change identity, not model weights; no second fit.
    fitted = GenericReturnVolumePrice5TDFitV1(recipe, models, frozen_control.intervals_bps, diagnostics, "")
    return GenericReturnVolumePrice5TDFitV1(recipe, models, fitted.intervals_bps, diagnostics, fitted_identity(fitted))


def validate_fit(fitted):
    if (not isinstance(fitted, GenericReturnVolumePrice5TDFitV1) or fitted_identity(fitted) != fitted.model_sha256
            or fitted.recipe.get("features") != list(MODEL_FEATURES) or fitted.recipe.get("schema_sha256") != SCHEMA_SHA256
            or fitted.recipe.get("policy_sha256") != POLICY_SHA256 or fitted.recipe.get("parameters") != PARAMETERS
            or fitted.recipe.get("label_contract") != POLICY["label_contract"] or set(fitted.models) != {"mean", "path"}):
        raise ValueError("return-volume model/schema/policy identity differs")
    return ValueAnchorGapSupportV1(fitted.intervals_bps)


def query_return_volume_nodes_v1(*, fitted, features, scenario_gap_bps):
    support = validate_fit(fitted)
    values, available = _values(features)
    gaps = np.asarray([_number(v) for v in scenario_gap_bps], dtype=float)
    if not features.index.is_unique or gaps.shape != (len(features),) or (gaps[~np.isnan(gaps)] <= -10000).any():
        raise ValueError("return-volume query index or price coordinates differ")
    selected = available & np.array([False if np.isnan(g) else support.contains(float(g)) for g in gaps], dtype=bool)
    result = pd.DataFrame(dict(status=["UNKNOWN_INPUT_OR_SUPPORT"]*len(features),
        expected_net_bps=np.full(len(features), np.nan), downside_q90_bps=np.full(len(features), np.nan)), index=features.index)
    metadata = (*KEY, "selection_effective_rank", "candidate_group_size", "package_id", "run_id", "list_version_id",
                "universe_identity", "source_evidence", "return_volume_reason")
    for name in metadata:
        if name in features:
            result[name] = features[name].copy()
    result["input_unknown_fields"] = [tuple(k for k, v in zip(MODEL_FEATURES, row, strict=True) if np.isnan(v)) for row in values]
    result["model_sha256"], result["policy_sha256"] = fitted.model_sha256, POLICY_SHA256
    result["label_contract"], result["holding_period"] = POLICY["label_contract"], "T_OPEN_THROUGH_T_PLUS_4_CLOSE"
    if selected.any():
        x = matrix_v1(features.iloc[np.flatnonzero(selected)], fitted.recipe["medians"], gaps[selected])
        mean, lower = (predict_json_v2(fitted.models[head], x) for head in ("mean", "path"))
        if not np.isfinite([mean, lower]).all() or (mean <= 0).any() or (lower <= 0).any():
            raise ValueError("return-volume predicted price ratios contradict prices")
        entry = 1+gaps[selected]/10000
        net = 10000*(mean*(1-POLICY["sell_bps"]/10000)/(entry*(1+POLICY["buy_bps"]/10000))-1)
        risk = 10000*np.maximum(0, 1-lower/entry)
        if not np.isfinite([net, risk]).all():
            raise ValueError("return-volume net/risk arithmetic is nonfinite")
        result.loc[selected, "status"] = np.where((net > 0) & (risk <= POLICY["risk_bps"]), "ACCEPTABLE", "AVOID")
        result.loc[selected, "expected_net_bps"], result.loc[selected, "downside_q90_bps"] = net, risk
    return result


def return_volume_price_set_5td_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    numbers = [_number(v, positive=True) for v in (reference_cny, legal_low_cny, legal_high_cny, tick_cny)]
    if any(v is None for v in numbers) or set(d_features) != set(MODEL_FEATURES):
        raise ValueError("return-volume price set schema or legal coordinates differ")
    reference, low, high, tick = numbers
    if low > high:
        raise ValueError("return-volume legal price bounds differ")
    step = Decimal(str(tick))
    first = int((Decimal(str(low))/step).to_integral_value(rounding=ROUND_CEILING))
    last = int((Decimal(str(high))/step).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("return-volume full tick grid exceeds budget")
    prices = [float(step*number) for number in range(first, last+1)]
    gaps = [float((Decimal(str(p))/Decimal(str(reference))-1)*10000) for p in prices]
    nodes = query_return_volume_nodes_v1(fitted=fitted, features=pd.DataFrame([d_features]*len(prices), columns=MODEL_FEATURES), scenario_gap_bps=gaps)
    bands, a, b = [], None, None
    for price, state in zip(prices, nodes.status, strict=True):
        if state == "ACCEPTABLE":
            a, b = price if a is None else a, price
        elif a is not None:
            bands.append((a, b))
            a = b = None
    if a is not None:
        bands.append((a, b))
    unknown = int(nodes.status.eq("UNKNOWN_INPUT_OR_SUPPORT").sum())
    status = ("EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands
              else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if 0 < unknown < len(prices)
              else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(status=status, intervals_cny=tuple(bands), legal_node_count=len(prices), unknown_node_count=unknown,
        model_sha256=fitted.model_sha256, policy_sha256=POLICY_SHA256, holding_sessions=5, price_basis="D_ANCHORED_CNY",
        label_contract=POLICY["label_contract"], evidence_use="NAVIGATION_ONLY", deployable=False,
        valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL")
