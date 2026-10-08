"""Frozen GP5 19-column prefix plus three D funding values and missing flags."""
from dataclasses import asdict, dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_price_campaign_models_v2 import export_gbdt_v2, predict_json_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_moneyflow_price_5td_contracts_v1 import (
    KEY, MATRIX_ORDER, MODEL_FEATURES, MONEYFLOW_FEATURES, PARAMETERS, POLICY,
    POLICY_SHA256, SCHEMA_SHA256, GenericPrice5TDConfigurationV1,
)
from backend.services.advisory_model_first.generic_moneyflow_price_5td_source_v1 import moneyflow_values
from backend.services.advisory_model_first.generic_price_5td_models_v1 import (
    feature_values, matrix as control_matrix, validate_fit as validate_control,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class GenericMoneyflowPrice5TDFitV1:
    recipe: dict
    models: dict
    intervals_bps: tuple
    diagnostics: dict
    model_sha256: str


def fitted_identity(fitted):
    return sha({name: value for name, value in asdict(fitted).items() if name != "model_sha256"})


def matrix_v1(features, *, base_medians, moneyflow_medians, gaps):
    gaps = np.asarray([_number(value) for value in gaps], dtype=float)
    medians = np.asarray(moneyflow_medians, dtype=float)
    if (not features.columns.is_unique or gaps.shape != (len(features),) or not np.isfinite(gaps).all()
            or (gaps <= -10000).any() or medians.shape != (3,) or not np.isfinite(medians).all()):
        raise ValueError("moneyflow encoding or hypothetical prices differ")
    prefix = control_matrix(features, base_medians, gaps=gaps)
    values = moneyflow_values(features)
    missing = np.isnan(values)
    return np.column_stack((prefix, np.where(missing, medians, values), missing.astype(float)))


def train_moneyflow_price_5td_v1(*, rows, frozen_control, configuration, before_fit, after_fit):
    from sklearn.ensemble import GradientBoostingRegressor
    config = GenericPrice5TDConfigurationV1.model_validate(configuration)
    support = validate_control(frozen_control)
    required = {*KEY, *MODEL_FEATURES, "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio",
                "label_status", "label_information_end", "policy_sha256", "label_contract"}
    if (not isinstance(rows, pd.DataFrame) or not rows.columns.is_unique or len(rows) > 7720
            or not required.issubset(rows.columns) or rows.duplicated(list(KEY)).any()
            or not rows.policy_sha256.eq(POLICY_SHA256).all()
            or not rows.label_contract.eq(POLICY["label_contract"]).all()
            or frozen_control.recipe["configuration"] != config.model_dump(mode="json")):
        raise ValueError("moneyflow training source/policy/frozen configuration differs; no fits")
    data = rows.copy(deep=True)
    for name in KEY[:2]:
        data[name] = data[name].map(_day).map(pd.Timestamp)
    allowed = (data[KEY[0]].between(pd.Timestamp(config.train_start), pd.Timestamp(config.train_end))
               | data[KEY[0]].between(pd.Timestamp(config.validation_start), pd.Timestamp(config.validation_end)))
    if not allowed.all():
        raise ValueError("moneyflow training input contains test or foreign development dates")
    maturity = data.label_information_end.map(lambda value: _day(value, nullable=True)).map(pd.Timestamp)
    immature = maturity.isna() | (data[KEY[0]].le(pd.Timestamp(config.train_end)) & maturity.gt(pd.Timestamp(config.train_end)))
    immature |= data[KEY[0]].ge(pd.Timestamp(config.validation_start)) & maturity.gt(pd.Timestamp(config.validation_end))
    if data.loc[immature, ["observed_gap_bps", "gross_terminal_ratio", "path_min_ratio"]].notna().any().any():
        raise ValueError("moneyflow boundary rows contain future numeric supervision; no fits")
    # Validate D inputs without selecting rows by funding availability.
    moneyflow_values(data)
    domain = data.loc[data[KEY[0]].between(pd.Timestamp(config.train_start), pd.Timestamp(config.train_end))
                      & data[KEY[1]].le(pd.Timestamp(config.train_end))]
    _, available = feature_values(domain)
    domain = domain.loc[available]
    values = moneyflow_values(domain)
    medians = [float(np.median(column[~np.isnan(column)])) if (~np.isnan(column)).any() else 0.
               for column in values.T]
    maturity = domain.label_information_end.map(lambda value: _day(value, nullable=True)).map(pd.Timestamp)
    gaps = domain.observed_gap_bps.map(_number)
    mask = domain.label_status.eq("AVAILABLE") & maturity.le(pd.Timestamp(config.train_end))
    mask &= gaps.map(lambda value: pd.notna(value) and support.contains(float(value)))
    train = domain.loc[mask].sort_values(list(KEY))
    keys_sha = sha([[str(value) for value in row] for row in train.loc[:, KEY].itertuples(index=False, name=None)])
    if (len(train) < 100 or train[KEY[0]].nunique() < 20
            or keys_sha != frozen_control.diagnostics["training_keys_sha256"]):
        raise ValueError("moneyflow mature training KEYs differ from frozen GP5; no fits")
    target = train.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda value: _number(value, positive=True))
    if target.isna().any().any() or target.path_min_ratio.gt(target.gross_terminal_ratio+1e-6).any():
        raise ValueError("moneyflow paired mature targets contradict prices; no fits")
    x = matrix_v1(train, base_medians=frozen_control.recipe["medians"], moneyflow_medians=medians, gaps=train.observed_gap_bps)
    models = {}
    for head, values in (("mean", target.gross_terminal_ratio), ("path", target.path_min_ratio)):
        before_fit(head)
        estimator = GradientBoostingRegressor(**PARAMETERS, loss="quantile" if head == "path" else "squared_error", alpha=.1)
        estimator.fit(x, values)
        after_fit(head)
        body = export_gbdt_v2(estimator)
        if not np.allclose(predict_json_v2(body, x), estimator.predict(x), rtol=1e-10, atol=1e-10):
            raise ValueError("moneyflow non-executing JSON prediction parity failed")
        models[head] = body
    recipe = dict(features=list(MODEL_FEATURES), matrix_order=list(MATRIX_ORDER),
        base_medians=frozen_control.recipe["medians"], moneyflow_medians=medians,
        schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256, parameters=PARAMETERS,
        label_contract=POLICY["label_contract"], configuration=config.model_dump(mode="json"),
        frozen_control_model_sha256=frozen_control.model_sha256, frozen_control_arm="candidate_19D")
    diagnostics = dict(train_rows=len(train), train_days=int(train[KEY[0]].nunique()), training_keys_sha256=keys_sha,
        moneyflow_known_train_counts={name: int(train[name].notna().sum()) for name in MONEYFLOW_FEATURES},
        physical_fit_count=2, test_used_for_training_or_calibration=False, deployable=False)
    validation = data.loc[data[KEY[0]].between(pd.Timestamp(config.validation_start), pd.Timestamp(config.validation_end))]
    maturity = validation.label_information_end.map(lambda value: _day(value, nullable=True)).map(pd.Timestamp)
    _, available = feature_values(validation)
    mask = validation.label_status.eq("AVAILABLE") & maturity.le(pd.Timestamp(config.validation_end)) & available
    mask &= validation.observed_gap_bps.map(lambda value: pd.notna(value) and support.contains(float(value)))
    validation = validation.loc[mask]
    diagnostics["validation_diagnostics_only"] = dict(supported_rows=len(validation), used_for_selection=False)
    if not validation.empty:
        targets = validation.loc[:, ["gross_terminal_ratio", "path_min_ratio"]].map(lambda value: _number(value, positive=True))
        if targets.isna().any().any():
            raise ValueError("moneyflow validation paired targets are incomplete")
        xv = matrix_v1(validation, base_medians=frozen_control.recipe["medians"], moneyflow_medians=medians,
                       gaps=validation.observed_gap_bps)
        mean, lower = (predict_json_v2(models[head], xv) for head in ("mean", "path"))
        error = targets.path_min_ratio.to_numpy()-lower
        diagnostics["validation_diagnostics_only"].update(mean_squared_error=float(np.mean((mean-targets.gross_terminal_ratio)**2)),
            path_pinball_loss=float(np.mean(np.maximum(.1*error, -.9*error))), path_lower_coverage=float(np.mean(error < 0)))
    fitted = GenericMoneyflowPrice5TDFitV1(recipe, models, frozen_control.intervals_bps, diagnostics, "")
    return GenericMoneyflowPrice5TDFitV1(recipe, models, fitted.intervals_bps, diagnostics, fitted_identity(fitted))


def validate_fit(fitted):
    if (not isinstance(fitted, GenericMoneyflowPrice5TDFitV1) or fitted_identity(fitted) != fitted.model_sha256
            or fitted.recipe.get("features") != list(MODEL_FEATURES) or fitted.recipe.get("matrix_order") != list(MATRIX_ORDER)
            or fitted.recipe.get("schema_sha256") != SCHEMA_SHA256 or fitted.recipe.get("policy_sha256") != POLICY_SHA256
            or fitted.recipe.get("parameters") != PARAMETERS or fitted.recipe.get("label_contract") != POLICY["label_contract"]
            or fitted.recipe.get("frozen_control_arm") != "candidate_19D" or set(fitted.models) != {"mean", "path"}):
        raise ValueError("moneyflow model/schema/policy identity differs")
    base = np.asarray(fitted.recipe["base_medians"], dtype=float)
    funding = np.asarray(fitted.recipe["moneyflow_medians"], dtype=float)
    if base.shape != (9,) or funding.shape != (3,) or not np.isfinite(np.concatenate((base, funding))).all():
        raise ValueError("moneyflow model median dimensions differ")
    if any(body.get("kind") != "gbdt" or body.get("features") != 25 for body in fitted.models.values()):
        raise ValueError("moneyflow heads must be independent 25-column JSON trees")
    return ValueAnchorGapSupportV1(fitted.intervals_bps)


def query_moneyflow_nodes_v1(*, fitted, features, scenario_gap_bps):
    support = validate_fit(fitted)
    _, available = feature_values(features)
    funding = moneyflow_values(features)
    gaps = np.asarray([_number(value) for value in scenario_gap_bps], dtype=float)
    if not features.index.is_unique or gaps.shape != (len(features),) or (gaps[~np.isnan(gaps)] <= -10000).any():
        raise ValueError("moneyflow query index or hypothetical price coordinates differ")
    selected = available & np.array([not np.isnan(value) and support.contains(float(value)) for value in gaps], dtype=bool)
    result = pd.DataFrame(dict(status=["UNKNOWN_INPUT_OR_SUPPORT"]*len(features),
        expected_net_bps=np.full(len(features), np.nan), downside_q90_bps=np.full(len(features), np.nan)), index=features.index)
    for name in (*KEY, "selection_effective_rank", "candidate_group_size", "package_id", "run_id", "list_version_id",
                 "universe_identity", "source_evidence", "moneyflow_feature_status", "moneyflow_feature_visible_through"):
        if name in features:
            result[name] = features[name].copy()
    original, _ = feature_values(features)
    result["input_unknown_fields"] = [tuple(name for name, value in zip(MODEL_FEATURES, row, strict=True) if np.isnan(value))
                                      for row in np.column_stack((original, funding))]
    result["input_sha256"] = [sha(dict(schema_sha256=SCHEMA_SHA256,
        values=[None if np.isnan(value) else float(value) for value in row], scenario_gap_bps=None if np.isnan(gap) else float(gap)))
        for row, gap in zip(np.column_stack((original, funding)), gaps, strict=True)]
    result["model_sha256"], result["policy_sha256"] = fitted.model_sha256, POLICY_SHA256
    result["label_contract"], result["holding_sessions"] = POLICY["label_contract"], 5
    if selected.any():
        x = matrix_v1(features.iloc[np.flatnonzero(selected)], base_medians=fitted.recipe["base_medians"],
            moneyflow_medians=fitted.recipe["moneyflow_medians"], gaps=gaps[selected])
        mean, lower = (predict_json_v2(fitted.models[head], x) for head in ("mean", "path"))
        if not np.isfinite([mean, lower]).all() or (mean <= 0).any() or (lower <= 0).any():
            raise ValueError("moneyflow predicted ratios contradict prices")
        entry = 1+gaps[selected]/10000
        net = 10000*(mean*(1-POLICY["sell_bps"]/10000)/(entry*(1+POLICY["buy_bps"]/10000))-1)
        risk = 10000*np.maximum(0, 1-lower/entry)
        if not np.isfinite([net, risk]).all():
            raise ValueError("moneyflow net/risk arithmetic is nonfinite")
        result.loc[selected, "status"] = np.where((net > 0) & (risk <= POLICY["risk_bps"]), "ACCEPTABLE", "AVOID")
        result.loc[selected, "expected_net_bps"], result.loc[selected, "downside_q90_bps"] = net, risk
    return result


def moneyflow_price_set_5td_v1(*, fitted, d_features, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    coordinates = (reference_cny, legal_low_cny, legal_high_cny, tick_cny)
    if any(_number(value, positive=True) is None for value in coordinates) or set(d_features) != set(MODEL_FEATURES):
        raise ValueError("moneyflow price set schema or positive legal coordinates differ")
    reference, low, high, tick = (Decimal(str(value)) for value in coordinates)
    if low > high:
        raise ValueError("moneyflow legal price bounds differ")
    single = pd.DataFrame([d_features], columns=MODEL_FEATURES)
    base, _ = feature_values(single)
    funding = moneyflow_values(single)
    values = np.column_stack((base, funding))[0]
    unknown_fields = tuple(name for name, value in zip(MODEL_FEATURES, values, strict=True) if np.isnan(value))
    input_sha = sha(dict(schema_sha256=SCHEMA_SHA256, values=[None if np.isnan(value) else float(value) for value in values],
                        coordinates=[str(value) for value in (reference, low, high, tick)], price_basis="D_ANCHORED_CNY"))
    first = int((low/tick).to_integral_value(rounding=ROUND_CEILING))
    last = int((high/tick).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("moneyflow full legal tick grid exceeds budget")
    prices = [tick*number for number in range(first, last+1)]
    gaps = [float((price/reference-1)*10000) for price in prices]
    nodes = query_moneyflow_nodes_v1(fitted=fitted, features=pd.DataFrame([d_features]*len(prices), columns=MODEL_FEATURES),
                                   scenario_gap_bps=gaps)
    bands, start, end = [], None, None
    for price, status in zip(prices, nodes.status, strict=True):
        if status == "ACCEPTABLE":
            start, end = price if start is None else start, price
        elif start is not None:
            bands.append((float(start), float(end)))
            start = end = None
    if start is not None:
        bands.append((float(start), float(end)))
    unknown = int(nodes.status.eq("UNKNOWN_INPUT_OR_SUPPORT").sum())
    status = ("EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands
              else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if 0 < unknown < len(prices)
              else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(status=status, intervals_cny=tuple(bands), legal_node_count=len(prices), unknown_node_count=unknown,
        model_sha256=fitted.model_sha256, policy_sha256=POLICY_SHA256, schema_sha256=SCHEMA_SHA256,
        holding_sessions=5, price_basis="D_ANCHORED_CNY", label_contract=POLICY["label_contract"],
        input_sha256=input_sha, input_unknown_fields=unknown_fields,
        evidence_use="NAVIGATION_ONLY", deployable=False, valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL")
