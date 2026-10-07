"""Past-only Ridge; S-only immutable curves and separate hypothetical queries."""
import json

import numpy as np
import pandas as pd
from sklearn.linear_model import Ridge

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_exit_price_conditioned_5td_contracts_v1 import (
    CURVE_FIELDS, FEATURES, KEY, POLICY_SHA256, QUERY_FIELDS, RECIPE, RECIPE_SHA256,
    S_FIELDS, TRAIN_FIELDS,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _s_frame(frame):
    if (not isinstance(frame, pd.DataFrame) or set(frame.columns) != set(S_FIELDS)
            or not frame.columns.is_unique or len(frame) > 15000 or frame.duplicated(list(KEY)).any()):
        raise ValueError("Exit curve needs exact S-only fields/original unique keys")
    result = frame.copy(deep=True)
    result["s_date"] = result.s_date.map(lambda value: _day(value).isoformat())
    if not result.remaining_session_fraction.map(_number).isin([.25, .5, .75, 1.]).all():
        raise ValueError("Exit curve remaining must be the original known one-to-four sessions")
    result[list(FEATURES)] = result.loc[:, FEATURES].map(_number).astype(float)
    return result


def _matrix(s_inputs, query_bps, recipe=None):
    state = _s_frame(s_inputs)
    query = np.asarray([_number(value) for value in query_bps], dtype=float)
    if query.shape != (len(state),) or not np.isfinite(query).all() or (query <= -10000).any():
        raise ValueError("Exit hypothetical net query must be finite and above minus 100 percent")
    raw = np.column_stack((state.loc[:, FEATURES].to_numpy(dtype=float),
        state.remaining_session_fraction.to_numpy(dtype=float), query/RECIPE["query_scale"]))
    missing = np.isnan(raw)
    if recipe is None:
        medians = np.asarray([np.median(col[np.isfinite(col)]) if np.isfinite(col).any() else 0. for col in raw.T])
        filled = np.where(missing, medians, raw)
        mean, scale = filled.mean(axis=0), filled.std(axis=0)
        scale[scale == 0] = 1.
        recipe = dict(medians=medians.tolist(), mean=mean.tolist(), scale=scale.tolist())
    arrays = [np.asarray(recipe[name], dtype=float) for name in ("medians", "mean", "scale")]
    if any(value.shape != (11,) or not np.isfinite(value).all() for value in arrays) or (arrays[2] <= 0).any():
        raise ValueError("Exit frozen train-only encoding differs")
    matrix = np.column_stack(((np.where(missing, arrays[0], raw)-arrays[1])/arrays[2], missing[:, :9].astype(float)))
    if matrix.shape[1] != RECIPE["model_inputs"] or not np.isfinite(matrix).all():
        raise ValueError("Exit encoded fixed twenty inputs are nonfinite")
    return matrix, recipe


def _support(train):
    output = {}
    for fraction, group in train.groupby("remaining_session_fraction", sort=True):
        values = group.sell_scenario_bps.to_numpy(dtype=float)
        low, high = np.quantile(values, RECIPE["support_quantiles"])
        buckets = [int(np.floor(value/RECIPE["support_bucket_bps"])) for value in values]
        intervals = []
        for bucket, subset in group.assign(support_bucket=buckets).groupby("support_bucket", sort=True):
            start = max(float(low), bucket*RECIPE["support_bucket_bps"])
            end = min(float(high), np.nextafter((bucket+1)*RECIPE["support_bucket_bps"], -np.inf))
            if len(subset) >= RECIPE["support_min_decisions"] and subset.entry_date.nunique() >= RECIPE["support_min_entry_cohorts"] and start <= end:
                intervals.append([float(start), float(end)])
        output[str(int(fraction*4))] = intervals
    return output


def fit_price_conditioned_fold_v1(*, train_rows, ordinal, before_fit):
    if (set(train_rows.columns) != set(TRAIN_FIELDS) or not train_rows.columns.is_unique
            or len(train_rows) > 15000 or train_rows.duplicated(list(KEY)).any()):
        raise ValueError("Exit matured train query schema differs")
    _s_frame(train_rows.loc[:, S_FIELDS])
    train = train_rows.loc[train_rows.held.eq(True) & train_rows.y_hold_bps.notna() & train_rows.sell_scenario_bps.notna()].copy()
    if train.empty:
        result = dict(status="UNKNOWN_NO_MATURE_QUERY_TRAIN", ordinal=ordinal, physical_fits=0,
            recipe_sha256=RECIPE_SHA256, support={}, train_decisions=0)
    else:
        y = train.y_hold_bps.to_numpy(dtype=float)
        if not np.isfinite(y).all():
            raise ValueError("Exit mature target cannot be infinite")
        x, encoding = _matrix(train.loc[:, S_FIELDS], train.sell_scenario_bps)
        support = _support(train)
        before_fit(ordinal)
        model = Ridge(alpha=RECIPE["alpha"]).fit(x, y)
        result = dict(status="FITTED", ordinal=ordinal, physical_fits=1,
            recipe_sha256=RECIPE_SHA256, encoding=encoding, support=support,
            coefficients=model.coef_.tolist(), intercept=float(model.intercept_),
            train_decisions=len(train), train_entry_cohorts=int(train.entry_date.nunique()))
        if not np.allclose(x@model.coef_+model.intercept_, model.predict(x), rtol=1e-12, atol=1e-9):
            raise ValueError("Exit linear curve/estimator export parity differs")
    result["model_sha256"] = sha(result)
    return result


def seal_s_curves_v1(*, model, s_inputs):
    state = _s_frame(s_inputs)
    identity = model.get("model_sha256")
    if identity != sha({key: value for key, value in model.items() if key != "model_sha256"}) or model.get("recipe_sha256") != RECIPE_SHA256:
        raise ValueError("Exit frozen model identity differs")
    if model["status"] == "FITTED":
        x, _ = _matrix(state, np.zeros(len(state)), model["encoding"])
        coefficients = np.asarray(model["coefficients"], dtype=float)
        if coefficients.shape != (20,) or not np.isfinite(coefficients).all() or not np.isfinite(model["intercept"]):
            raise ValueError("Exit fixed mean coefficients differ")
        intercepts = x@coefficients+model["intercept"]
        slope = float(coefficients[10]/model["encoding"]["scale"][10]/RECIPE["query_scale"])
        if not np.isfinite(intercepts).all() or not np.isfinite(slope):
            raise ValueError("Exit mean curve cannot be nonfinite")
    elif model["status"] == "UNKNOWN_NO_MATURE_QUERY_TRAIN":
        intercepts, slope = np.full(len(state), np.nan), None
    else:
        raise ValueError("Exit unrecognized model status")
    output = []
    for row, intercept in zip(state.to_dict("records"), intercepts, strict=True):
        remaining = int(row["remaining_session_fraction"]*4)
        observed = {key: None if pd.isna(value) else value for key, value in row.items()}
        curve = dict(episode_id=row["episode_id"], s_date=row["s_date"], remaining_sessions=remaining,
            a_bps=float(intercept) if np.isfinite(intercept) else None, b_per_bps=slope,
            support_json=json.dumps(model["support"].get(str(remaining), []), separators=(",", ":")),
            curve_status=model["status"], model_sha256=identity, s_input_sha256=sha(observed), policy_sha256=POLICY_SHA256)
        curve["curve_sha256"] = sha(curve)
        output.append(curve)
    return pd.DataFrame(output, columns=CURVE_FIELDS)


def query_sealed_curves_v1(*, curves, queries):
    if (set(curves.columns) != set(CURVE_FIELDS) or set(queries.columns) != set(QUERY_FIELDS)
            or not curves.columns.is_unique or not queries.columns.is_unique
            or curves.duplicated(list(KEY)).any() or queries.duplicated(list(KEY)).any()
            or len(curves) != len(queries) or len(curves) > 15000):
        raise ValueError("Exit curve/query original key domain differs")
    query = queries.copy(deep=True)
    query["s_date"] = query.s_date.map(lambda value: _day(value).isoformat())
    paired = curves.merge(query, on=list(KEY), how="outer", validate="one_to_one", indicator=True)
    if not paired._merge.eq("both").all():
        raise ValueError("Exit query cannot add/drop original S keys")
    records = []
    for row in paired.drop(columns="_merge").to_dict("records"):
        body = {name: None if name in ("a_bps", "b_per_bps") and pd.isna(row[name]) else row[name]
                for name in CURVE_FIELDS if name != "curve_sha256"}
        if sha(body) != row["curve_sha256"] or row["policy_sha256"] != POLICY_SHA256:
            raise ValueError("Exit sealed S curve identity differs")
        parsed = _number(row["sell_scenario_bps"])
        value = np.nan if parsed is None else parsed
        if np.isfinite(value) and value <= -10000:
            raise ValueError("Exit U query is not a possible net price")
        supported = any(low <= value <= high for low, high in json.loads(row["support_json"])) if np.isfinite(value) else False
        known = supported and row["curve_status"] == "FITTED"
        status = ("UNKNOWN_NO_MATURE_QUERY_TRAIN" if row["curve_status"] != "FITTED"
            else "KNOWN_SUPPORTED_QUERY" if known else "UNKNOWN_QUERY_SUPPORT" if np.isfinite(value) else "UNKNOWN_QUERY_VALUE")
        records.append({**{key: row[key] for key in KEY}, "predicted_y_hold_bps": row["a_bps"]+row["b_per_bps"]*value if known else np.nan,
            "query_status": status, "curve_sha256": row["curve_sha256"]})
    return pd.DataFrame(records, columns=[*KEY, "predicted_y_hold_bps", "query_status", "curve_sha256"])
